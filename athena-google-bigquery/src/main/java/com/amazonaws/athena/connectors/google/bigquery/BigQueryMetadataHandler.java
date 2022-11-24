
/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.fixCaseForDatasetName;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.fixCaseForTableName;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.translateToArrowType;

public class BigQueryMetadataHandler
    extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryMetadataHandler.class);
    /**
     * The {@link BigQuery} client to interact with the BigQuery Service.
     */
    private final BigQuery bigQuery;

    BigQueryMetadataHandler()
        throws IOException
    {
        this(BigQueryUtils.getBigQueryClient());
    }

    @VisibleForTesting
    public BigQueryMetadataHandler(BigQuery bigQuery)
    {
        super(BigQueryConstants.SOURCE_TYPE);
        this.bigQuery = bigQuery;
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        try {
            logger.info("doListSchemaNames called with Catalog: {}", listSchemasRequest.getCatalogName());

            final List<String> schemas = new ArrayList<>();
            final String projectName = BigQueryUtils.getProjectName(listSchemasRequest);
            Page<Dataset> response = bigQuery.listDatasets(projectName, BigQuery.DatasetListOption.pageSize(100));
            if (response == null) {
                logger.info("Dataset does not contain any models: {}");
            }
            else {
                do {
                    for (Dataset dataset : response.iterateAll()) {
                        if (schemas.size() > BigQueryConstants.MAX_RESULTS) {
                            throw new BigQueryExceptions.TooManyTablesException();
                        }
                        schemas.add(dataset.getDatasetId().getDataset().toLowerCase());
                        logger.debug("Found Dataset: {}", dataset.getDatasetId().getDataset());
                    }
                } while (response.hasNextPage());
            }

            logger.info("Found {} schemas!", schemas.size());

            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);
        }
        catch
        (Exception e) {
            logger.error("Error: ", e);
        }
        return null;
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        try {
            logger.info("doListTables called with request {}:{}", listTablesRequest.getCatalogName(),
                    listTablesRequest.getSchemaName());
            //Get the project name, dataset name, and dataset id. Google BigQuery is case sensitive.
            String nextToken = null;
            final String projectName = BigQueryUtils.getProjectName(listTablesRequest);
            final String datasetName = fixCaseForDatasetName(projectName, listTablesRequest.getSchemaName(), bigQuery);
            final DatasetId datasetId = DatasetId.of(projectName, datasetName);
            List<TableName> tables = new ArrayList<>();
            if (listTablesRequest.getPageSize() == UNLIMITED_PAGE_SIZE_VALUE) {
                Page<Table> response = bigQuery.listTables(datasetId);
                for (Table table : response.iterateAll()) {
                    if (tables.size() > BigQueryConstants.MAX_RESULTS) {
                        throw new BigQueryExceptions.TooManyTablesException();
                    }
                    tables.add(new TableName(listTablesRequest.getSchemaName(), table.getTableId().getTable()));
                }
            }
            else {
                Page<Table> response = bigQuery.listTables(datasetId,
                        BigQuery.TableListOption.pageToken(listTablesRequest.getNextToken()), BigQuery.TableListOption.pageSize(listTablesRequest.getPageSize()));
                for (Table table : response.getValues()) {
                    tables.add(new TableName(listTablesRequest.getSchemaName(), table.getTableId().getTable()));
                }
                nextToken = response.getNextPageToken();
            }
            return new ListTablesResponse(listTablesRequest.getCatalogName(), tables, nextToken);
        }
        catch
        (Exception e) {
            logger.error("Error:", e);
        }
        return null;
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        try {
            logger.info("doGetTable called with request {}:{}", BigQueryUtils.getProjectName(getTableRequest),
                    getTableRequest.getTableName());

            final Schema tableSchema = getSchema(BigQueryUtils.getProjectName(getTableRequest), getTableRequest.getTableName().getSchemaName(),
                    getTableRequest.getTableName().getTableName());
            return new GetTableResponse(BigQueryUtils.getProjectName(getTableRequest).toLowerCase(),
                    getTableRequest.getTableName(), tableSchema);
        }
        catch (Exception e) {
            logger.error("Error: ", e);
        }
        return null;
    }

    /**
     *
     * Currently not supporting Partitions since Bigquery having quota limits with triggering concurrent queries and having bit complexity to extract and use the partitions
     * in the query instead we are using limit and offset for non constraints query with basic concurrency limit
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        //NoOp since we don't support partitioning at this time.
    }

    /**
     * Making minimum(10) splits based on constraints. Since without constraints query may give lambda timeout if table has large data,
     * concurrencyLimit is configurable and it can be changed based on Google BigQuery Quota Limits.
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, and partition(s) being queried as well as
     * any filter predicate.
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws IOException, InterruptedException
    {
        int constraintsSize = request.getConstraints().getSummary().size();
        if (constraintsSize > 0) {
            //Every split must have a unique location if we wish to spill to avoid failures
            SpillLocation spillLocation = makeSpillLocation(request);

            return new GetSplitsResponse(request.getCatalogName(), Split.newBuilder(spillLocation,
                    makeEncryptionKey()).build());
        }
        else {
            String projectName = BigQueryUtils.getProjectName(request);
            String dataSetName = fixCaseForDatasetName(projectName, request.getTableName().getSchemaName(), bigQuery);
            String tableName = fixCaseForTableName(projectName, dataSetName, request.getTableName().getTableName(), bigQuery);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT count(*) FROM `" + projectName + "." + dataSetName + "." + tableName + "` ").setUseLegacySql(false).build();
            // Create a job ID so that we can safely retry.
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = BigQueryUtils.getBigQueryClient().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).waitFor();
            TableResult result = queryJob.getQueryResults();

            double numberOfRows = result.iterateAll().iterator().next().get(0).getLongValue();
            logger.debug("numberOfRows: " + numberOfRows);
            int concurrencyLimit = Integer.parseInt(BigQueryUtils.getEnvVar("concurrencyLimit"));
            logger.debug("concurrencyLimit: " + numberOfRows);
            long pageCount = (long) numberOfRows / concurrencyLimit;
            long totalPageCountLimit = (pageCount == 0) ? (long) numberOfRows : pageCount;
            double limit = (int) Math.ceil(numberOfRows / totalPageCountLimit);
            Set<Split> splits = new HashSet<>();
            long offSet = 0;

            for (int i = 1; i <= limit; i++) {
                if (i > 1) {
                    offSet = offSet + totalPageCountLimit;
                }
                // Every split must have a unique location if we wish to spill to avoid failures
                SpillLocation spillLocation = makeSpillLocation(request);
                // Create a new split (added to the splits set) that includes the domain and endpoint, and
                // shard information (to be used later by the Record Handler).
                Map<String, String> map = new HashMap<>();
                map.put(Long.toString(totalPageCountLimit), Long.toString(offSet));
                splits.add(new Split(spillLocation, makeEncryptionKey(), map));
            }
            return new GetSplitsResponse(request.getCatalogName(), splits);
        }
    }

    /**
     * Getting Bigquery table schema details
     * @param projectName
     * @param datasetName
     * @param tableName
     * @return
     */
    private Schema getSchema(String projectName, String datasetName, String tableName)
    {
        Schema schema = null;
        datasetName = fixCaseForDatasetName(projectName, datasetName, bigQuery);
        tableName = fixCaseForTableName(projectName, datasetName, tableName, bigQuery);
        TableId tableId = TableId.of(projectName, datasetName, tableName);
        Table response = bigQuery.getTable(tableId);
        TableDefinition tableDefinition = response.getDefinition();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        List<String> timeStampColsList = new ArrayList<>();

        for (Field field : tableDefinition.getSchema().getFields()) {
            if (field.getType().getStandardType().toString().equals("TIMESTAMP")) {
                timeStampColsList.add(field.getName());
            }
            schemaBuilder.addField(field.getName(), translateToArrowType(field.getType()));
        }
        schemaBuilder.addMetadata("timeStampCols", timeStampColsList.toString());
        logger.debug("BigQuery table schema {}", schemaBuilder.toString());
        return schemaBuilder.build();
    }
}
