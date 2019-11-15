/*-
 * #%L
 * athena-bigquery
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
package com.amazonaws.athena.connectors.bigquery;

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
import com.amazonaws.athena.connectors.bigquery.BigQueryExceptions.TooManyTablesException;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.athena.connectors.bigquery.BigQueryUtils.fixCaseForDatasetName;
import static com.amazonaws.athena.connectors.bigquery.BigQueryUtils.fixCaseForTableName;
import static com.amazonaws.athena.connectors.bigquery.BigQueryUtils.translateToArrowType;

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
    BigQueryMetadataHandler(BigQuery bigQuery)
    {
        super(BigQueryConstants.SOURCE_TYPE);
        this.bigQuery = bigQuery;
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        logger.info("doListSchemaNames called with Catalog: {}", listSchemasRequest.getCatalogName());

        final List<String> schemas = new ArrayList<>();
        final String projectName = BigQueryUtils.getProjectName(listSchemasRequest);
        Page<Dataset> response = bigQuery.listDatasets(projectName);

        for (Dataset dataset : response.iterateAll()) {
            if (schemas.size() > BigQueryConstants.MAX_RESULTS) {
                throw new TooManyTablesException();
            }
            schemas.add(dataset.getDatasetId().getDataset().toLowerCase());
            logger.debug("Found Dataset: {}", dataset.getDatasetId().getDataset());
        }

        logger.info("Found {} schemas!", schemas.size());

        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        logger.info("doListTables called with request {}:{}", listTablesRequest.getCatalogName(),
            listTablesRequest.getSchemaName());

        //Get the project name, dataset name, and dataset id. Google BigQuery is case sensitive.
        final String projectName = BigQueryUtils.getProjectName(listTablesRequest);
        final String datasetName = fixCaseForDatasetName(projectName, listTablesRequest.getSchemaName(), bigQuery);
        final DatasetId datasetId = DatasetId.of(projectName, datasetName);

        Page<Table> response = bigQuery.listTables(datasetId);
        List<TableName> tables = new ArrayList<>();

        for (Table table : response.iterateAll()) {
            if (tables.size() > BigQueryConstants.MAX_RESULTS) {
                throw new TooManyTablesException();
            }
            tables.add(new TableName(listTablesRequest.getSchemaName(), table.getTableId().getTable().toLowerCase()));
        }

        logger.info("Found {} table(s)!", tables.size());

        return new ListTablesResponse(listTablesRequest.getCatalogName(), tables);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        logger.info("doGetTable called with request {}:{}", BigQueryUtils.getProjectName(getTableRequest),
            getTableRequest.getTableName());

        final Schema tableSchema = getSchema(BigQueryUtils.getProjectName(getTableRequest), getTableRequest.getTableName().getSchemaName(),
            getTableRequest.getTableName().getTableName());
        return new GetTableResponse(BigQueryUtils.getProjectName(getTableRequest).toLowerCase(),
            getTableRequest.getTableName(), tableSchema);
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        //NoOp since we don't support partitioning at this time.
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        if (logger.isInfoEnabled()) {
            logger.info("DoGetSplits: {}.{} Part Cols: {}", request.getSchema(), request.getTableName(),
                String.join(",", request.getPartitionCols()));
        }

        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        return new GetSplitsResponse(request.getCatalogName(), Split.newBuilder(spillLocation,
            makeEncryptionKey()).build());
    }

    private Schema getSchema(String projectName, String datasetName, String tableName)
    {
        datasetName = fixCaseForDatasetName(projectName, datasetName, bigQuery);
        tableName = fixCaseForTableName(projectName, datasetName, tableName, bigQuery);
        TableId tableId = TableId.of(projectName, datasetName, tableName);
        Table response = bigQuery.getTable(tableId);
        TableDefinition tableDefinition = response.getDefinition();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (Field field : tableDefinition.getSchema().getFields()) {
            schemaBuilder.addField(field.getName(), translateToArrowType(field.getType()));
        }
        return schemaBuilder.build();
    }
}
