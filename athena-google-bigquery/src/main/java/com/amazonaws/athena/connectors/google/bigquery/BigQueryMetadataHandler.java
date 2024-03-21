
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
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.google.bigquery.qpt.BigQueryQueryPassthrough;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.fixCaseForDatasetName;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.fixCaseForTableName;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.translateToArrowType;
import static com.google.cloud.bigquery.JobStatistics.QueryStatistics.StatementType.SELECT;

public class BigQueryMetadataHandler
    extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryMetadataHandler.class);
    private final String projectName = configOptions.get(BigQueryConstants.GCP_PROJECT_ID);

    private final BigQueryQueryPassthrough queryPassthrough = new BigQueryQueryPassthrough();

    BigQueryMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(BigQueryConstants.SOURCE_TYPE, configOptions);
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT
        ));

        queryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest) throws IOException
    {
        logger.info("doListSchemaNames called with Catalog: {}", listSchemasRequest.getCatalogName());

        final List<String> schemas = new ArrayList<>();
        BigQuery bigQuery = BigQueryUtils.getBigQueryClient(configOptions);
        Page<Dataset> response = bigQuery.listDatasets(projectName, BigQuery.DatasetListOption.pageSize(100));
        if (response == null) {
            logger.info("Dataset does not contain any models");
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

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest) throws IOException
    {
        logger.info("doListTables called with request {}:{}", listTablesRequest.getCatalogName(),
                listTablesRequest.getSchemaName());
        //Get the project name, dataset name, and dataset id. Google BigQuery is case sensitive.
        String nextToken = null;
        BigQuery bigQuery = BigQueryUtils.getBigQueryClient(configOptions);
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

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest) throws java.io.IOException
    {
        logger.info("doGetTable called with request {}. Resolved projectName: {}", getTableRequest.getCatalogName(), projectName);
        final Schema tableSchema = getSchema(getTableRequest.getTableName().getSchemaName(),
                getTableRequest.getTableName().getTableName());
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(), tableSchema);
    }

    @Override
    public GetTableResponse doGetQueryPassthroughSchema(BlockAllocator allocator, GetTableRequest request) throws Exception
    {
        if (!request.isQueryPassthrough()) {
            throw new IllegalArgumentException("No Query passed through [{}]" + request);
        }
        queryPassthrough.verify(request.getQueryPassthroughArguments());
        String query = request.getQueryPassthroughArguments().get(BigQueryQueryPassthrough.QUERY);
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).setDryRun(true).setUseQueryCache(false).build();

        Job job = bigquery.create(JobInfo.of(queryConfig));
        JobStatistics.QueryStatistics statistics = job.getStatistics();
        if (!statistics.getStatementType().equals(SELECT)) {
            throw new IllegalArgumentException("Unsupported statement type: " + statistics.getStatementType());
        }
        com.google.cloud.bigquery.Schema bigQuerySchema = statistics.getSchema();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        List<String> timeStampColsList = new ArrayList<>();

        convertBigQuerySchema(bigQuerySchema, timeStampColsList, schemaBuilder);
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schemaBuilder.build());
    }

    /**
     * Currently not supporting Partitions since Bigquery having quota limits with triggering concurrent queries and having bit complexity to extract and use the partitions
     * in the query instead we are using limit and offset for non constraints query with basic concurrency limit
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
    {
        //NoOp since we don't support partitioning at this time.
    }

    /**
     * Making minimum(10) splits based on constraints. Since without constraints query may give lambda timeout if table has large data,
     * concurrencyLimit is configurable and it can be changed based on Google BigQuery Quota Limits.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details of the catalog, database, table, and partition(s) being queried as well as
     *                  any filter predicate.
     * @return GetSplitsResponse
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        return new GetSplitsResponse(request.getCatalogName(), Split.newBuilder(spillLocation,
                makeEncryptionKey()).build());
    }

    /**
     * Getting Bigquery table schema details
     *
     * @param datasetName
     * @param tableName
     * @return Schema
     */
    private Schema getSchema(String datasetName, String tableName) throws java.io.IOException
    {
        BigQuery bigQuery = BigQueryUtils.getBigQueryClient(configOptions);
        datasetName = fixCaseForDatasetName(projectName, datasetName, bigQuery);
        tableName = fixCaseForTableName(projectName, datasetName, tableName, bigQuery);

        TableId tableId = TableId.of(projectName, datasetName, tableName);
        Table response = bigQuery.getTable(tableId);
        TableDefinition tableDefinition = response.getDefinition();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        List<String> timeStampColsList = new ArrayList<>();

        convertBigQuerySchema(tableDefinition.getSchema(), timeStampColsList, schemaBuilder);
        schemaBuilder.addMetadata("timeStampCols", timeStampColsList.toString());
        logger.debug("BigQuery table schema {}", schemaBuilder.toString());
        return schemaBuilder.build();
    }

    /**
     * Responsible for converting BigQuery Schema to schema builder
     * @param bigQuerySchema
     * @param timeStampColsList
     * @param schemaBuilder
     */
    private void convertBigQuerySchema(com.google.cloud.bigquery.Schema bigQuerySchema, List<String> timeStampColsList, SchemaBuilder schemaBuilder)
    {
        for (Field field : bigQuerySchema.getFields()) {
            if (field.getType().getStandardType().toString().equals("TIMESTAMP")) {
                timeStampColsList.add(field.getName());
            }
            if (null != field.getMode() && field.getMode().name().equals("REPEATED")) {
                if (field.getType().getStandardType().name().equalsIgnoreCase("Struct")) {
                    schemaBuilder.addField(field.getName(), Types.MinorType.LIST.getType(), ImmutableList.of(new org.apache.arrow.vector.types.pojo.Field(field.getName(), FieldType.nullable(Types.MinorType.STRUCT.getType()), BigQueryUtils.getChildFieldList(field))));
                }
                else {
                    schemaBuilder.addField(field.getName(), Types.MinorType.LIST.getType(), BigQueryUtils.getChildFieldList(field));
                }
            }
            else if (field.getType().getStandardType().name().equalsIgnoreCase("Struct")) {
                schemaBuilder.addField(field.getName(), Types.MinorType.STRUCT.getType(), BigQueryUtils.getChildFieldList(field));
            }
            else {
                schemaBuilder.addField(field.getName(), translateToArrowType(field.getType()));
            }
        }
    }
}
