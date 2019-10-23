package com.amazonaws.connectors.athena.bigquery;

import static com.amazonaws.connectors.athena.bigquery.BigQueryUtils.fixCaseForDatasetName;
import static com.amazonaws.connectors.athena.bigquery.BigQueryUtils.fixCaseForTableName;
import static com.amazonaws.connectors.athena.bigquery.BigQueryUtils.translateToArrowType;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.resourcemanager.ResourceManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryMetadataHandler.class);
    private static final String sourceType = "bigquery";
    private static final long MAX_RESULTS = 10_000;
    public static final String PROJECT_NAME = "BQ_PROJECT_NAME";

    private final BigQuery bigQuery;
    private final ResourceManager resourceManager;

    public BigQueryMetadataHandler()
            throws IOException
    {
        this(BigQueryUtils.getBigQueryClient(),
                BigQueryUtils.getResourceManagerClient());
    }

    private String getProjectName(MetadataRequest request)
    {
        if (System.getenv(PROJECT_NAME) != null) {
            return System.getenv(PROJECT_NAME);
        }
        return request.getCatalogName();
    }

    @VisibleForTesting
    BigQueryMetadataHandler(BigQuery bigQuery, ResourceManager resourceManager)
    {
        super(sourceType);
        this.bigQuery = bigQuery;
        this.resourceManager = resourceManager;
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        logger.info("doListSchemaNames called with Catalog: {}", listSchemasRequest.getCatalogName());

        List<String> schemas = new ArrayList<>();
        String projectName = getProjectName(listSchemasRequest);
        Page<Dataset> response = bigQuery.listDatasets(projectName);

        for (Dataset dataset : response.iterateAll()) {
            if (schemas.size() > MAX_RESULTS) {
                throw new RuntimeException("Too many log groups, exceeded max metadata results for schema count.");
            }
            schemas.add(dataset.getDatasetId().getDataset().toLowerCase());
            logger.debug("Found Dataset: {}", dataset.getDatasetId().getDataset());
        }

        logger.info("Found {} schemas!", schemas.size());

        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        logger.info("doListTables called with request {}:{}", listTablesRequest.getCatalogName(),
                listTablesRequest.getSchemaName());

        //Check the case
        String projectName = getProjectName(listTablesRequest);
        String datasetName = fixCaseForDatasetName(projectName, listTablesRequest.getSchemaName(), bigQuery);
        DatasetId datasetId = DatasetId.of(projectName, datasetName);

        Page<Table> response = bigQuery.listTables(datasetId);
        List<TableName> tables = new ArrayList<>();

        for (Table table : response.iterateAll()) {
            if (tables.size() > MAX_RESULTS) {
                throw new RuntimeException("Too many log groups, exceeded max metadata results for schema count.");
            }
            tables.add(new TableName(listTablesRequest.getSchemaName(), table.getTableId().getTable().toLowerCase()));
        }

        logger.info("Found {} table(s)!", tables.size());

        return new ListTablesResponse(listTablesRequest.getCatalogName(), tables);
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        logger.info("doGetTable called with request {}:{}", getProjectName(getTableRequest),
                getTableRequest.getTableName());

        Schema tableSchema = getSchema(getProjectName(getTableRequest), getTableRequest.getTableName().getSchemaName(),
                getTableRequest.getTableName().getTableName());
        return new GetTableResponse(getProjectName(getTableRequest).toLowerCase(),
                getTableRequest.getTableName(), tableSchema);
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest getTableLayoutRequest)
    {
        logger.info("doGetTableLayout: Called for Table: {}:{} ", getProjectName(getTableLayoutRequest),
                getTableLayoutRequest.getTableName());

        //TODO:: Call GetTableSchema to determine if this table is partitioned or not. If it is partitioned, then we want to return
        //the partition. BigQuery ONLY supports time partitions. Not exactly sure how that would be defined (as a timestamp or a long?)
        Block partitions = BlockUtils.newBlock(blockAllocator, "partitionId", Types.MinorType.INT.getType(), 0);
        partitions.setRowCount(1);

        String projectName = getProjectName(getTableLayoutRequest);
        String datasetName = getTableLayoutRequest.getTableName().getSchemaName();
        return new GetTableLayoutResponse(projectName, new TableName(datasetName, getTableLayoutRequest.getTableName().getTableName()),
                partitions, new HashSet<>());
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
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
