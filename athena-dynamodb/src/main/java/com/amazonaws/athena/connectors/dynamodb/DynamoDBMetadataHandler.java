package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
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
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.PARTITION_TYPE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.PROVISIONED_READ_CAPACITY_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.QUERY_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.RANGE_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SCAN_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class DynamoDBMetadataHandler
        extends GlueMetadataHandler
{
    static final String DEFAULT_SCHEMA = "default";
    static final int MAX_SPLITS_PER_REQUEST = 1000;
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMetadataHandler.class);
    private static final String sourceType = "ddb";
    private static final String GLUE_ENV = "disable_glue";
    private final AmazonDynamoDB ddbClient;
    private final AWSGlue glueClient;

    public DynamoDBMetadataHandler()
    {
        super((System.getenv(GLUE_ENV) == null && Boolean.parseBoolean(System.getenv(GLUE_ENV))) ? AWSGlueClientBuilder.standard().build() : null,
                sourceType);
        ddbClient = AmazonDynamoDBClientBuilder.standard().build();
        glueClient = getAwsGlue();
    }

    @VisibleForTesting
    DynamoDBMetadataHandler(EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix,
            AmazonDynamoDB ddbClient,
            AWSGlue glueClient)
    {
        super(glueClient, keyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
        this.glueClient = glueClient;
        this.ddbClient = ddbClient;
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception
    {
        if (glueClient != null) {
            try {
                return super.doListSchemaNames(allocator, request);
            }
            catch (RuntimeException e) {
                logger.debug("doListSchemaNames: Unable to retrieve schemas from AWSGlue.", e);
            }
        }

        return new ListSchemasResponse(request.getCatalogName(), ImmutableList.of("default"));
    }

    /*
    TODO add table whitelist
     */
    @Override
    protected ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
            throws Exception
    {
        // LinkedHashSet for consistent ordering
        Set<TableName> combinedTables = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                // does not validate that the tables are actually DDB tables
                combinedTables.addAll(super.doListTables(allocator, request).getTables());
            }
            catch (RuntimeException e) {
                logger.debug("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }

        // add tables that may not be in Glue (if listing the default schema)
        if (DEFAULT_SCHEMA.equals(request.getSchemaName())) {
            List<TableName> tablesFromDDB = new ArrayList<>();
            String nextToken = null;
            do {
                com.amazonaws.services.dynamodbv2.model.ListTablesRequest ddbRequest = new com.amazonaws.services.dynamodbv2.model.ListTablesRequest()
                        .withExclusiveStartTableName(nextToken);
                ListTablesResult result = ddbClient.listTables(ddbRequest);
                tablesFromDDB.addAll(result.getTableNames().stream().map(table -> new TableName(DEFAULT_SCHEMA, table)).collect(toImmutableList()));
                nextToken = result.getLastEvaluatedTableName();
            }
            while (nextToken != null);
            combinedTables.addAll(tablesFromDDB);
        }
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables));
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
            throws Exception
    {
        if (glueClient != null) {
            try {
                // does not validate that the table is actually a DDB table
                return super.doGetTable(allocator, request);
            }
            catch (RuntimeException e) {
                logger.debug("doGetTable: Unable to retrieve table {} from AWSGlue in database/schema {}", request.getTableName().getSchemaName(), e);
            }
        }

        // ignore database/schema name since there are no databases/schemas in DDB
        Schema schema = DDBTableUtils.peekTableForSchema(request.getTableName().getTableName(), ddbClient);
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    /*
    TODO calculate filter expressions here to avoid recalculating them at the split level
     */
    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator allocator, GetTableLayoutRequest request)
    {
        DynamoDBTable table = DDBTableUtils.getTable(request.getTableName().getTableName(), ddbClient);
        Map<String, ValueSet> summary = request.getConstraints().getSummary();
        DynamoDBTable index = DDBTableUtils.getBestIndexForPredicates(table, summary);
        String hashKeyName = index.getHashKey();
        ValueSet hashKeyValueSet = summary.get(hashKeyName);
        SchemaBuilder partitionsSchemaBuilder = SchemaBuilder.newBuilder();
        partitionsSchemaBuilder.addMetadata(PROVISIONED_READ_CAPACITY_METADATA, String.valueOf(table.getProvisionedReadCapacity()));
        Block partitions = null;
        List<Object> hashKeyValues = ImmutableList.of();
        if (hashKeyValueSet != null) {
            hashKeyValues = DDBTableUtils.getHashKeyAttributeValues(hashKeyValueSet);
            if (!hashKeyValues.isEmpty()) {
                // can "partition" on hash key
                partitionsSchemaBuilder.addField(hashKeyName, hashKeyValueSet.getType());
                partitionsSchemaBuilder.addMetadata(HASH_KEY_NAME_METADATA, hashKeyName);
                index.getRangeKey().ifPresent((rangeKeyName) -> partitionsSchemaBuilder.addMetadata(RANGE_KEY_NAME_METADATA, rangeKeyName));
                partitionsSchemaBuilder.addMetadata(PARTITION_TYPE_METADATA, QUERY_PARTITION_TYPE);
                if (!table.equals(index)) {
                    partitionsSchemaBuilder.addMetadata(INDEX_METADATA, index.getName());
                }
                Schema partitionsSchema = partitionsSchemaBuilder.build();
                partitions = allocator.createBlock(partitionsSchema);
                int partitionCount = 0;
                for (Object hashKeyValue : hashKeyValues) {
                    BlockUtils.setValue(partitions.getFieldVector(hashKeyName), partitionCount++, hashKeyValue);
                }
                partitions.setRowCount(partitionCount);
            }
        }
        // always fall back to a scan
        if (partitions == null) {
            partitionsSchemaBuilder.addField(SEGMENT_COUNT_METADATA, Types.MinorType.INT.getType());
            partitionsSchemaBuilder.addMetadata(PARTITION_TYPE_METADATA, SCAN_PARTITION_TYPE);
            Schema partitionsSchema = partitionsSchemaBuilder.build();

            // need to return at least one partition so stick the segment count in it
            partitions = allocator.createBlock(partitionsSchema);
            int segmentCount = DDBTableUtils.getNumSegments(table.getProvisionedReadCapacity(), table.getApproxTableSizeInBytes());
            BlockUtils.setValue(partitions.getFieldVector(SEGMENT_COUNT_METADATA), 0, segmentCount);
            partitions.setRowCount(1);
        }

        return new GetTableLayoutResponse(request.getCatalogName(), request.getTableName(), partitions,
                hashKeyValues.isEmpty() ? ImmutableSet.of() : ImmutableSet.of(hashKeyName));
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        int partitionContd = decodeContinuationToken(request);
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        Map<String, String> customMetadata = partitions.getSchema().getCustomMetadata();
        String partitionType = customMetadata.get(PARTITION_TYPE_METADATA);
        if (partitionType == null) {
            throw new IllegalStateException(String.format("No metadata %s defined in Schema %s", PARTITION_TYPE_METADATA, partitions.getSchema()));
        }
        if (QUERY_PARTITION_TYPE.equals(partitionType)) {
            String hashKeyName = Iterables.getOnlyElement(request.getPartitionCols());
            FieldReader hashKeyValueReader = partitions.getFieldReader(hashKeyName);
            for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
                hashKeyValueReader.setPosition(curPartition);

                //Every split must have a unique location if we wish to spill to avoid failures
                SpillLocation spillLocation = makeSpillLocation(request);

                Object hashKeyValue = convertArrowTypeIfNecessary(hashKeyValueReader.readObject());
                String hashKeyValueJSON = Jackson.toJsonString(ItemUtils.toAttributeValue(hashKeyValue));

                // one split per hash key value (since one DDB query can only take on hash key value)
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(HASH_KEY_NAME_METADATA, hashKeyName)
                        .add(hashKeyName, hashKeyValueJSON)
                        // also encode the index that contains the hash key column (could be the original table)
                        .add(INDEX_METADATA, customMetadata.get(INDEX_METADATA))
                        .add(SEGMENT_COUNT_METADATA, String.valueOf(partitions.getRowCount()));
                splits.add(splitBuilder.build());

                if (splits.size() == MAX_SPLITS_PER_REQUEST && curPartition != partitions.getRowCount() - 1) {
                    // We've reached max page size and this is not the last partition
                    // so send the page back
                    return new GetSplitsResponse(request.getCatalogName(),
                            splits,
                            encodeContinuationToken(curPartition));
                }
            }
            return new GetSplitsResponse(request.getCatalogName(), splits, null);
        }
        else if (SCAN_PARTITION_TYPE.equals(partitionType)) {
            FieldReader segmentCountReader = partitions.getFieldReader(SEGMENT_COUNT_METADATA);
            int segmentCount = segmentCountReader.readInteger();
            for (int curPartition = partitionContd; curPartition < segmentCount; curPartition++) {
                SpillLocation spillLocation = makeSpillLocation(request);
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(SEGMENT_ID_PROPERTY, String.valueOf(curPartition))
                        .add(SEGMENT_COUNT_METADATA, String.valueOf(segmentCount));
                splits.add(splitBuilder.build());

                if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                    return new GetSplitsResponse(request.getCatalogName(),
                            splits,
                            encodeContinuationToken(curPartition));
                }
            }
            return new GetSplitsResponse(request.getCatalogName(), splits, null);
        }
        else {
            throw new IllegalStateException("Unexpected partition type " + partitionType);
        }
    }

    @Override
    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken()) + 1;
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }

    private Object convertArrowTypeIfNecessary(Object object)
    {
        if (object instanceof Text) {
            return object.toString();
        }
        return object;
    }
}
