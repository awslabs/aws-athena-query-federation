/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.StorageTable;
import com.amazonaws.athena.storage.TableListResult;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.STORAGE_SPLIT_JSON;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.getGcsCredentialJsonString;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.splitAsJson;
import static com.amazonaws.athena.storage.StorageConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.storage.datasource.StorageDatasourceFactory.createDatasource;
import static java.util.Objects.requireNonNull;

public class GcsMetadataHandler
        extends MetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsMetadataHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "gcs";
    private final GcsSchemaUtils gcsSchemaUtils;
    private final StorageDatasource datasource;

    public GcsMetadataHandler() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        super(SOURCE_TYPE);
        gcsSchemaUtils = new GcsSchemaUtils();
        this.datasource = createDatasource(getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR))), System.getenv());
    }

    @VisibleForTesting
    @SuppressWarnings("unused")
    protected GcsMetadataHandler(EncryptionKeyFactory keyFactory,
                                 AWSSecretsManager awsSecretsManager,
                                 AmazonAthena athena,
                                 String spillBucket,
                                 String spillPrefix,
                                 GcsSchemaUtils gcsSchemaUtils,
                                 AmazonS3 amazonS3) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.gcsSchemaUtils = gcsSchemaUtils;
        this.datasource = createDatasource(getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR))), System.getenv());
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        LOGGER.debug("doListSchemaNames: {}", request.getCatalogName());
        List<String> schemas = datasource.getAllDatabases();
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, final ListTablesRequest request)
    {
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=queryId {}",
                request.getQueryId());
        LOGGER.debug("doListTables: {}", request);
        List<TableName> tables = new ArrayList<>();
        String nextToken;
        try {
            LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=Fetching list of tables with page size {} and token {} for scheme {}",
                    request.getPageSize(), request.getNextToken(), request.getSchemaName());
            TableListResult result = datasource.getAllTables(request.getSchemaName(), request.getNextToken(),
                    request.getPageSize());
            nextToken = result.getNextToken();
            List<String> tableNames = result.getTables();
            LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=tables under schema {} are: {}",
                    request.getSchemaName(), tableNames);
            tableNames.forEach(name -> tables.add(new TableName(request.getSchemaName(), name)));
        }
        catch (Exception exception) {
            LOGGER.error("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=Exception occurred in GcsMetadataHandler.doListTables {}",
                    exception.getMessage());
            exception.printStackTrace();
            throw new RuntimeException("Exception occurred in GcsMetadataHandler.doListTables: " + exception.getMessage(), exception);
        }
        return new ListTablesResponse(request.getCatalogName(), tables, nextToken);
    }

    /**
     * Returns a schema with partition colum of type VARCHAR
     *
     * @return An instance of {@link Schema}
     */
    public Schema getPartitionSchema()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws IOException
    {
        TableName tableInfo = request.getTableName();
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetTable|Message=queryId {}",
                request.getQueryId());
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetTable|Message=Schema name {}, table name {}",
                tableInfo.getSchemaName(), tableInfo.getTableName());
        datasource.loadAllTables(tableInfo.getSchemaName());
        LOGGER.debug(MessageFormat.format("Running doGetTable for table {0}, in schema {1} ",
                tableInfo.getTableName(), tableInfo.getSchemaName()));
        Schema schema = gcsSchemaUtils.buildTableSchema(this.datasource,
                tableInfo.getSchemaName(),
                tableInfo.getTableName());
            Schema partitionSchema = getPartitionSchema();
            return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema,
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter        Used to write rows (partitions) into the Apache Arrow response.
     * @param request            Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws IOException
    {
        LOGGER.info("GetTableLayoutRequest: \n{}", request);
        LOGGER.debug("RecordHandler=GcsMetadataHandler|Method=getPartitions|Message=queryId {}", request.getQueryId());
        LOGGER.debug("readWithConstraint: schema[{}] tableName[{}]", request.getSchema(), request.getTableName());
        TableName tableName = request.getTableName();
        String bucketName = null;
        String objectName = null;
        Optional<StorageTable> optionalTable = datasource.getStorageTable(tableName.getSchemaName(),
                tableName.getTableName());
        if (optionalTable.isPresent()) {
            StorageTable table = optionalTable.get();
            bucketName = table.getParameters().get(TABLE_PARAM_BUCKET_NAME);
            objectName = table.getParameters().get(TABLE_PARAM_OBJECT_NAME);
        }
        requireNonNull(bucketName, "Schema + '" + tableName.getSchemaName() + "' not found");
        requireNonNull(objectName, "Table '" + tableName.getTableName() + "' not found under schema '"
                + tableName.getSchemaName() + "'");

        List<StoragePartition> storagePartition = datasource.getStoragePartitions(request.getSchema(), request.getTableName(), request.getConstraints(), bucketName, objectName);
        LOGGER.info("Storage partitions: \n{}", storagePartition);
        requireNonNull(storagePartition, "List of partition can't be retrieve from metadata");
        //this.datasource.loadAllTables(tableName.getSchemaName());
        int counter = 0;
        for (int i = 0; i < storagePartition.size(); i++) {
            final String splitIndex = Integer.toString(i);
            blockWriter.writeRows((Block block, int rowNum) ->
            {
                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, splitIndex);
                //we wrote 1 row so we return 1
                return 1;
            });
            counter++;
        }
        LOGGER.debug("Total partition rows written: {}", counter);
    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     * <p>
     * Here we execute the read operations based on row offset and limit form particular bucket and file on GCS
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details of the catalog, database, table, and partition(s) being queried as well as
     *                  any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws IOException
    {
        LOGGER.info("GetSplitsRequest: \n{}", request);
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=queryId {}", request.getQueryId());
        String bucketName = "";
        String objectName = "";
        TableName tableInfo = request.getTableName();
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Schema name{}, table name {}",
                tableInfo.getSchemaName(), tableInfo.getTableName());
        datasource.loadAllTables(tableInfo.getSchemaName());
        Optional<StorageTable> optionalTable = datasource.getStorageTable(tableInfo.getSchemaName(),
                tableInfo.getTableName());
        if (optionalTable.isPresent()) {
            StorageTable table = optionalTable.get();
            bucketName = table.getParameters().get(TABLE_PARAM_BUCKET_NAME);
            objectName = table.getParameters().get(TABLE_PARAM_OBJECT_NAME);
        }
        List<StoragePartition> storagePartitions = datasource.getByObjectNameInBucket(objectName, bucketName,
                request.getSchema(), request.getTableName(), request.getConstraints());
        requireNonNull(storagePartitions, "List of partitions can't be retrieve from metadata");

        Block partitions = request.getPartitions();
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Partition block {}", partitions);
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Block partition row count {}",
                partitions.getRowCount());
        Set<Split> splits = new HashSet<>();
        final ObjectMapper objectMapper = new ObjectMapper();

        int partitionContd = decodeContinuationToken(request);
        List<Integer> storageSplitListIndices = getSplitIndices(partitions);
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Start splitting from position {}",
                partitionContd);
        if (storageSplitListIndices.isEmpty()) {
            LOGGER.debug("No more storage split indices, returning empty split with null continuation token");
            return new GetSplitsResponse(request.getCatalogName(), splits, null);
        }
        int startSplitIndex = storageSplitListIndices.get(0);
        LOGGER.info("Current split start index {}", startSplitIndex);
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            int currentSplitIndex = startSplitIndex + curPartition;
            SpillLocation spillLocation = makeSpillLocation(request);
            StoragePartition storagePartition = storagePartitions.get(currentSplitIndex);
            List<StorageSplit> storageSplits = datasource.getSplitsByStoragePartition(storagePartition);
            for (StorageSplit split : storageSplits) {
                LOGGER.info("Splits \n{} found under the partition\n{}", split, storagePartition);
                String storageSplitJson = splitAsJson(split);
                LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=StorageSplit JSO\n{}",
                        storageSplitJson);

                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(currentSplitIndex))
                        .add(TABLE_PARAM_BUCKET_NAME, bucketName)
                        .add(TABLE_PARAM_OBJECT_NAME, objectName)
                        .add(STORAGE_SPLIT_JSON, storageSplitJson);
                splits.add(splitBuilder.build());
                if (splits.size() >= GcsConstants.MAX_SPLITS_PER_REQUEST) {
                    //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                    return new GetSplitsResponse(request.getCatalogName(), splits, String.valueOf(curPartition + 1));
                }
            }
        }
        return new GetSplitsResponse(request.getCatalogName(), splits, null);
    }

    // helpers
    /**
     * Decodes continuation token (if any)
     *
     * @param request An instance of {@link GetSplitsRequest}
     * @return Continuation token if found, 0 otherwise
     */
    private int decodeContinuationToken(GetSplitsRequest request)
    {
        LOGGER.debug("Decoding ContinuationToken");
        if (request.hasContinuationToken()) {
            LOGGER.debug("Found decoding ContinuationToken: " + request.getContinuationToken());
            return Integer.parseInt(request.getContinuationToken());
        }
        //No continuation token present
        LOGGER.debug("Not decoding ContinuationTokens found. Returning 0");
        return 0;
    }

    /**
     * Parses Block's toString representation to obtain list of StorageSplit indices to get the StorageSplit
     *
     * @param block An instance of Block. It's toString representation should look like the following
     *              <p>Block{rows=10, part_name=[20, 21, 22, 23, 24, 25, 26, 27, 28, 29]}</p>
     * @return List of split indices
     */
    private List<Integer> getSplitIndices(Block block)
    {
        String blockString = block.toString();
        if (blockString == null || blockString.isBlank()) {
            return List.of();
        }
        int index = blockString.indexOf("[");
        if (index > -1) {
            int toIndex = blockString.lastIndexOf("]");
            if (toIndex > -1) {
                List<String> stringIndices = Arrays.asList(blockString.substring(index + 1, toIndex).split(", "));
                return stringIndices.stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
            }
        }
        return List.of();
    }

    /**
     * @param request       An instance of {@link GetSplitsRequest}
     * @param bucketName    Name of the bucket
     * @param objectName    Name of the storage object (file)
     * @param storageSplits A list of {@link StorageSplit}
     * @param splits        A set of {@link Split}
     * @param objectMapper  An instance of {@link ObjectMapper}
     * @return An instance of {@link GetSplitsResponse}
     */
    private GetSplitsResponse getSingleSplit(GetSplitsRequest request, String bucketName, String objectName, List<StorageSplit> storageSplits,
                                             Set<Split> splits, ObjectMapper objectMapper)
    {
        LOGGER.debug("RecordHandler=GcsMetadataHandler|Method=doGetSplits|Message=No partitions, returning single Split");
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Block partition row count is 0, returning default partition");
        StorageSplit storageSplit = storageSplits.get(0);
        Split split;
        try {
            String storageSplitJson = Arrays.toString(objectMapper.writeValueAsBytes(storageSplit));
            LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=StorageSplit JSON\n{}", storageSplitJson);
            split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add(BLOCK_PARTITION_COLUMN_NAME, GcsConstants.ALL_PARTITIONS)
                    .add(TABLE_PARAM_BUCKET_NAME, bucketName)
                    .add(TABLE_PARAM_OBJECT_NAME, objectName)
                    .add(STORAGE_SPLIT_JSON, storageSplitJson)
                    .build();
        }
        catch (JsonProcessingException exception) {
            throw new RuntimeException("Error: " + exception.getMessage()
                    + "Storage split couldn't be deserialize as JSOon. StorageSplit is "
                    + storageSplit);
        }
        splits.add(split);
        return new GetSplitsResponse(request.getCatalogName(), split);
    }
}
