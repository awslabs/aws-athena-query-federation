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

import com.amazonaws.SDKGlobalConfiguration;
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
import com.amazonaws.athena.connectors.gcs.common.StorageObject;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.athena.connectors.gcs.storage.StorageMetadata;
import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import com.amazonaws.athena.connectors.gcs.storage.TableListResult;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageTable;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_CREDENTIAL_KEYS_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.STORAGE_SPLIT_JSON;
import static com.amazonaws.athena.connectors.gcs.GcsSchemaUtils.buildTableSchema;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.getGcsCredentialJsonString;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.splitAsJson;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.IS_TABLE_PARTITIONED;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory.createDatasource;
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
    private final StorageMetadata datasource;

    public GcsMetadataHandler() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        super(SOURCE_TYPE);
        String gcsCredentialsJsonString = getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR)), GCS_CREDENTIAL_KEYS_ENV_VAR);
        this.datasource = createDatasource(gcsCredentialsJsonString, System.getenv());
    }

    @VisibleForTesting
    @SuppressWarnings("unused")
    protected GcsMetadataHandler(EncryptionKeyFactory keyFactory,
                                 AWSSecretsManager awsSecretsManager,
                                 AmazonAthena athena,
                                 String spillBucket,
                                 String spillPrefix,
                                 AmazonS3 amazonS3) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        String gcsCredentialsJsonString = getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR)), GCS_CREDENTIAL_KEYS_ENV_VAR);
        this.datasource = createDatasource(gcsCredentialsJsonString, System.getenv());
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
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
    public ListTablesResponse doListTables(BlockAllocator allocator, final ListTablesRequest request) throws Exception
    {
        List<TableName> tables = new ArrayList<>();
        String nextToken;
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=Fetching list of tables with page size {} and token {} for scheme {}",
                request.getPageSize(), request.getNextToken(), request.getSchemaName());
        TableListResult result = datasource.getAllTables(request.getSchemaName(), request.getNextToken(),
                request.getPageSize());
        nextToken = result.getNextToken();
        List<StorageObject> tableNames = result.getTables();
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doListTables|Message=tables under schema {} are: {}",
                request.getSchemaName(), tableNames);
        tableNames.forEach(storageObject -> tables.add(new TableName(request.getSchemaName(), storageObject.getTableName())));
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
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request) throws Exception
    {
        TableName tableInfo = request.getTableName();
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetTable|Message=queryId {}",
                request.getQueryId());
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetTable|Message=Schema name {}, table name {}",
                tableInfo.getSchemaName(), tableInfo.getTableName());
        datasource.loadAllTables(tableInfo.getSchemaName());
        LOGGER.debug(MessageFormat.format("Running doGetTable for table {0}, in schema {1} ",
                tableInfo.getTableName(), tableInfo.getSchemaName()));
        Schema schema = buildTableSchema(this.datasource,
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
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception
    {
        LOGGER.debug("Handler=GcsMetadataHandler|Method=getPartitions|Message=queryId {}", request.getQueryId());
        LOGGER.info("readWithConstraint: schema[{}] tableName[{}]", request.getSchema(), request.getTableName());
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
        LOGGER.info("Getting storage table for {} under bucket {}", objectName, bucketName);
        requireNonNull(bucketName, "Schema + '" + tableName.getSchemaName() + "' not found");
        requireNonNull(objectName, "Table '" + tableName.getTableName() + "' not found under schema '"
                + tableName.getSchemaName() + "'");

        List<StoragePartition> partitions = datasource.getStoragePartitions(request.getSchema(), request.getTableName(), request.getConstraints(), bucketName, objectName);
        LOGGER.info("GcsMetadataHandler.getPartitions() -> Storage partitions:\n{}", partitions);
        requireNonNull(partitions, "List of partition can't be retrieve from metadata");
        int counter = 0;
        for (int i = 0; i < partitions.size(); i++) {
            final int currentIndex = i;
            blockWriter.writeRows((Block block, int rowNum) ->
            {
                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, String.valueOf(currentIndex));
                //we wrote 1 row so we return 1
                return 1;
            });
            counter++;
        }
        LOGGER.debug("Total partition rows written: {}", counter);
    }

    /**
     * Used to split up the reads required to scan the requested batch of partition(s).
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
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws Exception
    {
        LOGGER.debug("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=queryId {}", request.getQueryId());
        String bucketName = "";
        String objectName = "";
        boolean partitioned = false;
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
            partitioned = Boolean.parseBoolean(table.getParameters().get(IS_TABLE_PARTITIONED));
        }
        LOGGER.debug("Object {} under bucket {} is partitioned? {}", objectName, bucketName, partitioned);
        LOGGER.debug("Block partition @ doGetSplits \n{}", partitioned);
        Block partitions = request.getPartitions();
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Partition block {}", partitions);
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Block partition row count {}",
                partitions.getRowCount());
        Set<Split> splits = new HashSet<>();
        int partitionContd = decodeContinuationToken(request);
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Start splitting from position {}",
                partitionContd);
        List<StoragePartition> storagePartitions = datasource.getStoragePartitions(request.getSchema(), request.getTableName(), request.getConstraints(), bucketName, objectName);
        int startSplitIndex = 0;
        LOGGER.info("Current split start index {}", startSplitIndex);
        for (int curPartition = 0; curPartition < partitions.getRowCount(); curPartition++) {
            SpillLocation spillLocation = makeSpillLocation(request);
            StoragePartition partition = storagePartitions.get(curPartition);
            List<StorageSplit> storageSplits = datasource.getSplitsByBucketPrefix(bucketName, bucketName + "/" + partition.getLocation(),
                    partitioned, request.getConstraints());
            LOGGER.info("Splitting based on partition at position {}", curPartition);
            for (StorageSplit split : storageSplits) {
                String storageSplitJson = splitAsJson(split);
                LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=StorageSplit JSO\n{}",
                        storageSplitJson);
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(STORAGE_SPLIT_JSON, storageSplitJson);
                splits.add(splitBuilder.build());
                if (splits.size() >= GcsConstants.MAX_SPLITS_PER_REQUEST) {
                    //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                    return new GetSplitsResponse(request.getCatalogName(), splits, String.valueOf(curPartition + 1));
                }
            }
            LOGGER.info("Splits created {}", splits);
        }
        return new GetSplitsResponse(request.getCatalogName(), splits, null);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.parseInt(request.getContinuationToken());
        }
        //No continuation token present
        return 0;
    }
}
