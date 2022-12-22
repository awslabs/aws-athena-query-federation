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
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
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
import com.amazonaws.athena.connectors.gcs.common.PartitionFolder;
import com.amazonaws.athena.connectors.gcs.common.PartitionLocation;
import com.amazonaws.athena.connectors.gcs.common.PartitionResult;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.athena.connectors.gcs.glue.GlueUtil;
import com.amazonaws.athena.connectors.gcs.glue.HivePartitionResolver;
import com.amazonaws.athena.connectors.gcs.storage.StorageMetadata;
import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_CREDENTIAL_KEYS_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.STORAGE_SPLIT_JSON;
import static com.amazonaws.athena.connectors.gcs.GcsSchemaUtils.buildTableSchema;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.getGcsCredentialJsonString;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.splitAsJson;
import static com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory.createDatasource;

public class GcsMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsMetadataHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "gcs";
    private final StorageMetadata datasource;
    // Env variable name used to indicate that we want to disable the use of Glue DataCatalog for supplemental
    // metadata and instead rely solely on the connector's schema inference capabilities.
    private static final String GLUE_ENV = "disable_glue";
    private final AWSGlue glueClient;
    private static final CharSequence GCS_FLAG = "gcs";
    private static final String DEFAULT_SCHEMA = "default";
    private static final DatabaseFilter DB_FILTER = (Database database) -> (database.getLocationUri() != null && database.getLocationUri().contains(GCS_FLAG));
    public static final String TABLE_FILTER_IDENTIFIER = "gs://";
    // used to filter out Glue tables which lack indications of being used for DDB.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getStorageDescriptor().getLocation().contains(TABLE_FILTER_IDENTIFIER)
            || (table.getParameters() != null && GcsUtil.isSupportedFileType(table.getParameters().get("classification")))
            || (table.getStorageDescriptor().getParameters() != null && GcsUtil.isSupportedFileType(table.getStorageDescriptor().getParameters().get("classification")));

    public GcsMetadataHandler() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        super((System.getenv(GLUE_ENV) != null && !"false".equalsIgnoreCase(System.getenv(GLUE_ENV))), SOURCE_TYPE);
        String gcsCredentialsJsonString = getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR)), GCS_CREDENTIAL_KEYS_ENV_VAR);
        this.datasource = createDatasource(gcsCredentialsJsonString, System.getenv());
        this.glueClient = getAwsGlue();
    }

    @VisibleForTesting
    @SuppressWarnings("unused")
    protected GcsMetadataHandler(EncryptionKeyFactory keyFactory,
                                 AWSSecretsManager awsSecretsManager,
                                 AmazonAthena athena,
                                 String spillBucket,
                                 String spillPrefix,
                                 AmazonS3 amazonS3, AWSGlue glueClient) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        super(glueClient, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        String gcsCredentialsJsonString = getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR)), GCS_CREDENTIAL_KEYS_ENV_VAR);
        this.datasource = createDatasource(gcsCredentialsJsonString, System.getenv());
        this.glueClient = getAwsGlue();
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
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request) throws Exception
    {
        Set<String> schema = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                schema.addAll(super.doListSchemaNames(allocator, request, DB_FILTER).getSchemas());
            }
            catch (RuntimeException e) {
                LOGGER.warn("doListSchemaNames: Unable to retrieve schemas from AWSGlue.", e);
            }
        }
        return new ListSchemasResponse(request.getCatalogName(), schema);
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
        Set<TableName> tables = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                tables.addAll(super.doListTables(allocator,
                        new ListTablesRequest(request.getIdentity(), request.getQueryId(), request.getCatalogName(),
                                request.getSchemaName(), null, UNLIMITED_PAGE_SIZE_VALUE),
                        TABLE_FILTER).getTables());
            }
            catch (RuntimeException e) {
                LOGGER.warn("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(tables), null);
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
        if (glueClient != null) {
            try {
                return super.doGetTable(blockAllocator, request);
            }
            catch (RuntimeException e) {
                LOGGER.warn("doGetTable: Unable to retrieve table {} from AWSGlue in database/schema {}. " +
                                "Falling back to schema inference. If inferred schema is incorrect, create " +
                                "a matching table in Glue to define schema (see README)",
                        request.getTableName().getTableName(), request.getTableName().getSchemaName(), e);
            }
        }
        Schema schema = buildTableSchema(this.datasource,
                tableInfo.getSchemaName(),
                tableInfo.getTableName());
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter        Used to write rows (partitions) into the Apache Arrow response.
     * @param request            Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws ParseException
    {
        TableName tableInfo = request.getTableName();
        LOGGER.info("Retrieving partition for table {}.{}", tableInfo.getSchemaName(), tableInfo.getTableName());
        Table table = GlueUtil.getGlueTable(request, tableInfo, glueClient);

        List<PartitionFolder> partitionFolders = datasource.getPartitionFolders(request, request.getSchema(), tableInfo, request.getConstraints(), glueClient);
        LOGGER.info("Partition folders in table {}.{} are \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), partitionFolders);
        if (!partitionFolders.isEmpty()) {
            for (PartitionFolder folder : partitionFolders) {
                blockWriter.writeRows((Block block, int rowNum) ->
                {
                    for (StoragePartition partition : folder.getPartitions()) {
                        block.setValue(partition.getColumnName(), rowNum, partition.getColumnValue());
                    }
                    //we wrote 1 row so we return 1
                    return 1;
                });
            }
        }
        LOGGER.info("Wrote partition?: {}", !partitionFolders.isEmpty());
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
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=queryId {}", request.getQueryId());
        TableName tableInfo = request.getTableName();
        Set<Split> splits = new HashSet<>();
        int partitionContd = decodeContinuationToken(request);
        LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=Start splitting from position {}",
                partitionContd);
        PartitionResult partitionResult = new HivePartitionResolver().getPartitions(request, request.getSchema(), request.getTableName(), request.getConstraints(), glueClient);
        String tableType = partitionResult.getTableType();
        List<PartitionLocation> partitionLocations = partitionResult.getPartitions();
        for (int curPartition = 0; curPartition < partitionLocations.size(); curPartition++) {
            PartitionLocation partition = partitionLocations.get(curPartition);
            LOGGER.info("Partition for {}.{} at position {} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), curPartition + 1, partition);
            List<StorageSplit> storageSplits = datasource.getStorageSplits(tableType, partition);
            SpillLocation spillLocation = makeSpillLocation(request);
            LOGGER.info("Split list for {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), storageSplits);
            for (StorageSplit split : storageSplits) {
                String storageSplitJson = splitAsJson(split);
                LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=StorageSplit JSON\n{}",
                        storageSplitJson);
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(CLASSIFICATION_GLUE_TABLE_PARAM, tableType)
                        .add(STORAGE_SPLIT_JSON, storageSplitJson);
                splits.add(splitBuilder.build());
            }
            if (splits.size() >= GcsConstants.MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(request.getCatalogName(), splits, String.valueOf(curPartition + 1));
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
