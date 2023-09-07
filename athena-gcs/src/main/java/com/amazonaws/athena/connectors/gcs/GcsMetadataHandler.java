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
import com.amazonaws.athena.connectors.gcs.common.PartitionUtil;
import com.amazonaws.athena.connectors.gcs.storage.StorageMetadata;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.FILE_FORMAT;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_LOCATION_PREFIX;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.STORAGE_SPLIT_JSON;
import static java.util.Objects.requireNonNull;

public class GcsMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsMetadataHandler.class);
    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "gcs";
    private static final CharSequence GCS_FLAG = "google-cloud-storage-flag";
    private static final DatabaseFilter DB_FILTER = (Database database) -> (database.getLocationUri() != null && database.getLocationUri().contains(GCS_FLAG));
    // used to filter out Glue tables which lack indications of being used for GCS.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getStorageDescriptor().getLocation().startsWith(GCS_LOCATION_PREFIX);
    private final StorageMetadata datasource;
    private final AWSGlue glueClient;
    private final BufferAllocator allocator;

    public GcsMetadataHandler(BufferAllocator allocator, java.util.Map<String, String> configOptions) throws IOException
    {
        super(SOURCE_TYPE, configOptions);
        String gcsCredentialsJsonString = this.getSecret(configOptions.get(GCS_SECRET_KEY_ENV_VAR));
        this.datasource = new StorageMetadata(gcsCredentialsJsonString);
        this.glueClient = getAwsGlue();
        requireNonNull(glueClient, "Glue Client is null");
        this.allocator = allocator;
    }

    @VisibleForTesting
    protected GcsMetadataHandler(
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager awsSecretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        AWSGlue glueClient, BufferAllocator allocator,
        java.util.Map<String, String> configOptions) throws IOException
    {
        super(glueClient, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        String gcsCredentialsJsonString = this.getSecret(configOptions.get(GCS_SECRET_KEY_ENV_VAR));
        this.datasource = new StorageMetadata(gcsCredentialsJsonString);
        this.glueClient = getAwsGlue();
        requireNonNull(glueClient, "Glue Client is null");
        this.allocator = allocator;
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
        return super.doListSchemaNames(allocator, request, DB_FILTER);
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
        return super.doListTables(allocator,
                new ListTablesRequest(request.getIdentity(), request.getQueryId(),
                        request.getCatalogName(), request.getSchemaName(), null, UNLIMITED_PAGE_SIZE_VALUE),
                TABLE_FILTER);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request        Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request) throws Exception
    {
        GetTableResponse response = super.doGetTable(blockAllocator, request);
        //check whether schema added by user
        //return if schema present else fetch from files(dataset api)
        if (response != null && response.getSchema() != null && checkGlueSchema(response)) {
            return response;
        }
        else {
            LOGGER.warn("Fetching schema from google cloud storage files");
            //fetch schema from GCS in case user doesn't define it in glue table
            //this will get table(location uri and partition details) without schema metadata
            Table table = GcsUtil.getGlueTable(request.getTableName(), glueClient);
            //fetch schema from dataset api
            Schema schema = datasource.buildTableSchema(table, allocator);
            Map<String, String> columnNameMapping = getColumnNameMapping(table);
            List<Column> partitionKeys = table.getPartitionKeys() == null ? com.google.common.collect.ImmutableList.of() : table.getPartitionKeys();
            Set<String> partitionCols = partitionKeys.stream()
                .map(next -> columnNameMapping.getOrDefault(next.getName(), next.getName())).collect(Collectors.toSet());
            return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema, partitionCols);
        }
    }
    /**
     * Used to check whether user has added schema other than partition column.
     */
    private boolean checkGlueSchema(GetTableResponse response)
    {
        return (response.getSchema().getFields().stream().count() - response.getPartitionColumns().stream().count()) > 0;
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter        Used to write rows (partitions) into the Apache Arrow response.
     * @param request            Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws URISyntaxException
    {
        TableName tableInfo = request.getTableName();
        LOGGER.info("Retrieving partition for table {}.{}", tableInfo.getSchemaName(), tableInfo.getTableName());
        List<Map<String, String>> partitionFolders = datasource.getPartitionFolders(request.getSchema(), tableInfo, request.getConstraints(), glueClient);
        LOGGER.info("Partition folders in table {}.{} are \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), partitionFolders);
        for (Map<String, String> folder : partitionFolders) {
            blockWriter.writeRows((Block block, int rowNum) ->
            {
                for (Map.Entry<String, String> partition : folder.entrySet()) {
                    block.setValue(partition.getKey(), rowNum, partition.getValue());
                }
                //we wrote 1 row so we return 1
                return 1;
            });
        }
        LOGGER.info("Wrote partition?: {}", !partitionFolders.isEmpty());
    }

    /**
     * Used to split up the reads required to scan the requested batch of partition(s).
     * <p>
     * Here we execute the read operations files form particular GCS bucket
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
        int partitionContd = decodeContinuationToken(request);

        Table table = GcsUtil.getGlueTable(request.getTableName(), glueClient);
        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();

        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            //getting the partition folder name with bucket and file type
            URI locationUri = PartitionUtil.getPartitionsFolderLocationUri(table, partitions.getFieldVectors(), curPartition);
            LOGGER.info("Partition location {} ", locationUri);

            //getting storage file list
            List<String> fileList = datasource.getStorageSplits(locationUri);
            SpillLocation spillLocation = makeSpillLocation(request);
            LOGGER.info("Split list for {}.{} is \n{}", table.getDatabaseName(), table.getName(), fileList);

            //creating splits based folder
            String storageSplitJson = new ObjectMapper().writeValueAsString(fileList);
            LOGGER.info("MetadataHandler=GcsMetadataHandler|Method=doGetSplits|Message=StorageSplit JSON\n{}",
                    storageSplitJson);
            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(FILE_FORMAT, table.getParameters().get(CLASSIFICATION_GLUE_TABLE_PARAM))
                    .add(STORAGE_SPLIT_JSON, storageSplitJson);

            // set partition column name and value in split
            for (FieldVector fieldVector : partitions.getFieldVectors()) {
                fieldVector.getReader().setPosition(curPartition);
                if (fieldVector.getName().equalsIgnoreCase(FILE_FORMAT) || fieldVector.getName().equalsIgnoreCase(STORAGE_SPLIT_JSON)) {
                    throw new RuntimeException("column name is same as metadata");
                }
                splitBuilder.add(fieldVector.getName(), fieldVector.getReader().readObject().toString());
            }
            splits.add(splitBuilder.build());

            if (splits.size() >= GcsConstants.MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(request.getCatalogName(), splits, String.valueOf(curPartition + 1));
            }
            LOGGER.info("Splits created {}", splits);
        }
        LOGGER.info("doGetSplits: exit - {}", splits.size());
        return new GetSplitsResponse(catalogName, splits);
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
