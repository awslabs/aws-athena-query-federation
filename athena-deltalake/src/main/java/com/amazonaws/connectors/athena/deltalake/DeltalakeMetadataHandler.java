/*-
 * #%L
 * athena-deltalake
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.amazonaws.connectors.athena.deltalake.protocol.DeltaLogAction;
import com.amazonaws.connectors.athena.deltalake.protocol.DeltaTableSnapshotBuilder;
import com.amazonaws.connectors.athena.deltalake.protocol.DeltaTableSnapshotBuilder.DeltaTableSnapshot;
import com.amazonaws.connectors.athena.deltalake.protocol.DeltaTableStorage;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.connectors.athena.deltalake.converter.DeltaConverter.castPartitionValue;
import static com.amazonaws.connectors.athena.deltalake.converter.DeltaConverter.getArrowSchema;

/**
 * Handles metadata requests for the Athena Deltalake Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Uses delta log informations to resolve the tables schema, partitions and files.
 */
public class DeltalakeMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeMetadataHandler.class);

    public static final String SPLIT_FILE_PROPERTY = "file";
    public static final String SPLIT_PARTITION_VALUES_PROPERTY = "partitions_values";

    protected static final int MAX_SPLITS_PER_REQUEST = 1000;

    protected static final String ENHANCED_PARTITION_VALUES_COLUMN = "__reserved_partitionValues__";
    protected static final String ENHANCED_FILE_PATH_COLUMN = "__reserved_filePath__";

    private static final String SOURCE_TYPE = "deltalake";
    public static final String S3_FOLDER_SUFFIX = "_$folder$";
    public static final String S3_FOLDER_DELIMITER = "/";

    private String dataBucket;
    private final AmazonS3 amazonS3;

    private class ListFoldersResult
    {
        Stream<String> listedFolders;
        String nextContinuationToken;

        public ListFoldersResult(Stream<String> listedFolders, String nextContinuationToken)
        {
            this.listedFolders = listedFolders;
            this.nextContinuationToken = nextContinuationToken;
        }
    }

    public DeltalakeMetadataHandler(String dataBucket)
    {
        super(SOURCE_TYPE);
        this.amazonS3 = AmazonS3ClientBuilder.defaultClient();
        this.dataBucket = dataBucket;
    }

    @VisibleForTesting
    protected DeltalakeMetadataHandler(AmazonS3 amazonS3,
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager awsSecretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        String dataBucket)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.amazonS3 = amazonS3;
        this.dataBucket = dataBucket;
    }

    /**
     * Returns a JSON string representing a map of partition values
     * @param partitionValues Map of partition values
     * @return A JSON string
     * @throws JsonProcessingException
     */
    protected String serializePartitionValues(Map<String, String> partitionValues) throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(partitionValues);
    }

    private ListFoldersResult listFolders()
    {
        return listFolders("");
    }

    private ListFoldersResult listFolders(String prefix)
    {
        return listFolders(prefix, null, null);
    }

    /**
     * List all the folders inside a root folder.
     * This lookup relies on the existence of files named after the name of the folders, following this convention:
     * "<folder_name>_$folder$"
     * @param prefix Prefix key filter, i.e the folder inside which sub-folders are looked for.
     *               Empty string means we look at the root of the bucket
     * @param continuationToken If maxKeys < number of remaining keys, this token is used to look for the next keys.
     * @param maxKeys Maximum number of returned folders
     * @return
     */
    private ListFoldersResult listFolders(String prefix, String continuationToken, Integer maxKeys)
    {
        ListObjectsV2Request listObjects = new ListObjectsV2Request()
            .withPrefix(prefix)
            .withDelimiter(S3_FOLDER_DELIMITER)
            .withBucketName(dataBucket);
        if (maxKeys != null) {
            listObjects.withMaxKeys(maxKeys);
        }
        if (continuationToken != null) {
            listObjects.withContinuationToken(continuationToken);
        }
        ListObjectsV2Result listObjectsResult = amazonS3.listObjectsV2(listObjects);
        Stream<String> listedFolers = listObjectsResult
            .getObjectSummaries()
            .stream()
            .map(S3ObjectSummary::getKey)
            .filter(s3Object -> s3Object.endsWith(S3_FOLDER_SUFFIX))
            .map(s3Object -> StringUtils.removeEnd(s3Object, S3_FOLDER_SUFFIX))
            .map(s3Object -> StringUtils.removeStart(s3Object, prefix));
        return new ListFoldersResult(listedFolers, listObjectsResult.getNextContinuationToken());
    }

    private String tableKeyPrefix(String schemaName, String tableName)
    {
        return schemaName + S3_FOLDER_DELIMITER + tableName;
    }

    /**
     * Retrieves the current Delta table snapshot of a given table
     * @param schemaName The schema of the table
     * @param tableName The name of the table
     * @return The current snapshot of the Delta Table reconstructed from its Transaction Log
     * @throws IOException
     */
    private DeltaTableSnapshot getDeltaSnapshot(String schemaName, String tableName) throws IOException
    {
        DeltaTableStorage.TableLocation tableLocation = new DeltaTableStorage.TableLocation(dataBucket, tableKeyPrefix(schemaName, tableName));
        DeltaTableStorage deltaTableStorage = new DeltaTableStorage(amazonS3, new Configuration(), tableLocation);
        return new DeltaTableSnapshotBuilder(deltaTableStorage).getSnapshot();
    }

    /**
     * List databases inside a bucket by looking for files suffixed with _$folder$.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: " + request);
        Set<String> schemas = listFolders().listedFolders.collect(Collectors.toSet());
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * List tables inside a database by looking for files suffixed with _$folder$.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables:" + request);
        String nextToken = request.getNextToken();
        int pageSize = request.getPageSize();

        String schemaName = request.getSchemaName();
        String prefix = schemaName + S3_FOLDER_DELIMITER;
        Stream<String> listedFolders;
        if (pageSize != ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE) {
            ListFoldersResult listFoldersResult = listFolders(prefix, nextToken, pageSize);
            listedFolders = listFoldersResult.listedFolders;
            nextToken = listFoldersResult.nextContinuationToken;
        }
        else {
            ListFoldersResult listFoldersResult = listFolders(prefix);
            listedFolders = listFoldersResult.listedFolders;
        }
        Set<TableName> tables = listedFolders
            .map(table -> new TableName(schemaName, table))
            .collect(Collectors.toSet());
        return new ListTablesResponse(request.getCatalogName(), tables, nextToken);
    }

    /**
     * Retrieves the schema and the partition columns of the table by reading its Delta transaction log.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws IOException
    {
        logger.info("doGetTable: " + request);
        String catalogName = request.getCatalogName();
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        DeltaTableSnapshot deltaTableSnapshot = getDeltaSnapshot(schemaName, tableName);
        Schema schema = getArrowSchema(deltaTableSnapshot.metaData.schemaString);
        Set<String> partitions = new HashSet<>(deltaTableSnapshot.metaData.partitionColumns);

        return new GetTableResponse(catalogName, request.getTableName(), schema, partitions);
    }

    /**
     * Each file of the Delta table is associated to only 1 partition.
     * We get the partitions by looping through all the files of the table.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        logger.info("getPartitions: " + request);
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        DeltaTableSnapshot deltaTableSnapshot = getDeltaSnapshot(schemaName, tableName);

        for (DeltaLogAction.AddFile file : deltaTableSnapshot.files) {
            Set<Map.Entry<String, String>> keyValues = file.partitionValues.entrySet();
            blockWriter.writeRows((Block block, int row) -> {
                boolean matched = true;
                for (Map.Entry<String, String> partitionValue : keyValues) {
                    String partitionName = partitionValue.getKey();
                    ArrowType partitionType = request.getSchema().findField(partitionName).getType();
                    Object castPartitionValue = castPartitionValue(partitionValue.getValue(), partitionType);
                    matched &= block.setValue(partitionName, row, castPartitionValue);
                }
                block.setValue(ENHANCED_PARTITION_VALUES_COLUMN, row, serializePartitionValues(file.partitionValues));
                block.setValue(ENHANCED_FILE_PATH_COLUMN, row, file.path);
                return matched ? 1 : 0;
            });
        }
    }

    /**
     * For each partition (non removed Delta parquet file) we had 2 informations that will be used by doGetSplit:
     * - one for the file path
     * - one for the already serialized partition values since they are provided by Delta
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        partitionSchemaBuilder.addStringField(ENHANCED_FILE_PATH_COLUMN);
        partitionSchemaBuilder.addStringField(ENHANCED_PARTITION_VALUES_COLUMN);
    }

    /**
     * We parallelize the computation by making 1 split per file which form the Delta table.
     *
     * Each split contains 2 properties that are used by the readWithConstraint method:
     * - one for the file path
     * - one for the partition values associated to the file
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        logger.info("doGetSplits: " + request);
        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();

        Block partitions = request.getPartitions();
        int partitionContd = decodeContinuationToken(request);

        FieldReader partitionValuesReader = partitions.getFieldReader(ENHANCED_PARTITION_VALUES_COLUMN);
        FieldReader filePathReader = partitions.getFieldReader(ENHANCED_FILE_PATH_COLUMN);
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            partitionValuesReader.setPosition(curPartition);
            filePathReader.setPosition(curPartition);
            String partitionValues = partitionValuesReader.readText().toString();
            String filePath = filePathReader.readText().toString();
            Split.Builder splitBuilder = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey());
            Split split = splitBuilder
                .add(SPLIT_FILE_PROPERTY, filePath)
                .add(SPLIT_PARTITION_VALUES_PROPERTY, partitionValues)
                .build();
            splits.add(split);

            if (splits.size() == MAX_SPLITS_PER_REQUEST && curPartition < partitions.getRowCount() - 1) {
                return new GetSplitsResponse(request.getCatalogName(),
                    splits,
                    encodeContinuationToken(curPartition));
            }
        }

        return new GetSplitsResponse(catalogName, splits);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.parseInt(request.getContinuationToken()) + 1;
        }
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}
