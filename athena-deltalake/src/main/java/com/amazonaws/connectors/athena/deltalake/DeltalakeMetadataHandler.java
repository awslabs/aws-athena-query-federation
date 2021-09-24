/*-
 * #%L
 * athena-example
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.amazonaws.connectors.athena.deltalake.converter.DeltaConverter.*;

public class DeltalakeMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeMetadataHandler.class);

    public static String SPLIT_FILE_PROPERTY = "file";
    public static String SPLIT_PARTITION_VALUES_PROPERTY = "partitions_values";

    private static final String SOURCE_TYPE = "deltalake";
    public static String DATA_BUCKET = System.getenv("data_bucket");
    public String S3_FOLDER_SUFFIX = "_$folder$";
    private final AmazonS3 amazonS3;

    public DeltalakeMetadataHandler()
    {
        super(SOURCE_TYPE);
        this.amazonS3 = AmazonS3ClientBuilder.defaultClient();
    }

    @VisibleForTesting
    protected DeltalakeMetadataHandler(AmazonS3 amazonS3,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager awsSecretsManager,
            AmazonAthena athena,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.amazonS3 = amazonS3;
    }

    protected String serializePartitionValues(Map<String, String> partitionValues) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(partitionValues);
    }

    private Set<String> listFolders() {
        return listFolders("");
    }

    private Set<String> listFolders(String prefix) {
        ListObjectsV2Request listObjects = new ListObjectsV2Request()
                .withPrefix(prefix)
                .withDelimiter("/")
                .withBucketName(DATA_BUCKET);
        return amazonS3.listObjectsV2(listObjects)
                .getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .filter(s3Object -> s3Object.endsWith(S3_FOLDER_SUFFIX))
                .map(s3Object -> StringUtils.removeEnd(s3Object, S3_FOLDER_SUFFIX))
                .map(s3Object -> StringUtils.removeStart(s3Object, prefix))
                .collect(Collectors.toSet());
    }

    private String tableKeyPrefix(String schemaName, String tableName) {
        return schemaName + "/" + tableName;
    }

    private DeltaTableSnapshot getDeltaSnapshot(String schemaName, String tableName) throws IOException {
        DeltaTableStorage.TableLocation tableLocation = new DeltaTableStorage.TableLocation(DATA_BUCKET, tableKeyPrefix(schemaName, tableName));
        DeltaTableStorage deltaTableStorage = new DeltaTableStorage(amazonS3, new Configuration(), tableLocation);
        return new DeltaTableSnapshotBuilder(deltaTableStorage).getSnapshot();
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        System.out.println("doListSchemaNames: " + request);
        Set<String> schemas = listFolders();
        ListSchemasResponse res = new ListSchemasResponse(request.getCatalogName(), schemas);
        System.out.println(res.toString());
        return res;
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        System.out.println("doListTables:" + request);

        String schemaName = request.getSchemaName();
        String prefix = schemaName + "/";
        Set<TableName> tables = listFolders(prefix).stream()
                .map(table -> new TableName(schemaName, table))
                .collect(Collectors.toSet());
        ListTablesResponse res = new ListTablesResponse(schemaName, tables, null);
        System.out.println(res.toString());
        return res;
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws IOException {
        System.out.println("doGetTable: " + request);
        String catalogName = request.getCatalogName();
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        DeltaTableSnapshot deltaTableSnapshot = getDeltaSnapshot(schemaName, tableName);
        System.out.println("delta table schema: " + deltaTableSnapshot.metaData.schemaString);
        Schema schema = getArrowSchema(deltaTableSnapshot.metaData.schemaString);
        Set<String> partitions = new HashSet<>(deltaTableSnapshot.metaData.partitionColumns);

        GetTableResponse res = new GetTableResponse(catalogName, request.getTableName(), schema, partitions);
        System.out.println(res.toString());
        return res;
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        System.out.println("getPartitions: " + request);
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        DeltaTableSnapshot deltaTableSnapshot = getDeltaSnapshot(schemaName, tableName);

        List<String> partitions = deltaTableSnapshot.metaData.partitionColumns;
        System.out.println("partition columns: " + partitions.toString());

        for(DeltaLogAction.AddFile file: deltaTableSnapshot.files) {
            Set<Map.Entry<String, String>> keyValues = file.partitionValues.entrySet();
            blockWriter.writeRows((Block block, int row) -> {
                boolean matched = true;
                for (Map.Entry<String, String> partitionValue : keyValues) {
                    String partitionName = partitionValue.getKey();
                    ArrowType partitionType = request.getSchema().findField(partitionName).getType();
                    Object castPartitionValue = castPartitionValue(partitionValue.getValue(), partitionType);
                    matched &= block.setValue(partitionName, row, castPartitionValue);
                }
                return matched ? 1 : 0;
            });
        }
        System.out.println("block: " + blockWriter.toString());
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws IOException {
        System.out.println("doGetSplits: " + request);
        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();

        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        DeltaTableSnapshot deltaTableSnapshot = getDeltaSnapshot(schemaName, tableName);

        Collection<DeltaLogAction.AddFile> allFiles = deltaTableSnapshot.files;

        for (DeltaLogAction.AddFile file: allFiles) {
            Split.Builder splitBuilder = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey());
            Split split = splitBuilder
                    .add(SPLIT_FILE_PROPERTY, file.path)
                    .add(SPLIT_PARTITION_VALUES_PROPERTY, serializePartitionValues(file.partitionValues))
                    .build();
            splits.add(split);
        }
        GetSplitsResponse res = new GetSplitsResponse(catalogName, splits);
        System.out.println(res.toString());
        return res;
    }
}
