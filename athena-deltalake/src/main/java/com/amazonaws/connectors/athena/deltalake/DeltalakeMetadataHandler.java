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
import com.amazonaws.connectors.athena.deltalake.protocol.DeltaTable;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.tools.javac.util.Pair;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
//DO NOT REMOVE - this will not be _unused_ when customers go through the tutorial and uncomment
//the TODOs
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DeltalakeMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeMetadataHandler.class);

    public static String FILE_SPLIT_PROPERTY_KEY = "file";
    public static String PARTITION_NAMES_SPLIT_PROPERTY_KEY = "partitions_names";
    public static String PARTITION_VALUES_SPLIT_PROPERTY_KEY_PREFIX = "PARTITION_";

    public static String getPartitionValuePropertyKey(String partitionName) {
        return String.format("%s%s", PARTITION_NAMES_SPLIT_PROPERTY_KEY, partitionName);
    }

    public static String serializePartitionNames(List<String> partitionNames) {
        return String.join(",", partitionNames);
    }

    public static List<String> deserializePartitionNames(String partitionNames) {
        return Arrays.asList(partitionNames.split(",").clone());
    }


    private static final String SOURCE_TYPE = "example";
    public static String DATA_BUCKET = System.getenv("data_bucket");
    public S3Client s3 = S3Client.create();
    AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
    public String S3_FOLDER_SUFFIX = "_$folder$";
    static public Configuration HADOOP_CONF = defaultConfiguration();

    static private Configuration defaultConfiguration() {
        Configuration conf = new Configuration();
        conf.setLong("fs.s3a.multipart.size", 104857600);
        conf.setInt("fs.s3a.multipart.threshold", Integer.MAX_VALUE);
        conf.setBoolean("fs.s3a.impl.disable.cache", true);
        return conf;
    }

    public DeltalakeMetadataHandler()
    {
        super(SOURCE_TYPE);
    }

    @VisibleForTesting
    protected DeltalakeMetadataHandler(EncryptionKeyFactory keyFactory,
                                       AWSSecretsManager awsSecretsManager,
                                       AmazonAthena athena,
                                       String spillBucket,
                                       String spillPrefix)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
    }

    protected String extractPartitionValue(String partitionName, String path) {
        Pattern p = Pattern.compile(String.format("\\/%s=([^\\/]*)\\/", partitionName));
        Matcher m = p.matcher(path);
        m.find();
        return m.group(1);
    }


    protected List<Pair<String, String>> extractPartitionValues(List<String> partitions, String path) {
        return partitions.stream()
                .map(partitionName -> Pair.of(partitionName, extractPartitionValue(partitionName, path)))
                .collect(Collectors.toList());
    }

    protected Set<String> listFolders() {
        return listFolders("");
    }

    protected Set<String> listFolders(String prefix) {
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
                .collect(Collectors.toSet());
    }



    protected Field getAvroField(JsonNode fieldType, String fieldName, boolean fieldNullable) {
        if (fieldType.isTextual()) {
            String fieldTypeName = fieldType.asText();
            if(fieldTypeName.equals("integer")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.INT.getType(), null),
            null);
            }
            if(fieldTypeName.equals("string")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.VARCHAR.getType(), null),
            null);
            }
            if(fieldTypeName.equals("long")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.BIGINT.getType(), null),
            null);
            }
            if(fieldTypeName.equals("short")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.SMALLINT.getType(), null),
            null);
            }
            if(fieldTypeName.equals("byte")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.TINYINT.getType(), null),
            null);
            }
            if(fieldTypeName.equals("float")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.FLOAT4.getType(), null),
            null);
            }
            if(fieldTypeName.equals("double")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.FLOAT8.getType(), null),
            null);
            }
            if(fieldTypeName.equals("boolean")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.BIT.getType(), null),
            null);
            }
            if(fieldTypeName.equals("binary")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.VARBINARY.getType(), null),
            null);
            }
            if(fieldTypeName.equals("date")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.DATEDAY.getType(), null),
            null);
            }
            if(fieldTypeName.equals("timestamp")) {
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.DATEMILLI.getType(), null),
            null);
            }
        } else {
            String complexTypeName = fieldType.get("type").asText();
            if (complexTypeName.equals("struct")) {
                Iterator<JsonNode> structFields = fieldType.withArray("fields").elements();
                List<Field> children = new ArrayList<>();
                while (structFields.hasNext()) {
                    JsonNode structField = structFields.next();
                    children.add(getAvroField(structField));
                }
                return new Field(
                    fieldName,
                    new FieldType(fieldNullable, Types.MinorType.STRUCT.getType(), null),
                    children);
            } else if (complexTypeName.equals("array")){
                JsonNode elementType = fieldType.get("elementType");
                boolean elementNullable = fieldType.get("containsNull").asBoolean();
                String elementName = fieldName + ".element";
                Field elementField = getAvroField(elementType, elementName, elementNullable);
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.LIST.getType(), null),
                        Collections.singletonList(elementField));
            } else if (complexTypeName.equals("map")){
                JsonNode keyType = fieldType.get("keyType");
                JsonNode valueType = fieldType.get("valueType");
                boolean valueNullable = fieldType.get("valueContainsNull").asBoolean();
                boolean keyNullable = false;
                String keyName = fieldName + ".key";
                String valueName = fieldName + ".value";
                Field keyField = getAvroField(keyType, keyName, keyNullable);
                Field valueField = getAvroField(valueType, valueName, valueNullable);
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.MAP.getType(), null),
                        Arrays.asList(keyField, valueField));
            }
        }
        throw new UnsupportedOperationException("Unsupported field type: " + fieldType.toString());
    }

    public Field getAvroField(JsonNode field) {
        String fieldName = field.get("name").asText();
        boolean fieldNullable = field.get("nullable").asBoolean();
        JsonNode fieldType = field.get("type");
        return getAvroField(fieldType, fieldName, fieldNullable);
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: ", request);
        return new ListSchemasResponse(request.getCatalogName(), listFolders());
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables:", request);

        String schemaName = request.getSchemaName();
        String prefix = schemaName + "/";
        Set<TableName> tables = listFolders(prefix).stream()
                .map(table -> new TableName(schemaName, table))
                .collect(Collectors.toSet());
        return new ListTablesResponse(schemaName, tables, null);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws IOException {
        String catalogName = request.getCatalogName();
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        String tablePath = String.format("s3a://%s/%s/%s/", DATA_BUCKET, schemaName, tableName);

        DeltaTable.DeltaTableSnapshot log = new DeltaTable(schemaName + "/" + tableName , DATA_BUCKET).getSnapshot();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaJson = mapper.readTree(log.metaData.schemaString);
        Iterator<JsonNode> fields = schemaJson.withArray("fields").elements();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        while (fields.hasNext()) {
            JsonNode field = fields.next();
            String fieldName = field.get("name").asText();
            Field avroField = getAvroField(field);
            schemaBuilder.addField(avroField);
        }
        Schema schema = schemaBuilder.build();

        Set<String> partitions = new HashSet<>(log.metaData.partitionColumns);

        return new GetTableResponse(catalogName, request.getTableName(), schema, partitions);
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        String tablePath = String.format("s3a://%s/%s/%s/", DATA_BUCKET, schemaName, tableName);
        DeltaTable.DeltaTableSnapshot log = new DeltaTable(schemaName + "/" + tableName , DATA_BUCKET).getSnapshot();


        List<String> partitions = log.metaData.partitionColumns;

        log.files.stream()
            .map(file -> file.partitionValues.entrySet())
            .forEachOrdered(keyValues -> {
                blockWriter.writeRows((Block block, int row) -> {
                    boolean matched = true;
                    for(Map.Entry<String, String> partitionValue: keyValues) {
                        matched &= block.setValue(partitionValue.getKey(), row, partitionValue.getValue());
                    }
                    return matched ? 1 : 0;
                });
            });
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();

        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        String tablePath = String.format("s3a://%s/%s/%s/", DATA_BUCKET, schemaName, tableName);
        Snapshot log = DeltaLog.forTable(HADOOP_CONF, tablePath).snapshot();

        List<AddFile> allFiles = log.getAllFiles();
        Block partitions = request.getPartitions();

        List<FieldReader> fieldReaders = partitions.getFieldReaders();
        List<String> partitionNames = fieldReaders.stream().map(fr -> fr.getField().getName()).collect(Collectors.toList());

        int nbPartitions = partitions.getRowCount();
        IntStream.range(0, nbPartitions).forEachOrdered(i -> {
            fieldReaders.forEach(fieldReader -> fieldReader.setPosition(i));

            allFiles.stream().filter(file ->
                fieldReaders.stream().allMatch(fieldReader ->
                    fieldReader.readText().toString()
                        .equals(extractPartitionValue(fieldReader.getField().getName(), file.getPath()))
                )
            ).forEach(file -> {
                Split.Builder splitBuilder = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey());
                for(FieldReader fieldReader: fieldReaders) {
                    splitBuilder.add(
                            getPartitionValuePropertyKey(fieldReader.getField().getName()),
                            extractPartitionValue(fieldReader.getField().getName(), file.getPath())
                    );
                }
                Split split = splitBuilder
                        .add(FILE_SPLIT_PROPERTY_KEY, file.getPath())
                        .add(PARTITION_NAMES_SPLIT_PROPERTY_KEY, serializePartitionNames(partitionNames))
                        .build();
                splits.add(split);
            });
        });

        return new GetSplitsResponse(catalogName, splits);
    }
}
