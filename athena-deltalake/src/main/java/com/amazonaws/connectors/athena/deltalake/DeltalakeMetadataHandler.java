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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.sun.tools.javac.util.Pair;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.types.*;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
//DO NOT REMOVE - this will not be _unused_ when customers go through the tutorial and uncomment
//the TODOs
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example Metadatahandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with metadata about the schemas (aka databases),
 * tables, and table partitions that your source contains. Lastly, this class tells Athena how to split up reads against
 * this source. This gives you control over the level of performance and parallelism your source can support.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
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

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "example";
    public static String DATA_BUCKET = System.getenv("data_bucket");
    public S3Client s3 = S3Client.create();
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
        ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .prefix(prefix)
                .delimiter("/")
                .bucket(DATA_BUCKET)
                .build();
        return s3.listObjects(listObjects)
                .contents().stream()
                .map(S3Object::key)
                .filter(s3Object -> s3Object.endsWith(S3_FOLDER_SUFFIX))
                .map(s3Object -> StringUtils.removeEnd(s3Object, S3_FOLDER_SUFFIX))
                .collect(Collectors.toSet());
    }

    protected SchemaBuilder addFieldToSchema(SchemaBuilder schemaBuilder, StructField field) {
        DataType dataType = field.getDataType();
        if (dataType instanceof StringType) {
            return schemaBuilder.addStringField(field.getName());
        }
        else if (dataType instanceof BooleanType) {
            return schemaBuilder.addBitField(field.getName());
        }
        else if (dataType instanceof ByteType) {
            return schemaBuilder.addTinyIntField(field.getName());
        }
        else if (dataType instanceof ShortType) {
            return schemaBuilder.addSmallIntField(field.getName());
        }
        else if (dataType instanceof IntegerType) {
            return schemaBuilder.addIntField(field.getName());
        }
        else if (dataType instanceof LongType) {
            return schemaBuilder.addBigIntField(field.getName());
        }
        else if (dataType instanceof FloatType) {
            return schemaBuilder.addFloat4Field(field.getName());
        }
        else if (dataType instanceof DoubleType) {
            return schemaBuilder.addFloat8Field(field.getName());
        }
        else if (dataType instanceof DateType) {
            return schemaBuilder.addDateDayField(field.getName());
        }
        else if (dataType instanceof TimestampType) {
            return schemaBuilder.addDateMilliField(field.getName());
        }
        else if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType)dataType;
            return schemaBuilder.addDecimalField(field.getName(), decimalType.getPrecision(), decimalType.getScale());
        }
        else {
            return schemaBuilder;
        }
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
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        String catalogName = request.getCatalogName();
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        String tablePath = String.format("s3a://%s/%s/%s/", DATA_BUCKET, schemaName, tableName);

        Snapshot log = DeltaLog.forTable(HADOOP_CONF, tablePath).snapshot();
        StructField[] fields = log.getMetadata().getSchema().getFields();

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for(StructField field : fields) {
            schemaBuilder = addFieldToSchema(schemaBuilder, field);
        }
        Schema schema = schemaBuilder.build();

        Set<String> partitions = new HashSet<>(log.getMetadata().getPartitionColumns());

        return new GetTableResponse(catalogName, request.getTableName(), schema, partitions);
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        String tablePath = String.format("s3a://%s/%s/%s/", DATA_BUCKET, schemaName, tableName);
        Snapshot log = DeltaLog.forTable(HADOOP_CONF, tablePath).snapshot();

        List<String> partitions = log.getMetadata().getPartitionColumns();

        log.getAllFiles().stream()
            .map(file -> extractPartitionValues(partitions, file.getPath()))
            .forEachOrdered(extractedPartitions -> {
                blockWriter.writeRows((Block block, int row) -> {
                    boolean matched = true;
                    for(Pair<String, String> partitionValue: extractedPartitions) {
                        matched &= block.setValue(partitionValue.fst, row, partitionValue.snd);
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
