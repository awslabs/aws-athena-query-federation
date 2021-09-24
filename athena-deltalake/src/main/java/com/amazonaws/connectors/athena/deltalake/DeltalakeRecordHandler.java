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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.*;
import static com.amazonaws.connectors.athena.deltalake.converter.DeltaConverter.castPartitionValue;
import static com.amazonaws.connectors.athena.deltalake.converter.ParquetConverter.getExtractor;

public class DeltalakeRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeRecordHandler.class);

    private static final String SOURCE_TYPE = "deltalake";

    private Configuration conf;

    public DeltalakeRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient());
        Configuration conf = new Configuration();
        conf.setLong("fs.s3a.multipart.size", 104857600);
        conf.setInt("fs.s3a.multipart.threshold", Integer.MAX_VALUE);
        conf.setBoolean("fs.s3a.impl.disable.cache", true);
        conf.set("fs.s3a.metadatastore.impl", "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore");
        this.conf = conf;
    }

    @VisibleForTesting
    protected DeltalakeRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
    }

    protected Map<String, String> deserializePartitionValues(String partitionValuesJson) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaJson = mapper.readTree(partitionValuesJson);
        Map<String, String> partitionValues = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = schemaJson.fields();
        for (Iterator<Map.Entry<String, JsonNode>> it = fields; it.hasNext(); ) {
            Map.Entry<String, JsonNode> field = it.next();
            partitionValues.put(field.getKey(), field.getValue().textValue());
        }
        return partitionValues;
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException, ParseException {
        System.out.println("readWithConstraint: " + recordsRequest);

        Split split = recordsRequest.getSplit();

        String relativeFilePath = split.getProperty(SPLIT_FILE_PROPERTY);


        Map<String, String> partitionValues = deserializePartitionValues(split.getProperty(SPLIT_PARTITION_VALUES_PROPERTY));
        System.out.println("partitionValues: " + partitionValues);
        Set<String> partitionNames = partitionValues.keySet();

        String tableName = recordsRequest.getTableName().getTableName();
        String schemaName = recordsRequest.getTableName().getSchemaName();

        String tablePath = String.format("s3a://%s/%s/%s", DATA_BUCKET, schemaName, tableName);
        String filePath = String.format("%s/%s", tablePath, relativeFilePath);

        List<Field> fields = recordsRequest.getSchema().getFields();

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

        for(Field field : fields) {
            String fieldName = field.getName();
            if (partitionNames.contains(fieldName)) {
                Object partitionValue = castPartitionValue(partitionValues.get(fieldName), field.getType());
                builder.withExtractor(fieldName, getExtractor(field, Optional.of(partitionValue)));
            }
            else builder.withExtractor(fieldName, getExtractor(field));
        }

        ParquetReader<Group> reader = ParquetReader
                .builder(new GroupReadSupport(), new Path(filePath))
                .withConf(conf)
                .build();
        GeneratedRowWriter rowWriter = builder.build();

        long countRecord = 0L;
        Group record;
        while((record = reader.read()) != null) {
            Group finalRecord = record;
            spiller.writeRows((block, rowNum) -> rowWriter.writeRow(block, rowNum, finalRecord) ? 1 : 0);
            countRecord += 1;
        }
        System.out.println("Split finished with records: " +  countRecord);
    }
}
