/*-
 * #%L
 * Athena Kafka Connector
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
package com.amazonaws.athena.connectors.kafka;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.kafka.dto.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.kafka.KafkaConstants.AVRO_DATA_FORMAT;
import static com.amazonaws.athena.connectors.kafka.KafkaConstants.PROTOBUF_DATA_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaRecordHandlerTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private MockedStatic<GlueClient> awsGlueClientBuilder;

    @Mock
    GlueClient awsGlue;

    @Mock
    S3Client amazonS3;

    @Mock
    SecretsManagerClient awsSecretsManager;

    @Mock
    private AthenaClient athena;

    @Mock
    FederatedIdentity federatedIdentity;

    @Mock
    SpillConfig spillConfig;

    @Mock
    BlockAllocatorImpl allocator;

    MockConsumer<String, TopicResultSet> consumer;
    MockConsumer<String, GenericRecord> avroConsumer;
    MockConsumer<String, DynamicMessage> protobufConsumer;
    KafkaRecordHandler kafkaRecordHandler;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private EncryptionKey encryptionKey = keyFactory.create();
    private S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(UUID.randomUUID().toString())
            .withIsDirectory(true)
            .build();
    private MockedStatic<KafkaUtils> mockedKafkaUtils;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerRecord<String, TopicResultSet> record1 = createConsumerRecord("myTopic", 0, 0, "k1", createTopicResultSet("myTopic"));
        ConsumerRecord<String, TopicResultSet> record2 = createConsumerRecord("myTopic", 0, 1, "k2", createTopicResultSet("myTopic"));
        ConsumerRecord<String, TopicResultSet> nullValueRecord = createConsumerRecord("myTopic", 0, 2, "k3", null);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record1);
            consumer.addRecord(record2);
            consumer.addRecord(nullValueRecord);
        });
        avroConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerRecord<String, GenericRecord> avroRecord = createAvroConsumerRecord("greetings", 0 , 0, "k1", createGenericRecord("greetings"));
        ConsumerRecord<String, GenericRecord> avroNullValueRecord = createAvroConsumerRecord("greetings", 0 , 1, "k2", null);
        avroConsumer.schedulePollTask(() -> {
            avroConsumer.addRecord(avroRecord);
            avroConsumer.addRecord(avroNullValueRecord);
        });
        protobufConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerRecord<String, DynamicMessage> protobufRecord = createProtobufConsumerRecord("protobuftest", 0, 0, "k1", createDynamicRecord());
        ConsumerRecord<String, DynamicMessage> protobufNullValueRecord = createProtobufConsumerRecord("protobuftest", 0, 1, "k2", null);
        protobufConsumer.schedulePollTask(() -> {
            protobufConsumer.addRecord(protobufRecord);
            protobufConsumer.addRecord(protobufNullValueRecord);
        });
        spillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(encryptionKey)
                //This will be enough for a single block
                .withMaxBlockBytes(100000)
                //This will force the writer to spill.
                .withMaxInlineBlockBytes(100)
                //Async Writing.
                .withNumSpillThreads(0)
                .withRequestId(UUID.randomUUID().toString())
                .withSpillLocation(s3SpillLocation)
                .build();
        allocator = new BlockAllocatorImpl();
        mockedKafkaUtils = Mockito.mockStatic(KafkaUtils.class, Mockito.CALLS_REAL_METHODS);
        kafkaRecordHandler = new KafkaRecordHandler(amazonS3, awsSecretsManager, athena, com.google.common.collect.ImmutableMap.of());
        awsGlueClientBuilder = Mockito.mockStatic(GlueClient.class);
        awsGlueClientBuilder.when(()-> GlueClient.create()).thenReturn(awsGlue);
    }

    @After
    public void close(){
        mockedKafkaUtils.close();
        awsGlueClientBuilder.close();
    }

    @Test
    public void testForConsumeDataFromTopic() throws Exception {
        HashMap<TopicPartition, Long> offsets;
        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 0L);
        consumer.updateBeginningOffsets(offsets);

        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 1L);
        consumer.updateEndOffsets(offsets);

        SplitParameters splitParameters = new SplitParameters("myTopic", 0, 0, 2);
        Schema schema = createSchema(createCsvTopicSchema());

        mockedKafkaUtils.when(() -> KafkaUtils.getKafkaConsumer(schema, com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse());
        Mockito.when(awsGlue.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getJsonSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = new S3BlockSpiller(amazonS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
    }

    @Test
    public void testForConsumeAvroDataFromTopic() throws Exception {
        HashMap<TopicPartition, Long> offsets;
        offsets = new HashMap<>();
        offsets.put(new TopicPartition("greetings", 0), 0L);
        avroConsumer.updateBeginningOffsets(offsets);

        offsets = new HashMap<>();
        offsets.put(new TopicPartition("greetings", 0), 1L);
        avroConsumer.updateEndOffsets(offsets);

        SplitParameters splitParameters = new SplitParameters("greetings", 0, 0, 1);
        Schema schema = createAvroSchema(createAvroTopicSchema());

        mockedKafkaUtils.when(() -> KafkaUtils.getAvroKafkaConsumer(com.google.common.collect.ImmutableMap.of())).thenReturn(avroConsumer);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse());
        Mockito.when(awsGlue.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getAvroSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = new S3BlockSpiller(amazonS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(1, spiller.getBlock().getRowCount());
    }

    @Test
    public void testForConsumeProtobufDataFromTopic() throws Exception {
        HashMap<TopicPartition, Long> offsets;
        offsets = new HashMap<>();
        offsets.put(new TopicPartition("protobuftest", 0), 0L);
        protobufConsumer.updateBeginningOffsets(offsets);

        offsets = new HashMap<>();
        offsets.put(new TopicPartition("protobuftest", 0), 1L);
        protobufConsumer.updateEndOffsets(offsets);

        SplitParameters splitParameters = new SplitParameters("protobuftest", 0, 0, 1);
        Schema schema = createProtobufSchema(createProtobufTopicSchema());

        mockedKafkaUtils.when(() -> KafkaUtils.getProtobufKafkaConsumer(com.google.common.collect.ImmutableMap.of())).thenReturn(protobufConsumer);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse());
        Mockito.when(awsGlue.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getProtobufSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = new S3BlockSpiller(amazonS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(1, spiller.getBlock().getRowCount());
    }

    @Test
    public void testForQueryStatusChecker() throws Exception {
        HashMap<TopicPartition, Long> offsets;
        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 0L);
        consumer.updateBeginningOffsets(offsets);

        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 1L);
        consumer.updateEndOffsets(offsets);

        SplitParameters splitParameters = new SplitParameters("myTopic", 0, 0, 1);
        Schema schema = createSchema(createCsvTopicSchema());

        mockedKafkaUtils.when(() -> KafkaUtils.getKafkaConsumer(schema, com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse());
        Mockito.when(awsGlue.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getJsonSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);

        ReadRecordsRequest request = createReadRecordsRequest(schema);

        kafkaRecordHandler.readWithConstraint(null, request, queryStatusChecker);
    }

    @Test
    public void testForEndOffsetIsZero() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        HashMap<TopicPartition, Long> offsets;
        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 0L);
        consumer.updateBeginningOffsets(offsets);

        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 0L);
        consumer.updateEndOffsets(offsets);

        SplitParameters splitParameters = new SplitParameters("myTopic", 0, 0, 1);
        Schema schema = createSchema(createCsvTopicSchema());

        mockedKafkaUtils.when(() -> KafkaUtils.getKafkaConsumer(schema, com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse());
        Mockito.when(awsGlue.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getJsonSchemaVersionResponse());

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        kafkaRecordHandler.readWithConstraint(null, request, null);
    }

    @Test
    public void testForContinuousEmptyDataFromTopic() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        HashMap<TopicPartition, Long> offsets;
        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 0L);
        consumer.updateBeginningOffsets(offsets);

        offsets = new HashMap<>();
        offsets.put(new TopicPartition("myTopic", 0), 10L);
        consumer.updateEndOffsets(offsets);

        SplitParameters splitParameters = new SplitParameters("myTopic", 0, 0, 1);
        Schema schema = createSchema(createCsvTopicSchema());

        mockedKafkaUtils.when(() -> KafkaUtils.getKafkaConsumer(schema, com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse());
        Mockito.when(awsGlue.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(getJsonSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        kafkaRecordHandler.readWithConstraint(null, request, queryStatusChecker);
    }

    private ReadRecordsRequest createReadRecordsRequest(Schema schema) {
        return new ReadRecordsRequest(
                federatedIdentity,
                "testCatalog",
                "queryId",
                new TableName("testSchema", "testTable"),
                schema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket("bucket")
                                .withPrefix("prefix")
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                0,
                0);
    }

    private ConsumerRecord<String, TopicResultSet> createConsumerRecord(String topic, int partition, long offset, String key, TopicResultSet data) throws Exception {
        return new ConsumerRecord<>(topic, partition, offset, key, data);
    }

    private ConsumerRecord<String, GenericRecord> createAvroConsumerRecord(String topic, int partition, long offset, String key, GenericRecord data) throws Exception {
        return new ConsumerRecord<>(topic, partition, offset, key, data);
    }

    private ConsumerRecord<String, DynamicMessage> createProtobufConsumerRecord(String topic, int partition, long offset, String key, DynamicMessage data) throws Exception {
        return new ConsumerRecord<>(topic, partition, offset, key, data);
    }

    private TopicResultSet createTopicResultSet(String topic) {
        TopicResultSet resultSet = new TopicResultSet();
        resultSet.setTopicName(topic);
        resultSet.setDataFormat(Message.DATA_FORMAT_CSV);
        resultSet.getFields().add(new KafkaField("id", "0", "INTEGER", "", Integer.parseInt("1")));
        resultSet.getFields().add(new KafkaField("name", "1", "VARCHAR", "", new String("Smith")));
        resultSet.getFields().add(new KafkaField("isActive", "2", "BOOLEAN", "", Boolean.valueOf("true")));
        resultSet.getFields().add(new KafkaField("code", "3", "TINYINT", "", Byte.parseByte("101")));
        return resultSet;
    }

    private GenericRecord createGenericRecord(String topic) {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        String schemaString = "{\"type\": \"record\",\"name\":\"" + topic + "\",\"fields\": [{\"name\": \"id\", \"type\": \"int\"},{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"greeting\",\"type\": \"string\"}]}";
        org.apache.avro.Schema schema = parser.parse(schemaString);
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 1);
        record.put("name", "John");
        record.put("greeting", "Hello");

        return record;
    }

    private DynamicMessage createDynamicRecord() {
        String schema = "syntax = \"proto3\";\n" +
                "message protobuftest {\n" +
                "string name = 1;\n" +
                "int32 calories = 2;\n" +
                "string colour = 3; \n" +
                "}";
        ProtobufSchema protobufSchema = new ProtobufSchema(schema);
        Descriptors.Descriptor descriptor = protobufSchema.toDescriptor();

        // Build the dynamic message
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        builder.setField(descriptor.findFieldByName("name"), "cake");
        builder.setField(descriptor.findFieldByName("calories"), 260);
        builder.setField(descriptor.findFieldByName("colour"), "white");

        return builder.build();
    }

    private Schema createSchema(TopicSchema topicSchema) throws Exception {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        topicSchema.getMessage().getFields().forEach(it -> {
            FieldType fieldType = new FieldType(
                    true,
                    KafkaUtils.toArrowType(it.getType()),
                    null,
                    com.google.common.collect.ImmutableMap.of(
                            "mapping", it.getMapping(),
                            "formatHint", it.getFormatHint(),
                            "type", it.getType()
                    )
            );
            Field field = new Field(it.getName(), fieldType, null);
            schemaBuilder.addField(field);
        });

        schemaBuilder.addMetadata("dataFormat", topicSchema.getMessage().getDataFormat());
        return schemaBuilder.build();
    }

    private Schema createAvroSchema(AvroTopicSchema avroTopicSchema) throws Exception {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        avroTopicSchema.getFields().forEach(it -> {
            FieldType fieldType = new FieldType(
                    true,
                    KafkaUtils.toArrowType(it.getType()),
                    null,
                    com.google.common.collect.ImmutableMap.of(
                            "name", it.getName(),
                            "formatHint", it.getFormatHint(),
                            "type", it.getType()
                    )
            );
            Field field = new Field(it.getName(), fieldType, null);
            schemaBuilder.addField(field);
        });
        schemaBuilder.addMetadata("dataFormat", AVRO_DATA_FORMAT);
        return schemaBuilder.build();
    }

    private Schema createProtobufSchema(ProtobufSchema protobufTopicSchema) throws Exception {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        Descriptors.Descriptor descriptor = protobufTopicSchema.toDescriptor();
        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            FieldType fieldType = new FieldType(
                    true,
                    KafkaUtils.toArrowType(fieldDescriptor.getType().toString()),
                    null
            );
            Field field = new Field(fieldDescriptor.getName(), fieldType, null);
            schemaBuilder.addField(field);
        }
        schemaBuilder.addMetadata("dataFormat", PROTOBUF_DATA_FORMAT);
        return schemaBuilder.build();
    }

    private TopicSchema createCsvTopicSchema() throws JsonProcessingException {
        String csv = "{" +
                "\"topicName\":\"test\"," +
                "\"message\":{" +
                "\"dataFormat\":\"csv\"," +
                "\"fields\":[" +
                "{\"name\":\"id\",\"type\":\"INTEGER\",\"mapping\":\"0\",\"formatHint\": \"\"}," +
                "{\"name\":\"name\",\"type\":\"VARCHAR\",\"mapping\":\"1\",\"formatHint\": \"\"}," +
                "{\"name\":\"isActive\",\"type\":\"BOOLEAN\",\"mapping\":\"2\",\"formatHint\": \"\"}," +
                "{\"name\":\"code\",\"type\":\"TINYINT\",\"mapping\":\"3\",\"formatHint\": \"\"}" +
                "]}}";
        return objectMapper.readValue(csv, TopicSchema.class);
    }

    private AvroTopicSchema createAvroTopicSchema() throws JsonProcessingException {
        String avro = "{\"type\": \"record\",\"name\":\"greetings\",\"fields\": [{\"name\": \"id\", \"type\": \"int\"},{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"greeting\",\"type\": \"string\"}]}";
        return objectMapper.readValue(avro, AvroTopicSchema.class);
    }

    private ProtobufSchema createProtobufTopicSchema() {
        String protobuf = "syntax = \"proto3\";\n" +
                "message protobuftest {\n" +
                "string name = 1;\n" +
                "int32 calories = 2;\n" +
                "string colour = 3; \n" +
                "}";
        ProtobufSchema protobufSchema = new ProtobufSchema(protobuf);
        return protobufSchema;
    }

    private GetSchemaResponse getSchemaResponse() {
        String arn = "defaultArn", schemaName = "defaultSchemaName";
        Long latestSchemaVersion = 123L;
        GetSchemaResponse getSchemaResponse = GetSchemaResponse.builder()
                .schemaArn(arn)
                .schemaName(schemaName)
                .latestSchemaVersion(latestSchemaVersion)
                .build();
        return getSchemaResponse;
    }

    private GetSchemaVersionResponse getJsonSchemaVersionResponse() {
        String arn = "defaultArn", schemaVersionId = "defaultVersionId";
        GetSchemaVersionResponse getJsonSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(arn)
                .schemaVersionId(schemaVersionId)
                .dataFormat("json")
                .schemaDefinition("{\"topicName\": \"testtable\", \"\"message\": {\"dataFormat\": \"json\", \"fields\": [{\"name\": \"intcol\", \"mapping\": \"intcol\", \"type\": \"INTEGER\"}]}\"}")
                .build();
        return getJsonSchemaVersionResponse;
    }

    private GetSchemaVersionResponse getAvroSchemaVersionResponse() {
        String arn = "defaultArn", schemaVersionId = "defaultVersionId";
        GetSchemaVersionResponse getAvroSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(arn)
                .schemaVersionId(schemaVersionId)
                .dataFormat("avro")
                .schemaDefinition("{\"type\": \"record\",\"name\":\"greetings\",\"fields\": [{\"name\": \"id\", \"type\": \"int\"},{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"greeting\",\"type\": \"string\"}]}")
                .build();
        return getAvroSchemaVersionResponse;
    }

    private GetSchemaVersionResponse getProtobufSchemaVersionResponse() {
        String arn = "defaultArn", schemaVersionId = "defaultVersionId";
        GetSchemaVersionResponse getProtobufSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaArn(arn)
                .schemaVersionId(schemaVersionId)
                .dataFormat("protobuf")
                .schemaDefinition("syntax = \"proto3\";\n" +
                        "message protobuftest {\n" +
                        "string name = 1;\n" +
                        "int32 calories = 2;\n" +
                        "string colour = 3; \n" +
                        "}")
                .build();
        return getProtobufSchemaVersionResponse;
    }
}
