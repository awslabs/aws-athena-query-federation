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
    private static final String EARLIEST = "earliest";
    private static final String MY_TOPIC = "myTopic";
    private static final String GREETINGS_TOPIC = "greetings";
    private static final String PROTOBUF_TEST_TOPIC = "protobuftest";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String QUERY_ID = "queryId";
    private static final String DEFAULT_ARN = "defaultArn";
    private static final String DEFAULT_SCHEMA_NAME = "defaultSchemaName";
    private static final String DEFAULT_VERSION_ID = "defaultVersionId";
    private static final Long DEFAULT_LATEST_SCHEMA_VERSION = 123L;
    private static final String TEST_BUCKET = "bucket";
    private static final String TEST_PREFIX = "prefix";
    private static final String JSON_FORMAT = "json";
    private static final String AVRO_FORMAT = "avro";
    private static final String PROTOBUF_FORMAT = "protobuf";
    private static final String TEST_KEY_1 = "k1";
    private static final String TEST_KEY_2 = "k2";
    private static final String TEST_KEY_3 = "k3";
    private static final int PARTITION = 0;

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
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        consumer = new MockConsumer<>(EARLIEST);
        ConsumerRecord<String, TopicResultSet> record1 = createConsumerRecord(0, TEST_KEY_1, createTopicResultSet());
        ConsumerRecord<String, TopicResultSet> record2 = createConsumerRecord(1, TEST_KEY_2, createTopicResultSet());
        ConsumerRecord<String, TopicResultSet> nullValueRecord = createConsumerRecord(2, TEST_KEY_3, null);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record1);
            consumer.addRecord(record2);
            consumer.addRecord(nullValueRecord);
        });
        avroConsumer = new MockConsumer<>(EARLIEST);
        ConsumerRecord<String, GenericRecord> avroRecord = createAvroConsumerRecord(0, TEST_KEY_1, createGenericRecord());
        ConsumerRecord<String, GenericRecord> avroNullValueRecord = createAvroConsumerRecord(1, TEST_KEY_2, null);
        avroConsumer.schedulePollTask(() -> {
            avroConsumer.addRecord(avroRecord);
            avroConsumer.addRecord(avroNullValueRecord);
        });
        protobufConsumer = new MockConsumer<>(EARLIEST);
        ConsumerRecord<String, DynamicMessage> protobufRecord = createProtobufConsumerRecord(0, TEST_KEY_1, createDynamicRecord());
        ConsumerRecord<String, DynamicMessage> protobufNullValueRecord = createProtobufConsumerRecord(1, TEST_KEY_2, null);
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
        awsGlueClientBuilder.when(GlueClient::create).thenReturn(awsGlue);
    }

    @After
    public void close(){
        mockedKafkaUtils.close();
        awsGlueClientBuilder.close();
    }

    @Test
    public void testForConsumeDataFromTopic() throws Exception {
        Schema schema = createSchema(createCsvTopicSchema());
        setupKafkaMocks(consumer, MY_TOPIC, 1L, schema);
        setupGlueMocks(getJsonSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(2, spiller.getBlock().getRowCount());
    }

    @Test
    public void testForConsumeAvroDataFromTopic() throws Exception {
        Schema schema = createAvroSchema(createAvroTopicSchema());
        setupKafkaMocks(avroConsumer, GREETINGS_TOPIC, 1L, schema);
        setupGlueMocks(getAvroSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(1, spiller.getBlock().getRowCount());
    }

    @Test
    public void testForConsumeProtobufDataFromTopic() throws Exception {
        Schema schema = createProtobufSchema(createProtobufTopicSchema());
        setupKafkaMocks(protobufConsumer, PROTOBUF_TEST_TOPIC, 1L, schema);
        setupGlueMocks(getProtobufSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(1, spiller.getBlock().getRowCount());
    }

    @Test
    public void testForQueryStatusChecker() throws Exception {
        Schema schema = createSchema(createCsvTopicSchema());
        setupKafkaMocks(consumer, MY_TOPIC, 1L, schema);
        setupGlueMocks(getJsonSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void testForEndOffsetIsZero() throws Exception {
        consumer = new MockConsumer<>(EARLIEST);

        Schema schema = createSchema(createCsvTopicSchema());
        setupKafkaMocks(consumer, MY_TOPIC, 0L, schema);
        setupGlueMocks(getJsonSchemaVersionResponse());

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, null);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void testForContinuousEmptyDataFromTopic() throws Exception {
        consumer = new MockConsumer<>(EARLIEST);

        Schema schema = createSchema(createCsvTopicSchema());
        setupKafkaMocks(consumer, MY_TOPIC, 10L, schema);
        setupGlueMocks(getJsonSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(0, spiller.getBlock().getRowCount());
    }
    
    private ReadRecordsRequest createReadRecordsRequest(Schema schema) {
        return new ReadRecordsRequest(
                federatedIdentity,
                TEST_CATALOG,
                QUERY_ID,
                new TableName(TEST_SCHEMA, TEST_TABLE),
                schema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket(TEST_BUCKET)
                                .withPrefix(TEST_PREFIX)
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                0,
                0);
    }

    private ConsumerRecord<String, TopicResultSet> createConsumerRecord(long offset, String key, TopicResultSet data) {
        return new ConsumerRecord<>(MY_TOPIC, PARTITION, offset, key, data);
    }

    private ConsumerRecord<String, GenericRecord> createAvroConsumerRecord(long offset, String key, GenericRecord data) {
        return new ConsumerRecord<>(GREETINGS_TOPIC, PARTITION, offset, key, data);
    }

    private ConsumerRecord<String, DynamicMessage> createProtobufConsumerRecord(long offset, String key, DynamicMessage data) {
        return new ConsumerRecord<>(PROTOBUF_TEST_TOPIC, PARTITION, offset, key, data);
    }

    private TopicResultSet createTopicResultSet() {
        TopicResultSet resultSet = new TopicResultSet();
        resultSet.setTopicName(MY_TOPIC);
        resultSet.setDataFormat(Message.DATA_FORMAT_CSV);
        resultSet.getFields().add(new KafkaField("id", "0", "INTEGER", "", Integer.parseInt("1")));
        resultSet.getFields().add(new KafkaField("name", "1", "VARCHAR", "", "Smith"));
        resultSet.getFields().add(new KafkaField("isActive", "2", "BOOLEAN", "", Boolean.valueOf("true")));
        resultSet.getFields().add(new KafkaField("code", "3", "TINYINT", "", Byte.parseByte("101")));
        return resultSet;
    }

    private GenericRecord createGenericRecord() {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        String schemaString = "{\"type\": \"record\",\"name\":\"" + GREETINGS_TOPIC + "\",\"fields\": [{\"name\": \"id\", \"type\": \"int\"},{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"greeting\",\"type\": \"string\"}]}";
        org.apache.avro.Schema schema = parser.parse(schemaString);
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 1);
        record.put("name", "John");
        record.put("greeting", "Hello");

        return record;
    }

    private DynamicMessage createDynamicRecord() {
        String schema = "syntax = \"proto3\";\n" +
                "message " + PROTOBUF_TEST_TOPIC + " {\n" +
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

    private Schema createSchema(TopicSchema topicSchema) {
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

    private Schema createAvroSchema(AvroTopicSchema avroTopicSchema) {
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

    private Schema createProtobufSchema(ProtobufSchema protobufTopicSchema) {
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
        return new ProtobufSchema(protobuf);
    }

    @Test
    public void readWithConstraint_withAvroEndOffsetZero_returnsZeroRows() throws Exception {
        avroConsumer = new MockConsumer<>(EARLIEST);

        Schema schema = createAvroSchema(createAvroTopicSchema());
        setupKafkaMocks(avroConsumer, GREETINGS_TOPIC, 0L, schema);
        setupGlueMocks(getAvroSchemaVersionResponse());

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, null);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void readWithConstraint_withProtobufEndOffsetZero_returnsZeroRows() throws Exception {
        protobufConsumer = new MockConsumer<>(EARLIEST);

        Schema schema = createProtobufSchema(createProtobufTopicSchema());
        setupKafkaMocks(protobufConsumer, PROTOBUF_TEST_TOPIC, 0L, schema);
        setupGlueMocks(getProtobufSchemaVersionResponse());

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, null);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void readWithConstraint_withAvroQueryNotRunning_returnsZeroRows() throws Exception {
        Schema schema = createAvroSchema(createAvroTopicSchema());
        setupKafkaMocks(avroConsumer, GREETINGS_TOPIC, 1L, schema);
        setupGlueMocks(getAvroSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void readWithConstraint_withProtobufQueryNotRunning_returnsZeroRows() throws Exception {
        Schema schema = createProtobufSchema(createProtobufTopicSchema());
        setupKafkaMocks(protobufConsumer, PROTOBUF_TEST_TOPIC, 1L, schema);
        setupGlueMocks(getProtobufSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void readWithConstraint_withAvroContinuousEmptyData_returnsZeroRows() throws Exception {
        avroConsumer = new MockConsumer<>(EARLIEST);

        Schema schema = createAvroSchema(createAvroTopicSchema());
        setupKafkaMocks(avroConsumer, GREETINGS_TOPIC, 10L, schema);
        setupGlueMocks(getAvroSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void readWithConstraint_withProtobufContinuousEmptyData_returnsZeroRows() throws Exception {
        protobufConsumer = new MockConsumer<>(EARLIEST);

        Schema schema = createProtobufSchema(createProtobufTopicSchema());
        setupKafkaMocks(protobufConsumer, PROTOBUF_TEST_TOPIC, 10L, schema);
        setupGlueMocks(getProtobufSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void readWithConstraint_withAvroNullRecords_returnsZeroRows() throws Exception {
        avroConsumer = new MockConsumer<>(EARLIEST);

        // Add only null records to test null handling
        ConsumerRecord<String, GenericRecord> nullRecord1 = createAvroConsumerRecord(0, "k1", null);
        ConsumerRecord<String, GenericRecord> nullRecord2 = new ConsumerRecord<>(GREETINGS_TOPIC, PARTITION, 1, "k2", null);

        avroConsumer.schedulePollTask(() -> {
            avroConsumer.addRecord(nullRecord1);
            avroConsumer.addRecord(nullRecord2);
        });

        Schema schema = createAvroSchema(createAvroTopicSchema());
        setupKafkaMocks(avroConsumer, GREETINGS_TOPIC, 2L, schema);
        setupGlueMocks(getAvroSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test
    public void readWithConstraint_withProtobufNullRecords_returnsZeroRows() throws Exception {
        protobufConsumer = new MockConsumer<>(EARLIEST);

        // Add only null records to test null handling
        ConsumerRecord<String, DynamicMessage> nullRecord1 = createProtobufConsumerRecord(0, "k1", null);
        ConsumerRecord<String, DynamicMessage> nullRecord2 = new ConsumerRecord<>(PROTOBUF_TEST_TOPIC, PARTITION, 1, "k2", null);

        protobufConsumer.schedulePollTask(() -> {
            protobufConsumer.addRecord(nullRecord1);
            protobufConsumer.addRecord(nullRecord2);
        });

        Schema schema = createProtobufSchema(createProtobufTopicSchema());
        setupKafkaMocks(protobufConsumer, PROTOBUF_TEST_TOPIC, 2L, schema);
        setupGlueMocks(getProtobufSchemaVersionResponse());

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = createBlockSpiller(schema);
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);

        assertEquals(0, spiller.getBlock().getRowCount());
    }

    @Test(expected = RuntimeException.class)
    public void readWithConstraint_withInvalidSplitParameters_throwsRuntimeException() throws Exception {
        // Test with invalid split parameters that would cause KafkaUtils.createSplitParam to fail
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenThrow(new RuntimeException("Invalid split parameters"));

        Schema schema = createSchema(createCsvTopicSchema());
        ReadRecordsRequest request = createReadRecordsRequest(schema);
        kafkaRecordHandler.readWithConstraint(null, request, null);
    }

    @Test(expected = RuntimeException.class)
    public void readWithConstraint_whenGlueThrowsException_throwsRuntimeException() throws Exception {
        SplitParameters splitParameters = new SplitParameters(MY_TOPIC, PARTITION, 0, 1);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        // Mock Glue to throw exception
        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenThrow(new RuntimeException("Glue schema error"));

        Schema schema = createSchema(createCsvTopicSchema());
        ReadRecordsRequest request = createReadRecordsRequest(schema);
        kafkaRecordHandler.readWithConstraint(null, request, null);
    }

    private GetSchemaResponse getSchemaResponse() {
        return GetSchemaResponse.builder()
                .schemaArn(DEFAULT_ARN)
                .schemaName(DEFAULT_SCHEMA_NAME)
                .latestSchemaVersion(DEFAULT_LATEST_SCHEMA_VERSION)
                .build();
    }

    private GetSchemaVersionResponse getJsonSchemaVersionResponse() {
        return GetSchemaVersionResponse.builder()
                .schemaArn(DEFAULT_ARN)
                .schemaVersionId(DEFAULT_VERSION_ID)
                .dataFormat(JSON_FORMAT)
                .schemaDefinition("{\"topicName\": \"testtable\", \"\"message\": {\"dataFormat\": \"json\", \"fields\": [{\"name\": \"intcol\", \"mapping\": \"intcol\", \"type\": \"INTEGER\"}]}\"}")
                .build();
    }

    private GetSchemaVersionResponse getAvroSchemaVersionResponse() {
        return GetSchemaVersionResponse.builder()
                .schemaArn(DEFAULT_ARN)
                .schemaVersionId(DEFAULT_VERSION_ID)
                .dataFormat(AVRO_FORMAT)
                .schemaDefinition("{\"type\": \"record\",\"name\":\"" + GREETINGS_TOPIC + "\",\"fields\": [{\"name\": \"id\", \"type\": \"int\"},{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"greeting\",\"type\": \"string\"}]}")
                .build();
    }

    private GetSchemaVersionResponse getProtobufSchemaVersionResponse() {
        return GetSchemaVersionResponse.builder()
                .schemaArn(DEFAULT_ARN)
                .schemaVersionId(DEFAULT_VERSION_ID)
                .dataFormat(PROTOBUF_FORMAT)
                .schemaDefinition("syntax = \"proto3\";\n" +
                        "message " + PROTOBUF_TEST_TOPIC + " {\n" +
                        "string name = 1;\n" +
                        "int32 calories = 2;\n" +
                        "string colour = 3; \n" +
                        "}")
                .build();
    }
    
    private void setupKafkaMocks(MockConsumer<?, ?> consumer, String topic, long endOffset, Schema schema) {
        setupConsumerOffsets(consumer, topic, endOffset);
        SplitParameters splitParameters = new SplitParameters(topic, KafkaRecordHandlerTest.PARTITION, 0, (int) (endOffset));
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);
        if (consumer == avroConsumer) {
            mockedKafkaUtils.when(() -> KafkaUtils.getAvroKafkaConsumer(com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        } else if (consumer == protobufConsumer) {
            mockedKafkaUtils.when(() -> KafkaUtils.getProtobufKafkaConsumer(com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        } else {
            mockedKafkaUtils.when(() -> KafkaUtils.getKafkaConsumer(schema, com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        }
    }
    
    private void setupConsumerOffsets(MockConsumer<?, ?> consumer, String topic, long endOffset) {
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(topic, KafkaRecordHandlerTest.PARTITION), 0L);
        consumer.updateBeginningOffsets(offsets);
        
        offsets = new HashMap<>();
        offsets.put(new TopicPartition(topic, KafkaRecordHandlerTest.PARTITION), endOffset);
        consumer.updateEndOffsets(offsets);
    }
    
    private void setupGlueMocks(GetSchemaVersionResponse schemaVersionResponse) {
        Mockito.when(awsGlue.getSchema(any(GetSchemaRequest.class))).thenReturn(getSchemaResponse());
        Mockito.when(awsGlue.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(schemaVersionResponse);
    }

    private BlockSpiller createBlockSpiller(Schema schema) {
        return new S3BlockSpiller(amazonS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
    }
}
