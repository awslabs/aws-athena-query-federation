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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.MockedStatic;
import org.mockito.Mockito;


import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;


import static org.mockito.ArgumentMatchers.anyMap;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaRecordHandlerTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    AmazonS3 amazonS3;

    @Mock
    AWSSecretsManager awsSecretsManager;

    @Mock
    private AmazonAthena athena;

    @Mock
    FederatedIdentity federatedIdentity;

    @Mock
    SpillConfig spillConfig;

    @Mock
    BlockAllocatorImpl allocator;

    MockConsumer<String, TopicResultSet> consumer;
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
        ConsumerRecord<String, TopicResultSet> record1 = createConsumerRecord("myTopic", 0, "k1", createTopicResultSet("myTopic"));
        ConsumerRecord<String, TopicResultSet> record2 = createConsumerRecord("myTopic", 0, "k2", createTopicResultSet("myTopic"));
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record1);
            consumer.addRecord(record2);
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
    }

    @After
    public void close(){
        mockedKafkaUtils.close();
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

        SplitParameters splitParameters = new SplitParameters("myTopic", 0, 0, 1);
        Schema schema = createSchema(createCsvTopicSchema());

        mockedKafkaUtils.when(() -> KafkaUtils.getKafkaConsumer(schema, com.google.common.collect.ImmutableMap.of())).thenReturn(consumer);
        mockedKafkaUtils.when(() -> KafkaUtils.createSplitParam(anyMap())).thenReturn(splitParameters);

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        BlockSpiller spiller = new S3BlockSpiller(amazonS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
        kafkaRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);
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
                new Constraints(Collections.EMPTY_MAP),
                0,
                0);
    }

    private ConsumerRecord<String, TopicResultSet> createConsumerRecord(String topic, int partition, String key, TopicResultSet data) throws Exception {
        return new ConsumerRecord<>(topic, partition, 0, key, data);
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
}
