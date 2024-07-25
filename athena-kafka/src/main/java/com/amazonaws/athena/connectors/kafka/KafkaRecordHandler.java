/*-
 * #%L
 * athena-kafka
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
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.kafka.dto.KafkaField;
import com.amazonaws.athena.connectors.kafka.dto.SplitParameters;
import com.amazonaws.athena.connectors.kafka.dto.TopicResultSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import static com.amazonaws.athena.connectors.kafka.KafkaConstants.AVRO_DATA_FORMAT;
import static com.amazonaws.athena.connectors.kafka.KafkaConstants.PROTOBUF_DATA_FORMAT;

public class KafkaRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordHandler.class);
    private static final int MAX_EMPTY_RESULT_FOUND_COUNT = 3;

    KafkaRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(
            S3Client.create(),
            SecretsManagerClient.create(),
            AthenaClient.create(),
            configOptions);
    }

    @VisibleForTesting
    public KafkaRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, KafkaConstants.KAFKA_SOURCE, configOptions);
    }

    /**
     * generates the sql to executes on basis of where condition and executes it.
     *
     * @param spiller - instance of {@link BlockSpiller}
     * @param recordsRequest - instance of {@link ReadRecordsRequest}
     * @param queryStatusChecker - instance of {@link QueryStatusChecker}
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws Exception
    {
        // Taking the Split parameters in a readable pojo format.
        SplitParameters splitParameters = KafkaUtils.createSplitParam(recordsRequest.getSplit().getProperties());
        LOGGER.info("[kafka] {} RecordHandler running", splitParameters);
        // Set which topic and partition we are going to read.
        TopicPartition partition = new TopicPartition(splitParameters.topic, splitParameters.partition);
        Collection<TopicPartition> partitions = com.google.common.collect.ImmutableList.of(partition);
        GlueRegistryReader registryReader = new GlueRegistryReader();

        String dataFormat = registryReader.getGlueSchemaType(recordsRequest.getTableName().getSchemaName(), recordsRequest.getTableName().getTableName());
        if (dataFormat.equalsIgnoreCase(AVRO_DATA_FORMAT)) {
            try (Consumer<String, GenericRecord> kafkaAvroConsumer = KafkaUtils.getAvroKafkaConsumer(configOptions)) {
                // Assign the topic and partition into this consumer.
                kafkaAvroConsumer.assign(partitions);

                // Setting the start offset from where we are interested to read data from topic partition.
                // We have configured this start offset when we had created the split on MetadataHandler.
                kafkaAvroConsumer.seek(partition, splitParameters.startOffset);

                // If endOffsets is 0 that means there is no data close consumer and exit
                Map<TopicPartition, Long> endOffsets = kafkaAvroConsumer.endOffsets(partitions);
                if (endOffsets.get(partition) == 0) {
                    LOGGER.debug("[kafka] topic does not have data, closing consumer {}", splitParameters);
                    kafkaAvroConsumer.close();

                    // For debug insight
                    splitParameters.info = "endOffset is 0 i.e partition does not have data";
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(splitParameters.debug());
                    }
                    return;
                }
                // Consume topic data
                avroConsume(spiller, recordsRequest, queryStatusChecker, splitParameters, kafkaAvroConsumer);
            }
        }
        else if (dataFormat.equalsIgnoreCase(PROTOBUF_DATA_FORMAT)) {
            try (Consumer<String, DynamicMessage> kafkaProtobufConsumer = KafkaUtils.getProtobufKafkaConsumer(configOptions)) {
                // Assign the topic and partition into this consumer.
                kafkaProtobufConsumer.assign(partitions);

                // Setting the start offset from where we are interested to read data from topic partition.
                // We have configured this start offset when we had created the split on MetadataHandler.
                kafkaProtobufConsumer.seek(partition, splitParameters.startOffset);

                // If endOffsets is 0 that means there is no data close consumer and exit
                Map<TopicPartition, Long> endOffsets = kafkaProtobufConsumer.endOffsets(partitions);
                if (endOffsets.get(partition) == 0) {
                    LOGGER.debug("[kafka] topic does not have data, closing consumer {}", splitParameters);
                    kafkaProtobufConsumer.close();

                    // For debug insight
                    splitParameters.info = "endOffset is 0 i.e partition does not have data";
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(splitParameters.debug());
                    }
                    return;
                }
                // Consume topic data
                protobufConsume(spiller, recordsRequest, queryStatusChecker, splitParameters, kafkaProtobufConsumer);
            }
        }
        else {
            // Initiate new KafkaConsumer that MUST not belong to any consumer group.
            try (Consumer<String, TopicResultSet> kafkaConsumer = KafkaUtils.getKafkaConsumer(recordsRequest.getSchema(), configOptions)) {
                // Assign the topic and partition into this consumer.
                kafkaConsumer.assign(partitions);

                // Setting the start offset from where we are interested to read data from topic partition.
                // We have configured this start offset when we had created the split on MetadataHandler.
                kafkaConsumer.seek(partition, splitParameters.startOffset);

                // If endOffsets is 0 that means there is no data close consumer and exit
                Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions);
                if (endOffsets.get(partition) == 0) {
                    LOGGER.debug("[kafka] topic does not have data, closing consumer {}", splitParameters);
                    kafkaConsumer.close();

                    // For debug insight
                    splitParameters.info = "endOffset is 0 i.e partition does not have data";
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(splitParameters.debug());
                    }
                    return;
                }
                // Consume topic data
                consume(spiller, recordsRequest, queryStatusChecker, splitParameters, kafkaConsumer);
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(splitParameters.debug());
        }
    }

    /**
     * Consume topic data as batch.
     *
     * @param spiller - instance of {@link BlockSpiller}
     * @param recordsRequest - instance of {@link ReadRecordsRequest}
     * @param queryStatusChecker - instance of {@link QueryStatusChecker}
     * @param splitParameters - instance of {@link SplitParameters}
     * @param kafkaConsumer - instance of {@link KafkaConsumer}
     */
    private void consume(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            SplitParameters splitParameters,
            Consumer<String, TopicResultSet> kafkaConsumer)
    {
        LOGGER.info("[kafka] {} Polling for data", splitParameters);
        int emptyResultFoundCount = 0;
        try (Consumer<String, TopicResultSet> consumer = kafkaConsumer) {
            while (true) {
                if (!queryStatusChecker.isQueryRunning()) {
                    LOGGER.debug("[kafka]{}  Stopping and closing consumer due to query execution terminated by athena", splitParameters);
                    splitParameters.info = "query status is false i.e no need to work";
                    return;
                }

                // Call the poll on consumer to fetch data from kafka server
                // poll returns data as batch which can be configured.
                ConsumerRecords<String, TopicResultSet> records = consumer.poll(Duration.ofSeconds(1L));
                LOGGER.debug("[kafka] {} polled records size {}", splitParameters, records.count());

                // For debug insight
                splitParameters.pulled += records.count();

                // Keep track for how many times we are getting empty result for the polling call.
                if (records.count() == 0) {
                    emptyResultFoundCount++;
                }

                // We will close KafkaConsumer if we are getting empty result again and again.
                // Here we are comparing with a max threshold (MAX_EMPTY_RESULT_FOUNT_COUNT) to
                // stop the polling.
                if (emptyResultFoundCount >= MAX_EMPTY_RESULT_FOUND_COUNT) {
                    LOGGER.debug("[kafka] {} Closing consumer due to getting empty result from broker", splitParameters);
                    splitParameters.info = "always getting empty data i.e leaving from work";
                    return;
                }

                for (ConsumerRecord<String, TopicResultSet> record : records) {
                    // Pass batch data one by one to be processed to execute. execute method is
                    // a kind of abstraction to keep data filtering and writing on spiller separate.
                    execute(spiller, recordsRequest, queryStatusChecker, splitParameters, record);

                    // If we have reached at the end offset of the partition. we will not continue
                    // to call the polling.
                    if (record.offset() >= splitParameters.endOffset) {
                        LOGGER.debug("[kafka] {} Closing consumer due to reach at end offset (current record offset is {})", splitParameters, record.offset());

                        // For debug insight
                        splitParameters.info = String.format(
                                "reached at the end offset i.e no need to work: condition [if(record.offset() >= splitParameters.endOffset) i.e if(%s >= %s)]",
                                record.offset(),
                                splitParameters.endOffset
                        );
                        return;
                    }
                }
            }
        }
    }

    /**
     * Abstraction to keep the data filtering and writing on spiller separate.
     *
     * @param spiller - instance of {@link BlockSpiller}
     * @param recordsRequest - instance of {@link ReadRecordsRequest}
     * @param queryStatusChecker - instance of {@link QueryStatusChecker}
     * @param splitParameters - instance of {@link SplitParameters}
     * @param record - instance of {@link ConsumerRecord}
     */
    private void execute(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            SplitParameters splitParameters,
            ConsumerRecord<String, TopicResultSet> record)
    {
        final boolean[] isExecuted = {false};
        spiller.writeRows((Block block, int rowNum) -> {
            for (KafkaField field : record.value().getFields()) {
                boolean isMatched = block.offerValue(field.getName(), rowNum, field.getValue());
                if (!isMatched) {
                    LOGGER.debug("[FailedToSpill] {} Failed to spill record, offset: {}", splitParameters, record.offset());
                    return 0;
                }
            }
            // For debug insight
            splitParameters.spilled += 1;
            return 1;
        });
    }

    private void avroConsume(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            SplitParameters splitParameters,
            Consumer<String, GenericRecord> kafkaAvroConsumer)
    {
        LOGGER.info("[kafka] {} Polling for data", splitParameters);
        int emptyResultFoundCount = 0;
        try (Consumer<String, GenericRecord> avroConsumer = kafkaAvroConsumer) {
            while (true) {
                if (!queryStatusChecker.isQueryRunning()) {
                    LOGGER.debug("[kafka]{}  Stopping and closing consumer due to query execution terminated by athena", splitParameters);
                    splitParameters.info = "query status is false i.e no need to work";
                    return;
                }

                // Call the poll on consumer to fetch data from kafka server
                // poll returns data as batch which can be configured.
                ConsumerRecords<String, GenericRecord> records = avroConsumer.poll(Duration.ofSeconds(1L));
                LOGGER.debug("[kafka] {} polled records size {}", splitParameters, records.count());

                // For debug insight
                splitParameters.pulled += records.count();

                // Keep track for how many times we are getting empty result for the polling call.
                if (records.count() == 0) {
                    emptyResultFoundCount++;
                }

                // We will close KafkaConsumer if we are getting empty result again and again.
                // Here we are comparing with a max threshold (MAX_EMPTY_RESULT_FOUNT_COUNT) to
                // stop the polling.
                if (emptyResultFoundCount >= MAX_EMPTY_RESULT_FOUND_COUNT) {
                    LOGGER.debug("[kafka] {} Closing consumer due to getting empty result from broker", splitParameters);
                    splitParameters.info = "always getting empty data i.e leaving from work";
                    return;
                }

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    // Pass batch data one by one to be processed to execute. execute method is
                    // a kind of abstraction to keep data filtering and writing on spiller separate.
                    avroExecute(spiller, recordsRequest, queryStatusChecker, splitParameters, record);

                    // If we have reached at the end offset of the partition. we will not continue
                    // to call the polling.
                    if (record.offset() >= splitParameters.endOffset) {
                        LOGGER.debug("[kafka] {} Closing consumer due to reach at end offset (current record offset is {})", splitParameters, record.offset());

                        // For debug insight
                        splitParameters.info = String.format(
                                "reached at the end offset i.e no need to work: condition [if(record.offset() >= splitParameters.endOffset) i.e if(%s >= %s)]",
                                record.offset(),
                                splitParameters.endOffset
                        );
                        return;
                    }
                }
            }
        }
    }

    private void avroExecute(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            SplitParameters splitParameters,
            ConsumerRecord<String, GenericRecord> record)
    {
        spiller.writeRows((Block block, int rowNum) -> {
            for (Schema.Field next : record.value().getSchema().getFields()) {
                boolean isMatched = block.offerValue(next.name(), rowNum, record.value().get(next.name()));
                if (!isMatched) {
                    LOGGER.debug("[FailedToSpill] {} Failed to spill record, offset: {}", splitParameters, record.offset());
                    return 0;
                }
            }
            // For debug insight
            splitParameters.spilled += 1;
            return 1;
        });
    }

    private void protobufConsume(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            SplitParameters splitParameters,
            Consumer<String, DynamicMessage> kafkaProtobufConsumer)
    {
        LOGGER.info("[kafka] {} Polling for data", splitParameters);
        int emptyResultFoundCount = 0;
        try (Consumer<String, DynamicMessage> protobufConsumer = kafkaProtobufConsumer) {
            while (true) {
                if (!queryStatusChecker.isQueryRunning()) {
                    LOGGER.debug("[kafka]{}  Stopping and closing consumer due to query execution terminated by athena", splitParameters);
                    splitParameters.info = "query status is false i.e no need to work";
                    return;
                }

                // Call the poll on consumer to fetch data from kafka server
                // poll returns data as batch which can be configured.
                ConsumerRecords<String, DynamicMessage> records = protobufConsumer.poll(Duration.ofSeconds(1L));
                LOGGER.debug("[kafka] {} polled records size {}", splitParameters, records.count());

                // For debug insight
                splitParameters.pulled += records.count();

                // Keep track for how many times we are getting empty result for the polling call.
                if (records.count() == 0) {
                    emptyResultFoundCount++;
                }

                // We will close KafkaConsumer if we are getting empty result again and again.
                // Here we are comparing with a max threshold (MAX_EMPTY_RESULT_FOUNT_COUNT) to
                // stop the polling.
                if (emptyResultFoundCount >= MAX_EMPTY_RESULT_FOUND_COUNT) {
                    LOGGER.debug("[kafka] {} Closing consumer due to getting empty result from broker", splitParameters);
                    splitParameters.info = "always getting empty data i.e leaving from work";
                    return;
                }

                for (ConsumerRecord<String, DynamicMessage> record : records) {
                    // Pass batch data one by one to be processed to execute. execute method is
                    // a kind of abstraction to keep data filtering and writing on spiller separate.
                    protobufExecute(spiller, recordsRequest, queryStatusChecker, splitParameters, record);

                    // If we have reached at the end offset of the partition. we will not continue
                    // to call the polling.
                    if (record.offset() >= splitParameters.endOffset) {
                        LOGGER.debug("[kafka] {} Closing consumer due to reach at end offset (current record offset is {})", splitParameters, record.offset());

                        // For debug insight
                        splitParameters.info = String.format(
                                "reached at the end offset i.e no need to work: condition [if(record.offset() >= splitParameters.endOffset) i.e if(%s >= %s)]",
                                record.offset(),
                                splitParameters.endOffset
                        );
                        return;
                    }
                }
            }
        }
    }
    private void protobufExecute(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            SplitParameters splitParameters,
            ConsumerRecord<String, DynamicMessage> record)
    {
        spiller.writeRows((Block block, int rowNum) -> {
            for (Descriptors.FieldDescriptor next : record.value().getAllFields().keySet()) {
                boolean isMatched = block.offerValue(next.getName(), rowNum, record.value().getField(next));
                if (!isMatched) {
                    LOGGER.debug("[FailedToSpill] {} Failed to spill record, offset: {}", splitParameters, record.offset());
                    return 0;
                }
            }
            // For debug insight
            splitParameters.spilled += 1;
            return 1;
        });
    }
}
