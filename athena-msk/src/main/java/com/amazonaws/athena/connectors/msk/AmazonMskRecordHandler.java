/*-
 * #%L
 * athena-msk
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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.msk.dto.MSKField;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AmazonMskRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskRecordHandler.class);
    private static final int MAX_EMPTY_RESULT_FOUND_COUNT = 3;

    AmazonMskRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient()
        );
    }

    @VisibleForTesting
    public AmazonMskRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena)
    {
        super(amazonS3, secretsManager, athena, AmazonMskConstants.KAFKA_SOURCE);
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
        SplitParameters splitParameters = AmazonMskUtils.createSplitParam(recordsRequest.getSplit().getProperties());
        LOGGER.info("[kafka] {} RecordHandler running", splitParameters);

        // Initiate new KafkaConsumer that MUST not belong to any consumer group.
        try (Consumer<String, TopicResultSet> kafkaConsumer = AmazonMskUtils.getKafkaConsumer(recordsRequest.getSchema())) {
            // Set which topic and partition we are going to read.
            TopicPartition partition = new TopicPartition(splitParameters.topic, splitParameters.partition);
            Collection<TopicPartition> partitions = List.of(partition);

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
        try {
            while (true) {
                if (!queryStatusChecker.isQueryRunning()) {
                    LOGGER.debug("[kafka]{}  Stopping and closing consumer due to query execution terminated by athena", splitParameters);
                    splitParameters.info = "query status is false i.e no need to work";
                    return;
                }

                // Call the poll on consumer to fetch data from kafka server
                // poll returns data as batch which can be configured.
                ConsumerRecords<String, TopicResultSet> records = kafkaConsumer.poll(Duration.ofSeconds(1L));
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
                    boolean isExecuted = execute(spiller, recordsRequest, queryStatusChecker, splitParameters, record);
                    if (!isExecuted) {
                        LOGGER.debug("[FailedToSpill] {} Failed to split record, offset: {}", splitParameters, record.offset());
                    }

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
        finally {
            kafkaConsumer.close();
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
     * @return boolean - true if all the values are spilled else false
     */
    private boolean execute(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            SplitParameters splitParameters,
            ConsumerRecord<String, TopicResultSet> record)
    {
        final boolean[] isExecuted = {false};
        spiller.writeRows((Block block, int rowNum) -> {
            for (MSKField field : record.value().getFields()) {
                boolean isMatched = block.offerValue(field.getName(), rowNum, field.getValue());
                if (!isMatched) {
                    return 0;
                }
            }
            // For debug insight
            splitParameters.spilled += 1;
            isExecuted[0] = true;
            return 1;
        });
        return isExecuted[0];
    }
}
