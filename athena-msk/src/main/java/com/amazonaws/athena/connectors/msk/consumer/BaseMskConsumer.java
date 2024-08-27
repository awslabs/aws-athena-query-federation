/*-
 * #%L
 * Athena MSK Connector
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.msk.consumer;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public abstract class BaseMskConsumer<T> implements MskConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMskConsumer.class);
    protected static final int MAX_EMPTY_RESULT_FOUND_COUNT = 3;

    @Override
    public void consume(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker, SplitParameters splitParameters, Consumer<?, ?> consumer)
    {
        @SuppressWarnings("unchecked")
        Consumer<String, T> typedConsumer = (Consumer<String, T>) consumer;
        typedConsumer.assign(Collections.singleton(new TopicPartition(splitParameters.topic, splitParameters.partition)));
        typedConsumer.seek(new TopicPartition(splitParameters.topic, splitParameters.partition), splitParameters.startOffset);

        Map<TopicPartition, Long> endOffsets = typedConsumer.endOffsets(Collections.singleton(new TopicPartition(splitParameters.topic, splitParameters.partition)));
        if (endOffsets.get(new TopicPartition(splitParameters.topic, splitParameters.partition)) == 0) {
            LOGGER.debug("[kafka] topic does not have data, closing consumer {}", splitParameters);
            typedConsumer.close();
            splitParameters.info = "endOffset is 0 i.e partition does not have data";
            return;
        }

        pollAndProcess(spiller, queryStatusChecker, splitParameters, typedConsumer);
    }

    private void pollAndProcess(BlockSpiller spiller, QueryStatusChecker queryStatusChecker, SplitParameters splitParameters, Consumer<String, T> consumer)
    {
        LOGGER.info("[kafka] {} Polling for data", splitParameters);
        int emptyResultFoundCount = 0;
        while (true) {
            if (!queryStatusChecker.isQueryRunning()) {
                LOGGER.debug("[kafka]{} Stopping and closing consumer due to query execution terminated by athena", splitParameters);
                splitParameters.info = "query status is false i.e no need to work";
                return;
            }

            ConsumerRecords<String, T> records = consumer.poll(Duration.ofSeconds(1L));
            LOGGER.debug("[kafka] {} polled records size {}", splitParameters, records.count());

            splitParameters.pulled += records.count();

            if (records.count() == 0) {
                emptyResultFoundCount++;
            }

            if (emptyResultFoundCount >= MAX_EMPTY_RESULT_FOUND_COUNT) {
                LOGGER.debug("[kafka] {} Closing consumer due to getting empty result from broker", splitParameters);
                splitParameters.info = "always getting empty data i.e leaving from work";
                return;
            }

            for (ConsumerRecord<String, T> record : records) {
                processRecord(spiller, splitParameters, record);

                if (record.offset() >= splitParameters.endOffset) {
                    LOGGER.debug("[kafka] {} Closing consumer due to reach at end offset (current record offset is {})", splitParameters, record.offset());
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

    protected abstract void processRecord(BlockSpiller spiller, SplitParameters splitParameters, ConsumerRecord<String, T> record);
}
