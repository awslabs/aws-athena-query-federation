/*-
 * #%L
 * Athena MSK Connector
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
package com.amazonaws.athena.connectors.msk.consumer;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MskProtobufConsumer extends BaseMskConsumer<DynamicMessage>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MskProtobufConsumer.class);

    @Override
    protected void processRecord(BlockSpiller spiller, SplitParameters splitParameters, ConsumerRecord<String, DynamicMessage> record)
    {
        getRecordProcessor().processRecord(spiller, splitParameters, record);
    }

    private MskRecordProcessor<DynamicMessage> getRecordProcessor()
    {
        return (spiller, splitParameters, record) -> spiller.writeRows((Block block, int rowNum) -> {
            for (Descriptors.FieldDescriptor next : record.value().getAllFields().keySet()) {
                boolean isMatched = block.offerValue(next.getName(), rowNum, record.value().getField(next));
                if (!isMatched) {
                    LOGGER.debug("[FailedToSpill] {} Failed to spill record, offset: {}", splitParameters, record.offset());
                    return 0;
                }
            }
            splitParameters.spilled += 1;
            return 1;
        });
    }
}
