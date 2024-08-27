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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MskAvroConsumer extends BaseMskConsumer<GenericRecord>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MskAvroConsumer.class);

    @Override
    protected void processRecord(BlockSpiller spiller, SplitParameters splitParameters, ConsumerRecord<String, GenericRecord> record)
    {
        getRecordProcessor().processRecord(spiller, splitParameters, record);
    }

    private MskRecordProcessor<GenericRecord> getRecordProcessor()
    {
        return (spiller, splitParameters, record) -> spiller.writeRows((Block block, int rowNum) -> {
            for (Schema.Field next : record.value().getSchema().getFields()) {
                boolean isMatched = block.offerValue(next.name(), rowNum, record.value().get(next.name()));
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
