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

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MskRecordProcessor<T>
{
    void processRecord(BlockSpiller spiller, SplitParameters splitParameters, ConsumerRecord<String, T> record);
}
