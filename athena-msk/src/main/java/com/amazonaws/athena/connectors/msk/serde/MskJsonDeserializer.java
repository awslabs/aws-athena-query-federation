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
package com.amazonaws.athena.connectors.msk.serde;

import com.amazonaws.athena.connectors.msk.AmazonMskUtils;
import com.amazonaws.athena.connectors.msk.dto.MSKField;
import com.amazonaws.athena.connectors.msk.dto.Message;
import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.charset.StandardCharsets;

public class MskJsonDeserializer extends MskDeserializer
{
    public MskJsonDeserializer(Schema schema)
    {
        super(schema);
    }

    @Override
    public TopicResultSet deserialize(String topic, byte[] data)
    {
        if (data == null) {
            return null;
        }

        try {
            // Transforming the topic raw (json) data to JsonNode using ObjectMapper.
            JsonNode json = AmazonMskUtils
                    .getObjectMapper()
                    .readValue(new String(data, StandardCharsets.UTF_8), JsonNode.class);

            // Initiating TopicResultSet pojo to put the raw data.
            TopicResultSet topicResultSet = new TopicResultSet();
            topicResultSet.setTopicName(topic);
            topicResultSet.setDataFormat(Message.DATA_FORMAT_JSON);

            // Creating Field object for each fields in raw data.
            // Also putting additional information in fields from fields metadata.
            schema.getFields().forEach(field -> {
                String mapping = field.getMetadata().get("mapping");
                topicResultSet.getFields().add(new MSKField(
                        field.getName(),
                        mapping,
                        field.getMetadata().get("type"),
                        field.getMetadata().get("formatHint"),
                        castValue(field, json.get(mapping).asText())
                ));
            });

            close();
            return topicResultSet;
        }
        catch (Exception e) {
            throw new SerializationException("MskJsonDeserializer: Error when deserializing byte[] to TopicResultSet");
        }
    }

    /**
     * Converts raw value to typed value, if fails returns null.
     *
     * @param field - arrow type field
     * @param value - raw value
     * @return Object
     */
    private Object castValue(org.apache.arrow.vector.types.pojo.Field field, String value)
    {
        Object object = null;
        try {
            object = cast(field, value);
        }
        catch (Exception ignored) {
        }
        return object;
    }
}
