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

import com.amazonaws.athena.connectors.msk.dto.MSKField;
import com.amazonaws.athena.connectors.msk.dto.Message;
import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MskCsvDeserializer extends MskDeserializer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MskCsvDeserializer.class);
    public MskCsvDeserializer(Schema schema)
    {
        super(schema);
    }

    @Override
    public TopicResultSet deserialize(String topic, byte[] data)
    {
        if (data == null) {
            return null;
        }
        // Initiating TopicResultSet pojo to put the raw data.
        TopicResultSet topicResultSet = new TopicResultSet();
        topicResultSet.setTopicName(topic);
        topicResultSet.setDataFormat(Message.DATA_FORMAT_CSV);

        try {
            // Transforming the topic raw (csv) data to string List.
            List<String> words = Arrays
                    .stream(new String(data, StandardCharsets.UTF_8).split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());

            // Creating Field object for each fields in raw data.
            // Also putting additional information in fields from fields metadata.
            schema.getFields().forEach(field -> {
                String mapping = field.getMetadata().get("mapping");
                try {
                    topicResultSet.getFields().add(new MSKField(
                            field.getName(),
                            mapping,
                            field.getMetadata().get("type"),
                            field.getMetadata().get("formatHint"),
                            castValue(field, words.get(Integer.parseInt(mapping)))
                    ));
                }
                catch (Exception e) {
                    LOGGER.error("MskCsvDeserializer: Error in castValue : while converting raw value to typed value", e);
                }
            });
            return topicResultSet;
        }
        catch (Exception e) {
            LOGGER.error("MskCsvDeserializer: Error when deserializing byte[] to TopicResultSet", e);
        }
        finally {
            close();
        }
        return topicResultSet;
    }

    /**
     * Converts raw value to typed value, if fails returns null.
     *
     * @param field - arrow type field
     * @param value - raw value
     * @return Object
     */
    private Object castValue(org.apache.arrow.vector.types.pojo.Field field, String value) throws Exception
    {
        return cast(field, value);
    }
}
