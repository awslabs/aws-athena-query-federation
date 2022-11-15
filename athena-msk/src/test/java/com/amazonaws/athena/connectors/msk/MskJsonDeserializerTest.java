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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import com.amazonaws.athena.connectors.msk.dto.TopicSchema;
import com.amazonaws.athena.connectors.msk.serde.MskJsonDeserializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MskJsonDeserializerTest extends MskAbstractDeserializerTest
{
    @Test
    public void testMskJsonDeserializer() throws Exception {
        TopicSchema topicSchema = createJsonTopicSchema();
        Schema schema = createSchema(topicSchema);
        MskJsonDeserializer jsonDeserializer = new MskJsonDeserializer(schema);
        TopicResultSet resultSet;

        resultSet = jsonDeserializer.deserialize("test", null);
        assertNull(resultSet);

        resultSet = jsonDeserializer.deserialize("test", "{\"id\": \"abc\", \"name\": \"James\", \"isActive\": \"true\", \"phone\": \"1010010110\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(resultSet.getDataFormat(), topicSchema.getMessage().getDataFormat());

        resultSet = jsonDeserializer.deserialize("test", "{\"id\": \"10\", \"name\": \"James\", \"isActive\": \"true\", \"phone\": \"1010010110\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(resultSet.getDataFormat(), topicSchema.getMessage().getDataFormat());
    }

    @Test
    public void testExceptionInMskJsonDeserializer() throws Exception
    {
        TopicSchema topicSchema = createJsonTopicSchema();
        Schema schema = createSchemaForException(topicSchema);
        MskJsonDeserializer jsonDeserializer = new MskJsonDeserializer(schema);
        jsonDeserializer.deserialize("test", "{\"id\": \"10\", \"name\": \"James\", \"isActive\": \"true\", \"phone\": \"1010010110\"}".getBytes(StandardCharsets.UTF_8));
    }

    private TopicSchema createJsonTopicSchema() throws JsonProcessingException
    {
        String json = "{" +
                "\"tableName\":\"test\"," +
                "\"schemaName\":\"default\"," +
                "\"topicName\":\"test\"," +
                "\"message\":{" +
                "\"dataFormat\":\"json\"," +
                "\"fields\":[" +
                "{\"name\":\"id\",\"type\":\"INTEGER\",\"mapping\":\"id\", \"formatHint\": \"\"}," +
                "{\"name\":\"name\",\"type\":\"VARCHAR\",\"mapping\":\"name\", \"formatHint\": \"\"}," +
                "{\"name\":\"isActive\",\"type\":\"BOOLEAN\",\"mapping\":\"isActive\", \"formatHint\": \"\"}," +
                "{\"name\":\"phone\",\"type\":\"BIGINT\",\"mapping\":\"phone\", \"formatHint\": \"\"}" +
                "]}}";
        return objectMapper.readValue(json, TopicSchema.class);
    }
}
