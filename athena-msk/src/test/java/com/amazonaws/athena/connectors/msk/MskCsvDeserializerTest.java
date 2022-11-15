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

import com.amazonaws.athena.connectors.msk.dto.TopicSchema;
import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import com.amazonaws.athena.connectors.msk.serde.MskCsvDeserializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MskCsvDeserializerTest extends MskAbstractDeserializerTest
{
    @Test
    public void testMskCsvDeserializer() throws Exception
    {
        TopicSchema topicSchema = createCsvTopicSchema();
        Schema schema = createSchema(topicSchema);
        MskCsvDeserializer csvDeserializer = new MskCsvDeserializer(schema);
        TopicResultSet resultSet;

        resultSet = csvDeserializer.deserialize("test", null);
        assertNull(resultSet);

        resultSet = csvDeserializer.deserialize("test", "abc, James, true, 11, 2255, 120.30, 2000-01-01".getBytes(StandardCharsets.UTF_8));
        assertEquals(resultSet.getDataFormat(), topicSchema.getMessage().getDataFormat());

        resultSet = csvDeserializer.deserialize("test", "10, James, true, 11, 2255, 120.30, 2000-01-01".getBytes(StandardCharsets.UTF_8));
        assertEquals(resultSet.getDataFormat(), topicSchema.getMessage().getDataFormat());
    }

    @Test
    public void testExceptionInMskCsvDeserializer() throws Exception
    {
        TopicSchema topicSchema = createCsvTopicSchema();
        Schema schema = createSchemaForException(topicSchema);
        MskCsvDeserializer csvDeserializer = new MskCsvDeserializer(schema);
        csvDeserializer.deserialize("test", "10, James, true, 11, 2255, 120.30, 2000-01-01".getBytes(StandardCharsets.UTF_8));
    }

    private TopicSchema createCsvTopicSchema() throws JsonProcessingException
    {
        String csv = "{" +
                "\"tableName\":\"test\"," +
                "\"schemaName\":\"default\"," +
                "\"topicName\":\"test\"," +
                "\"message\":{" +
                "\"dataFormat\":\"csv\"," +
                "\"fields\":[" +
                "{\"name\":\"id\",\"type\":\"INTEGER\",\"mapping\":\"0\", \"formatHint\": \"\"}," +
                "{\"name\":\"name\",\"type\":\"VARCHAR\",\"mapping\":\"1\", \"formatHint\": \"\"}," +
                "{\"name\":\"isActive\",\"type\":\"BOOLEAN\",\"mapping\":\"2\", \"formatHint\": \"\"}," +
                "{\"name\":\"code\",\"type\":\"TINYINT\",\"mapping\":\"3\", \"formatHint\": \"\"}," +
                "{\"name\":\"zip\",\"type\":\"SMALLINT\",\"mapping\":\"4\", \"formatHint\": \"\"}," +
                "{\"name\":\"salary\",\"type\":\"DECIMAL\",\"mapping\":\"5\", \"formatHint\": \"\"}," +
                "{\"name\":\"dob\",\"type\":\"DATE\",\"mapping\":\"6\", \"formatHint\": \"yyyy-dd-MM\"}" +
                "]}}";
        return objectMapper.readValue(csv, TopicSchema.class);
    }
}
