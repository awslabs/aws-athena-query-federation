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

import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public abstract class MskDeserializer implements Deserializer<TopicResultSet>
{
    private final SimpleDateFormat dateFormat = new SimpleDateFormat();
    protected final Schema schema;

    public MskDeserializer(Schema schema)
    {
        this.schema = schema;
    }

    /**
     * Converts string data to other data type based on
     * datatype of source schema registered in Glue schema registry.
     *
     * @param field - arrow type field
     * @param value - raw value
     * @return Object
     * @throws ParseException - {@link ParseException}
     */
    public Object cast(Field field, String value) throws Exception
    {
        String type = field.getMetadata().get("type");
        String formatHint = field.getMetadata().get("formatHint");
        switch (type.toUpperCase()) {
            case "BOOLEAN":
                return Boolean.parseBoolean(value);
            case "TINYINT":
                return Byte.parseByte(value);
            case "SMALLINT":
                return Short.parseShort(value);
            case "INT":
            case "INTEGER":
                return Integer.parseInt(value);
            case "BIGINT":
                return Long.parseLong(value);
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                return Double.parseDouble(value);
            case "DATE":
            case "TIMESTAMP":
                dateFormat.applyPattern(formatHint);
                return dateFormat.parse(value);
            default:
                return value;
        }
    }
}
