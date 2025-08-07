/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * Collection of helpful utilities that handle Neptune schema inference, type, and naming conversion.
 */
public class NeptuneSchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(NeptuneSchemaUtils.class);

    private NeptuneSchemaUtils() {}

    public static Schema getSchemaFromResults(Map resultsMap, String componentTypeValue, String tableName)
    {
        Schema schema;
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        //Building schema from gremlin/sparql query results.
        resultsMap.forEach((columnName, columnValue) -> buildSchema(columnName.toString(), columnValue,  schemaBuilder));
        schemaBuilder.addMetadata(Constants.SCHEMA_COMPONENT_TYPE, componentTypeValue);
        schemaBuilder.addMetadata(Constants.SCHEMA_GLABEL, tableName);
        schema = schemaBuilder.build();
        return schema;
    }

    private static void buildSchema(String columnName, Object columnValue, SchemaBuilder schemaBuilder)
    {
        schemaBuilder.addField(getArrowFieldForNeptune(columnName, columnValue));
    }

    /**
     * Infers the type of a field from Neptune data.
     *
     * @param key The key of the field we are attempting to infer.
     * @param value A value from the key whose type we are attempting to infer.
     * @return The Apache Arrow field definition of the inferred key/value.
     */
    private static Field getArrowFieldForNeptune(String key, Object value)
    {
        if (value instanceof String || value instanceof java.util.UUID) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (value instanceof Integer) {
            return new Field(key, FieldType.nullable(Types.MinorType.INT.getType()), null);
        }
        else if (value instanceof BigInteger) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        }
        else if (value instanceof Long) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        }
        else if (value instanceof Boolean) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIT.getType()), null);
        }
        else if (value instanceof Float) {
            return new Field(key, FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        }
        else if (value instanceof Double) {
            return new Field(key, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        }
        else if (value instanceof java.util.Date) {
            return new Field(key, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        }
        else if (value instanceof List) {
            return getArrowFieldForNeptune(key, ((List<?>) value).get(0));
        }

        String className = (value == null || value.getClass() == null) ? "null" : value.getClass().getName();
        logger.warn("Unknown type[{}] for field[{}], defaulting to varchar.", className, key);
        return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
    }
}
