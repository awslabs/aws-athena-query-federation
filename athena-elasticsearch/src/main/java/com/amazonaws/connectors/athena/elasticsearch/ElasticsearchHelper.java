/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.LinkedHashMap;

class ElasticsearchHelper {

    /**
     * toArrowType
     *
     * Convert the data type from Elasticsearch to Arrow.
     *
     * @param elasticType is the Elasticsearch datatype.
     * @return an ArrowType corresponding to the Elasticsearch type (default value is a VARCHAR).
     */
    public static ArrowType toArrowType(String elasticType)
    {
        switch (elasticType) {
            // case "text":
            // case "keyword":
            //    return Types.MinorType.VARCHAR.getType();
            case "long":
                return Types.MinorType.BIGINT.getType();
            case "integer":
                return Types.MinorType.INT.getType();
            case "short":
                return Types.MinorType.SMALLINT.getType();
            case "byte":
                return Types.MinorType.TINYINT.getType();
            case "double":
            case "scaled_float":
                return Types.MinorType.FLOAT8.getType();
            case "float":
            case "half_float":
                return Types.MinorType.FLOAT4.getType();
            case "date":
            case "date_nanos":
                return Types.MinorType.DATEMILLI.getType();
            case "boolean":
                return Types.MinorType.BIT.getType();
            case "binary":
                return Types.MinorType.VARBINARY.getType();
            default:
                return Types.MinorType.VARCHAR.getType();
        }
    }

    /**
     * parseMapping
     *
     * Parses the response to GET index/_mapping recursively to derive the index's schema.
     *
     * @param prefix is the parent field names in the mapping structure. The final field-name will be
     *               a concatenation of the prefix and the current field-name (e.g. 'address.zip').
     * @param mapping is the current map of the element in question (e.g. address).
     * @param builder builds the schema at the iteration through the mapping.
     */
    private static void parseMapping(String prefix, LinkedHashMap<String, Object> mapping, SchemaBuilder builder)
    {
        for (String key : mapping.keySet()) {
            String fieldName = prefix.isEmpty() ? key : prefix + "." + key;
            LinkedHashMap<String, Object> currMapping = (LinkedHashMap<String, Object>) mapping.get(key);

            if (currMapping.containsKey("properties")) {
                parseMapping(fieldName, (LinkedHashMap<String, Object>) currMapping.get("properties"), builder);
            }
            else if (currMapping.containsKey("type")) {
                builder.addField(fieldName, toArrowType((String) currMapping.get("type")));
            }
        }
    }

    /**
     * parseMapping
     *
     * Main parsing method for the GET <index>/_mapping request.
     *
     * @param mapping is the structure that contains the mapping for all elements for the index.
     * @return a Schema derived from the mapping.
     */
    public static Schema parseMapping(LinkedHashMap<String, Object> mapping)
    {
        LinkedHashMap<String, String> schema = new LinkedHashMap<>();
        SchemaBuilder builder = SchemaBuilder.newBuilder();
        String fieldName = "";

        if (mapping.containsKey("properties")) {
            parseMapping(fieldName, (LinkedHashMap<String, Object>) mapping.get("properties"), builder);
        }

        return builder.build();
    }
}