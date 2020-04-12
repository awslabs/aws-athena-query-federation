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
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.LinkedHashMap;

class ElasticsearchHelper {
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
    private static void parseMapping(String prefix, LinkedHashMap<String, Object> mapping, SchemaBuilder builder) {
        for (String key : mapping.keySet()) {
            String fieldName = prefix.isEmpty() ? key : prefix + "." + key;
            LinkedHashMap<String, Object> currMapping = (LinkedHashMap<String, Object>) mapping.get(key);

            if (currMapping.containsKey("properties")) {
                parseMapping(fieldName, (LinkedHashMap<String, Object>) currMapping.get("properties"), builder);
            }
            else if (currMapping.containsKey("type")) {
                builder.addStringField(fieldName);
            }
        }
    }

    /**
     * parseMapping
     *
     * Main parsing method for the GET <index>/_mapping request.
     *
     * @param mapping is the structure that contains the mapping for all elements for the index.
     * @return returns a Schema derived from the mapping.
     */
    public static Schema parseMapping(LinkedHashMap<String, Object> mapping) {
        LinkedHashMap<String, String> schema = new LinkedHashMap<>();
        SchemaBuilder builder = SchemaBuilder.newBuilder();
        String fieldName = "";

        if (mapping.containsKey("properties")) {
            parseMapping(fieldName, (LinkedHashMap<String, Object>) mapping.get("properties"), builder);
        }

        return builder.build();
    }
}