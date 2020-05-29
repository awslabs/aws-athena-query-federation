/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class has interfaces used for the parsing and creation of a schema based on an index mapping retrieved
 * from an Elasticsearch instance. It also has an interface for converting Elasticsearch data types to Apache Arrow.
 */
class ElasticsearchSchemaUtil
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSchemaUtil.class);

    // Used in parseMapping() to store the _meta structure (the mapping containing the fields that should be
    // considered a list).
    private LinkedHashMap<String, Object> meta = new LinkedHashMap<>();

    // Used in parseMapping() to build the schema recursively.
    private SchemaBuilder builder;

    protected ElasticsearchSchemaUtil() {}

    /**
     * Main parsing method for the GET <index>/_mapping request.
     * @param mapping is the structure that contains the mapping for all elements for the index.
     * @param metaMap is the structure in the mapping containing the fields that should be considered a list.
     * @return a Schema derived from the mapping.
     */
    protected Schema parseMapping(LinkedHashMap<String, Object> mapping, LinkedHashMap<String, Object> metaMap)
    {
        logger.info("parseMapping - enter");

        builder = SchemaBuilder.newBuilder();
        meta.clear();
        meta.putAll(metaMap);

        if (mapping.containsKey("properties")) {
            LinkedHashMap<String, Object> fields = (LinkedHashMap) mapping.get("properties");

            for (String fieldName : fields.keySet()) {
                builder.addField(inferField(fieldName, fieldName, (LinkedHashMap) fields.get(fieldName)));
            }
        }

        return builder.build();
    }

    /**
     * Parses the response to GET index/_mapping recursively to derive the index's schema.
     * @param fieldName is the name of the current field being processed (e.g. street).
     * @param qualifiedName is the qualified name of the field (e.g. address.street).
     * @param mapping is the current map of the element in question.
     * @return a Field object injected with the field's info.
     */
    private Field inferField(String fieldName, String qualifiedName, LinkedHashMap<String, Object> mapping)
    {
        Field field;

        if (mapping.containsKey("properties")) {
            // Process STRUCT.
            LinkedHashMap<String, Object> childFields = (LinkedHashMap) mapping.get("properties");
            List<Field> children = new ArrayList<>();

            for (String childField : childFields.keySet()) {
                children.add(inferField(childField, qualifiedName + "." + childField,
                        (LinkedHashMap) childFields.get(childField)));
            }

            field = new Field(fieldName, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
        }
        else {
            field = new Field(fieldName, toFieldType(mapping), null);

            if (meta.containsKey(qualifiedName)) {
                // Process LIST.
                return new Field(fieldName, FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(field));
            }
        }

        return field;
    }

    /**
     * Convert the data type from Elasticsearch to Arrow and injects it in a FieldType.
     * @param mapping is the map containing the Elasticsearch datatype.
     * @return a new FieldType corresponding to the Elasticsearch type.
     */
    private FieldType toFieldType(LinkedHashMap<String, Object> mapping)
    {
        logger.info("toFieldType - enter: " + mapping);

        String elasticType = (String) mapping.get("type");
        Types.MinorType minorType;
        Map<String, String> metadata = new HashMap<>();

        switch (elasticType) {
            case "text":
            case "keyword":
            case "binary":
                minorType = Types.MinorType.VARCHAR;
                break;
            case "long":
                minorType = Types.MinorType.BIGINT;
                break;
            case "integer":
                minorType = Types.MinorType.INT;
                break;
            case "short":
                minorType = Types.MinorType.SMALLINT;
                break;
            case "byte":
                minorType = Types.MinorType.TINYINT;
                break;
            case "double":
                minorType = Types.MinorType.FLOAT8;
                break;
            case "scaled_float":
                minorType = Types.MinorType.BIGINT;
                metadata.put("scaling_factor", mapping.get("scaling_factor").toString());
                break;
            case "float":
            case "half_float":
                minorType = Types.MinorType.FLOAT4;
                break;
            case "date":
            case "date_nanos":
                minorType = Types.MinorType.DATEMILLI;
                break;
            case "boolean":
                minorType = Types.MinorType.BIT;
                break;
            default:
                minorType = Types.MinorType.NULL;
                break;
        }

        logger.info("Arrow Type: {}, metadata: {}", minorType.toString(), metadata);

        return new FieldType(true, minorType.getType(), null, metadata);
    }
}
