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
import java.util.Objects;

/**
 * This class has interfaces used for the parsing and creation of a schema based on an index mapping retrieved
 * from an Elasticsearch instance. It also has an interface for converting Elasticsearch data types to Apache Arrow.
 */
class ElasticsearchSchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSchemaUtils.class);

    // Used in parseMapping() to store the _meta structure (the mapping containing the fields that should be
    // considered a list).
    private LinkedHashMap<String, Object> meta = new LinkedHashMap<>();

    // Used in parseMapping() to build the schema recursively.
    private SchemaBuilder builder;

    protected ElasticsearchSchemaUtils() {}

    /**
     * Main parsing method for the GET <index>/_mapping request.
     * @param mappings is the structure that contains the metadata definitions for the index, as well as the _meta
     *                 property used to define list fields.
     * @return a Schema derived from the mapping.
     */
    protected Schema parseMapping(LinkedHashMap<String, Object> mappings)
    {
        logger.info("parseMapping - enter");

        builder = SchemaBuilder.newBuilder();

        // Elasticsearch does not have a dedicated array type. All fields can contain zero or more elements
        // so long as they are of the same type. For this reasons, users will have to add a _meta property
        // to the indices they intend on using with Athena. This property is used in the building of the
        // Schema to indicate which fields should be considered a LIST.
        meta.clear();
        if (mappings.containsKey("_meta")) {
            meta.putAll((LinkedHashMap) mappings.get("_meta"));
        }

        if (mappings.containsKey("properties")) {
            LinkedHashMap<String, Object> fields = (LinkedHashMap) mappings.get("properties");

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

    /**
     * Checks that two mappings are equal.
     * @param mapping1 is a mapping to be compared.
     * @param mapping2 is a mapping to be compared.
     * @return true if the lists are equal, false otherwise.
     */
    protected final boolean mappingsEqual(Schema mapping1, Schema mapping2)
    {
        logger.info("mappingsEqual - Enter:\nMapping1: {}\nMapping2: {}", mapping1, mapping2);

        // Schemas must have the same number of elements.
        if (mapping1.getFields().size() != mapping2.getFields().size()) {
            logger.warn("Mappings are different sizes!");
            return false;
        }

        // Mappings must have the same fields (irrespective of internal ordering).
        for (Field field1 : mapping1.getFields()) {
            Field field2 = mapping2.findField(field1.getName());
            if (field2 == null || field1.getType() != field2.getType()) {
                logger.warn("Mapping fields mismatch!");
                return false;
            }

            switch(Types.getMinorTypeForArrowType(field1.getType())) {
                // process complex/nested types (LIST and STRUCT), the children fields must also equal.
                case LIST:
                case STRUCT:
                    if (!childrenEqual(field1.getChildren(), field2.getChildren())) {
                        logger.warn("Children fields mismatch!");
                        return false;
                    }
                    break;
                default:
                    // For non-complex types, compare the metadata as well.
                    if (!Objects.equals(field1.getMetadata(), field2.getMetadata())) {
                        logger.warn("Fields' metadata mismatch!");
                        return false;
                    }
                    break;
            }
        }

        return true;
    }

    /**
     * Used to assert that children fields inside two mappings are equal.
     * @param list1 is a list of children fields to be compared.
     * @param list2 is a list of children fields to be compared.
     * @return true if the lists are equal, false otherwise.
     */
    private final boolean childrenEqual(List<Field> list1, List<Field> list2)
    {
        logger.info("childrenEqual - Enter:\nChildren1: {}\nChildren2: {}", list1, list2);

        // Children lists must have the same number of elements.
        if (list1.size() != list2.size()) {
            logger.warn("Children lists are different sizes!");
            return false;
        }

        Map<String, Field> fields = new LinkedHashMap<>();
        list2.forEach(value -> fields.put(value.getName(), value));

        // lists must have the same Fields (irrespective of internal ordering).
        for (Field field1 : list1) {
            Field field2 = fields.get(field1.getName());
            if (field2 == null || field1.getType() != field2.getType()) {
                logger.warn("Children fields mismatch!");
                return false;
            }
            // process complex/nested types (LIST and STRUCT), the children fields must also equal.
            switch(Types.getMinorTypeForArrowType(field1.getType())) {
                case LIST:
                case STRUCT:
                    if (!childrenEqual(field1.getChildren(), field2.getChildren())) {
                        return false;
                    }
                    break;
                default:
                    // For non-complex types, compare the metadata as well.
                    if (!Objects.equals(field1.getMetadata(), field2.getMetadata())) {
                        logger.warn("Fields' metadata mismatch!");
                        return false;
                    }
                    break;
            }
        }

        return true;
    }
}
