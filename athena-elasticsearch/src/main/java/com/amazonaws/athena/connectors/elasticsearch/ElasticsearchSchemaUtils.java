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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.util.VisibleForTesting;
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
class ElasticsearchSchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSchemaUtils.class);

    private ElasticsearchSchemaUtils() {}

    /**
     * Main parsing method for the GET <index>/_mapping request.
     * @param mappings is the structure that contains the metadata definitions for the index, as well as the _meta
     *                 property used to define list fields.
     * @return a Schema derived from the mapping.
     */
    protected static Schema parseMapping(Map<String, Object> mappings)
    {
        // Used to store the _meta structure (the mapping containing the fields that should be considered a list).
        Map<String, Object> meta = new HashMap<>();
        SchemaBuilder builder = SchemaBuilder.newBuilder();

        // Elasticsearch does not have a dedicated array type. All fields can contain zero or more elements
        // so long as they are of the same type. For this reasons, users will have to add a _meta property
        // to the indices they intend on using with Athena. This property is used in the building of the
        // Schema to indicate which fields should be considered a LIST.
        if (mappings.containsKey("_meta")) {
            meta.putAll((Map) mappings.get("_meta"));
        }

        if (mappings.containsKey("properties")) {
            Map<String, Object> fields = (Map) mappings.get("properties");

            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                Map<String, Object> value = (Map) entry.getValue();
                builder.addField(inferField(fieldName, fieldName, value, meta));
            }
        }

        return builder.build();
    }

    /**
     * Parses the response to GET index/_mapping recursively to derive the index's schema.
     * @param fieldName is the name of the current field being processed (e.g. street).
     * @param qualifiedName is the qualified name of the field (e.g. address.street).
     * @param mapping is the current map of the element in question.
     * @param meta is the map of fields that are considered to be lists.
     * @return a Field object injected with the field's info.
     */
    private static Field inferField(String fieldName, String qualifiedName,
                                    Map<String, Object> mapping, Map<String, Object> meta)
    {
        Field field;

        //For ES, only struct will has properties field.
        if (mapping.containsKey("properties")) {
            // Process STRUCT.
            Map<String, Object> childFields = (Map) mapping.get("properties");
            List<Field> children = new ArrayList<>();

            for (Map.Entry<String, Object> entry : childFields.entrySet()) {
                String childField = entry.getKey();
                Map<String, Object> value = (Map) entry.getValue();
                children.add(inferField(childField, qualifiedName + "." + childField, value, meta));
            }

            //This is to handle the case of List of Struct, if customer put _meta:list on their struct field.
            if (meta.containsKey(qualifiedName)) {
                String metaValue = (String) meta.get(qualifiedName);
                if (!metaValue.equalsIgnoreCase("list")) {
                    throw new IllegalArgumentException(String.format("_meta only support value `list`, key:%s, value:%s", qualifiedName, metaValue));
                }

                Field field1 = new Field(fieldName, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
                field = new Field(fieldName, FieldType.nullable(Types.MinorType.LIST.getType()),  Collections.singletonList(field1));
            }
            else {
                field = new Field(fieldName, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
            }
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
    private static FieldType toFieldType(Map<String, Object> mapping)
    {
        logger.debug("toFieldType - enter: " + mapping);

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
                // Store the scaling factor in the field's metadata map.
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

        logger.debug("Arrow Type: {}, metadata: {}", minorType.toString(), metadata);

        return new FieldType(true, minorType.getType(), null, metadata);
    }

    /**
     * Checks that two Schema objects are equal using the following criteria:
     * 1) The Schemas must have the same number of fields.
     * 2) The corresponding fields in the two Schema objects must also be the same irrespective of ordering within
     *    the Schema object using the following criteria:
     *    a) The fields' names must match.
     *    b) The fields' Arrow types must match.
     *    c) The fields' children lists (used for complex fields, e.g. LIST and STRUCT) must match irrespective of
     *       field ordering within the lists.
     *    d) The fields' metadata maps must match. Currently that's only applicable for scaled_float data types that
     *       use the field's metadata map to store the scaling factor associated with the data type.
     * @param mapping1 is a mapping to be compared.
     * @param mapping2 is a mapping to be compared.
     * @return true if the lists are equal, false otherwise.
     */
    @VisibleForTesting
    protected static final boolean mappingsEqual(Schema mapping1, Schema mapping2)
    {
        logger.info("mappingsEqual - Enter - Mapping1: {}, Mapping2: {}", mapping1, mapping2);

        // Schemas must have the same number of elements.
        if (mapping1.getFields().size() != mapping2.getFields().size()) {
            logger.warn("Mappings are different sizes - Mapping1: {}, Mapping2: {}",
                    mapping1.getFields().size(), mapping2.getFields().size());
            return false;
        }

        // Mappings must have the same fields (irrespective of internal ordering).
        for (Field field1 : mapping1.getFields()) {
            Field field2 = mapping2.findField(field1.getName());
            // Corresponding fields must have the same Arrow types or the Schemas are deemed not equal.
            if (field2 == null || field1.getType() != field2.getType()) {
                logger.warn("Fields' types do not match - Field1: {}, Field2: {}",
                        field1.getType(), field2 == null ? "null" : field2.getType());
                return false;
            }
            logger.info("Field1 Name: {}, Field1 Type: {}, Field1 Metadata: {}",
                    field1.getName(), field1.getType(), field1.getMetadata());
            logger.info("Field2 Name: {}, Field2 Type: {}, Field2 Metadata: {}",
                    field2.getName(), field2.getType(), field2.getMetadata());
            // The corresponding fields' children and metadata maps must also match or the Schemas are deemed not equal.
            if (!childrenEqual(field1.getChildren(), field2.getChildren()) ||
                    !field1.getMetadata().equals(field2.getMetadata())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks that two lists of Field objects (corresponding to the children lists of two corresponding fields in
     * two different Schema objects) are the same irrespective of ordering within the lists using the following
     * criteria:
     *    1) The lists of Field objects must be the same size.
     *    2) The corresponding fields' names must match.
     *    3) The corresponding fields' Arrow types must match.
     *    4) The corresponding fields' children lists (used for complex fields, e.g. LIST and STRUCT) must match
     *       irrespective of field ordering within the lists.
     *    5) The corresponding fields' metadata maps must match. Currently that's only applicable for scaled_float
     *       data types that use the field's metadata map to store the scaling factor associated with the data type.
     * @param list1 is a list of children fields to be compared.
     * @param list2 is a list of children fields to be compared.
     * @return true if the lists are equal, false otherwise.
     */
    private static final boolean childrenEqual(List<Field> list1, List<Field> list2)
    {
        logger.info("childrenEqual - Enter - Children1: {}, Children2: {}", list1, list2);

        // Children lists must have the same number of elements.
        if (list1.size() != list2.size()) {
            logger.warn("Children lists are different sizes - List1: {}, List2: {}", list1.size(), list2.size());
            return false;
        }

        Map<String, Field> fields = new LinkedHashMap<>();
        list2.forEach(value -> fields.put(value.getName(), value));

        // lists must have the same Fields (irrespective of internal ordering).
        for (Field field1 : list1) {
            // Corresponding fields must have the same Arrow types or the Schemas are deemed not equal.
            Field field2 = fields.get(field1.getName());
            if (field2 == null || field1.getType() != field2.getType()) {
                logger.warn("Fields' types do not match - Field1: {}, Field2: {}",
                        field1.getType(), field2 == null ? "null" : field2.getType());
                return false;
            }
            logger.info("Field1 Name: {}, Field1 Type: {}, Field1 Metadata: {}",
                    field1.getName(), field1.getType(), field1.getMetadata());
            logger.info("Field2 Name: {}, Field2 Type: {}, Field2 Metadata: {}",
                    field2.getName(), field2.getType(), field2.getMetadata());
            // The corresponding fields' children and metadata maps must also match or the Schemas are deemed not equal.
            if (!childrenEqual(field1.getChildren(), field2.getChildren()) ||
                    !field1.getMetadata().equals(field2.getMetadata())) {
                return false;
            }
        }

        return true;
    }
}
