/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.connectors.athena.deltalake.converter;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang.NotImplementedException;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.toIntExact;

/**
 * Contains util functions to convert Delta types into Arrow or Java types
 */
public class DeltaConverter
{
    private DeltaConverter() {}
    /**
     * Convert a delta schema into a Arrow schema
     * @param deltaSchemaString The Delta schema represented as a JSON string
     * @return A Arrow Schema
     * @throws JsonProcessingException
     */
    public static Schema getArrowSchema(String deltaSchemaString) throws JsonProcessingException
    {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaJson = mapper.readTree(deltaSchemaString);
        Iterator<JsonNode> fields = schemaJson.withArray("fields").elements();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        while (fields.hasNext()) {
            JsonNode field = fields.next();
            Field avroField = getAvroField(field);
            schemaBuilder.addField(avroField);
        }
        return schemaBuilder.build();
    }

    /**
     * Convert a Delta field to a Arrow Field
     * @param fieldType The Delta type of the field as a parsed JSON
     * @param fieldName The name of the field
     * @param fieldNullable If the field is nullable
     * @return A Arrow field
     */
    protected static Field getAvroField(JsonNode fieldType, String fieldName, boolean fieldNullable)
    {
        if (fieldType.isTextual()) {
            String fieldTypeName = fieldType.asText();
            switch (fieldTypeName) {
                case "integer":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.INT.getType(), null),
                            null);
                case "string":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.VARCHAR.getType(), null),
                            null);
                case "long":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.BIGINT.getType(), null),
                            null);
                case "short":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.SMALLINT.getType(), null),
                            null);
                case "byte":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.TINYINT.getType(), null),
                            null);
                case "float":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.FLOAT4.getType(), null),
                            null);
                case "double":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.FLOAT8.getType(), null),
                            null);
                case "boolean":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.BIT.getType(), null),
                            null);
                case "binary":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.VARBINARY.getType(), null),
                            null);
                case "date":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.DATEDAY.getType(), null),
                            null);
                case "timestamp":
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.DATEMILLI.getType(), null),
                            null);
                default:
                   if (fieldTypeName.startsWith("decimal(")) {
                        Pattern pattern = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");
                        Matcher matcher = pattern.matcher(fieldTypeName);
                        matcher.find();
                        int precision = Integer.parseInt(matcher.group(1));
                        int scale = Integer.parseInt(matcher.group(2));
                        return new Field(
                                fieldName,
                                new FieldType(fieldNullable, ArrowType.Decimal.createDecimal(precision, scale, null), null),
                                null);
                    }
                   else {
                       throw new UnsupportedOperationException("Field type name not supported: " + fieldTypeName);
                   }
            }
        }
        else {
            String complexTypeName = fieldType.get("type").asText();
            switch (complexTypeName) {
                case "struct":
                    Iterator<JsonNode> structFields = fieldType.withArray("fields").elements();
                    List<Field> children = new ArrayList<>();
                    while (structFields.hasNext()) {
                        JsonNode structField = structFields.next();
                        children.add(getAvroField(structField));
                    }
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.STRUCT.getType(), null),
                            children);
                case "array":
                    JsonNode elementType = fieldType.get("elementType");
                    boolean elementNullable = fieldType.get("containsNull").asBoolean();
                    String elementName = "element";
                    Field elementField = getAvroField(elementType, elementName, elementNullable);
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, Types.MinorType.LIST.getType(), null),
                            Collections.singletonList(elementField));
                case "map":
                    JsonNode keyType = fieldType.get("keyType");
                    JsonNode valueType = fieldType.get("valueType");
                    boolean valueNullable = fieldType.get("valueContainsNull").asBoolean();
                    boolean keyNullable = false;
                    Field keyField = getAvroField(keyType, "key", keyNullable);
                    Field valueField = getAvroField(valueType, "value", valueNullable);
                    Field entriesStructField = new Field(
                        "entries",
                        new FieldType(false, Types.MinorType.STRUCT.getType(), null),
                        Arrays.asList(keyField, valueField));
                    return new Field(
                            fieldName,
                            new FieldType(fieldNullable, new ArrowType.Map(true), null),
                            Arrays.asList(entriesStructField));
            }
        }
        throw new UnsupportedOperationException("Unsupported field type: " + fieldType.toString());
    }

    public static Field getAvroField(JsonNode field)
    {
        String fieldName = field.get("name").asText();
        boolean fieldNullable = field.get("nullable").asBoolean();
        JsonNode fieldType = field.get("type");
        return getAvroField(fieldType, fieldName, fieldNullable);
    }

    /**
     * Convert a Delta partition value in String format, to the type of its column
     * @param partitionValue The value of the partition as a String
     * @param arrowType The expected arrow type for the partition column
     * @return The parition value as an Object
     */
    public static Object castPartitionValue(String partitionValue, ArrowType arrowType)
    {
        if (partitionValue.isEmpty()) {
            return null;
        }
        switch (arrowType.getTypeID()) {
            case Utf8: return new Text(partitionValue);
            case Int: return Integer.parseInt(partitionValue);
            case FloatingPoint: return Float.parseFloat(partitionValue);
            case Timestamp: return Timestamp.valueOf(partitionValue);
            case Date: return toIntExact(LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay());
            case Bool: return Boolean.parseBoolean(partitionValue) ? 1 : 0;
            default: throw new NotImplementedException(String.format("Partitions of type %s are not supported", arrowType.getTypeID().name()));
        }
    }
}
