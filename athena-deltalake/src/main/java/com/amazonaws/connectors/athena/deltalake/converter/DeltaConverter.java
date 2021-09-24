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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DeltaConverter {

    static public Schema getArrowSchema(String deltaSchemaString) throws JsonProcessingException {
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

    static protected Field getAvroField(JsonNode fieldType, String fieldName, boolean fieldNullable) {
        if (fieldType.isTextual()) {
            String fieldTypeName = fieldType.asText();
            if(fieldTypeName.equals("integer")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.INT.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("string")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.VARCHAR.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("long")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.BIGINT.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("short")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.SMALLINT.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("byte")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.TINYINT.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("float")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.FLOAT4.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("double")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.FLOAT8.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("boolean")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.BIT.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("binary")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.VARBINARY.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("date")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.DATEDAY.getType(), null),
                        null);
            }
            if(fieldTypeName.equals("timestamp")) {
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.DATEMILLI.getType(), null),
                        null);
            }
            if(fieldTypeName.startsWith("decimal(")) {
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
        } else {
            String complexTypeName = fieldType.get("type").asText();
            if (complexTypeName.equals("struct")) {
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
            } else if (complexTypeName.equals("array")){
                JsonNode elementType = fieldType.get("elementType");
                boolean elementNullable = fieldType.get("containsNull").asBoolean();
                String elementName = fieldName + ".element";
                Field elementField = getAvroField(elementType, elementName, elementNullable);
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, Types.MinorType.LIST.getType(), null),
                        Collections.singletonList(elementField));
            } else if (complexTypeName.equals("map")){
                JsonNode keyType = fieldType.get("keyType");
                JsonNode valueType = fieldType.get("valueType");
                boolean valueNullable = fieldType.get("valueContainsNull").asBoolean();
                boolean keyNullable = false;
                String keyName = fieldName + ".key";
                String valueName = fieldName + ".value";
                Field keyField = getAvroField(keyType, keyName, keyNullable);
                Field valueField = getAvroField(valueType, valueName, valueNullable);
                return new Field(
                        fieldName,
                        new FieldType(fieldNullable, new ArrowType.Map(true), null),
                        Arrays.asList(keyField, valueField));
            }
        }
        throw new UnsupportedOperationException("Unsupported field type: " + fieldType.toString());
    }

    static public Field getAvroField(JsonNode field) {
        String fieldName = field.get("name").asText();
        boolean fieldNullable = field.get("nullable").asBoolean();
        JsonNode fieldType = field.get("type");
        return getAvroField(fieldType, fieldName, fieldNullable);
    }

    public static Object castPartitionValue(String partitionValue, ArrowType arrowType) {
        if (partitionValue.isEmpty()) return null;
        if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Utf8) return new Text(partitionValue);
        if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Int) return Integer.parseInt(partitionValue);
        if (arrowType.getTypeID() == ArrowType.ArrowTypeID.FloatingPoint) return Float.parseFloat(partitionValue);
        if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Timestamp) return Timestamp.valueOf(partitionValue);
        if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Date) return LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE);
        if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Bool) return Boolean.valueOf(partitionValue);
        throw new NotImplementedException(String.format("Partitions of type %s are not supported", arrowType.getTypeID().name()));
    }

}
