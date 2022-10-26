/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.datasource;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.pig.convert.DecimalUtils;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TypeFactory.class);
    protected TypeFactory()
    {
    }

    /**
     * Creates an instance of ValueResolver based on the provided MessageType
     *
     * @param schema An instance of MessageType
     * @return An instance of ValueResolver
     */
    public static synchronized ValueResolver valueResolver(MessageType schema)
    {
        return new ValueResolver(schema);
    }

    /**
     * Creates an instance of FieldResolver based on the provided ParquetMetadata
     *
     * @param metadata An instance of ParquetMetadata
     * @return An instance of FieldResolver
     */
    public static synchronized FieldResolver filedResolver(ParquetMetadata metadata)
    {
        return new FieldResolver(metadata);
    }

    /**
     * Creates an instance of TypedValueConverter
     *
     * @return An instance of TypedValueConverter
     */
    public static synchronized TypedValueConverter typedValueConverter()
    {
        return new TypedValueConverter();
    }

    /**
     * Value resolver resolves field values from an instance of Group
     */
    public static class ValueResolver
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(ValueResolver.class);
        private final MessageType schema;
        private final List<ColumnDescriptor> columns;

        /**
         * Constructs this instance with MessageType
         *
         * @param schema An instance of MessageType
         */
        private ValueResolver(MessageType schema)
        {
            this.schema = schema;
            this.columns = schema.getColumns();
        }

        /**
         * Reads records from the Group and converts values based on Parquet's Primitive type and Logical type as
         * applicable
         *
         * @param group An instance of Group that represents a record essential in Parquet
         * @return A map of column name and column value
         */
        public Map<String, Object> getRecord(Group group)
        {
            Map<String, Object> fieldValueMap = new HashMap<>();
            int i = 0;

            PrimitiveType primitiveType = null;
            for (; i < schema.getFieldCount(); i++) {
                ColumnDescriptor descriptor = columns.get(i);
                String fieldName = descriptor.getPath()[0];
                int fieldIndex = schema.getFieldIndex(fieldName);
                try {
                    primitiveType = descriptor.getPrimitiveType();
                    switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                        case INT64:
                        case INT96:
                            addLong(descriptor, group, fieldName, fieldIndex, fieldValueMap);
                            break;
                        case INT32:
                            addInteger(descriptor, group, fieldName, fieldIndex, fieldValueMap);
                            break;
                        case FLOAT:
                            fieldValueMap.put(fieldName, group.getFloat(fieldIndex, 0));
                            break;
                        case DOUBLE:
                            fieldValueMap.put(fieldName, group.getDouble(fieldIndex, 0));
                            break;
                        case BOOLEAN:
                            fieldValueMap.put(fieldName, group.getBoolean(fieldIndex, 0));
                            break;
                        case FIXED_LEN_BYTE_ARRAY:
                            addFixedLenByteArrayValue(descriptor, group, fieldName, fieldIndex, fieldValueMap);
                            break;
                        case BINARY:
                            addBinaryValue(descriptor, group, fieldName, fieldIndex, fieldValueMap);
                        default:
                            fieldValueMap.put(fieldName, group.getString(fieldIndex, 0));
                    }
                }
                catch (Exception exception) {
                    fieldValueMap.put(fieldName, null);
                    if (exception.getClass().equals(RuntimeException.class)
                            || exception.getClass().equals(NullPointerException.class)) {
                        continue;
                    }
                    LOGGER.error("Error resolving value for field {}, Primitive type {}, Logical type {}, Message {}", fieldName,
                            primitiveType, primitiveType != null
                                    ? primitiveType.getLogicalTypeAnnotation() != null
                                    ? primitiveType.getLogicalTypeAnnotation().toString()
                                    : null
                                    : null, exception.getMessage(), exception);
                }
            }
            return fieldValueMap;
        }

        /**
         * Add appropriate value converted from Binary. In the most cases, it's VARCHAR. However, in some cases it can
         * be DECIMAL
         *
         * @param descriptor    Description of a PARQUET field
         * @param group         An instance of Group
         * @param fieldName     Name of the field
         * @param fieldIndex    Index of the field in the schema
         * @param fieldValueMap fieldValueMap A record in the form of Map of column name and column value
         */
        protected void addBinaryValue(ColumnDescriptor descriptor, Group group, String fieldName,
                                      int fieldIndex, Map<String, Object> fieldValueMap)
        {
            // default is string when the logical type is null
            if (setStringWhenLogicalTypeIsNull(descriptor, group, fieldName,
                    fieldIndex, fieldValueMap)) {
                return;
            }

            // check the logical type if it matches to convert to DECIMAL
            LogicalTypeAnnotation typeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            if (typeAnnotation.getClass().equals(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class)) {
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation logicalType =
                        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
                BigDecimal decimal = BigDecimal.valueOf(Double.parseDouble(DecimalUtils
                        .binaryToDecimal(group.getBinary(fieldIndex, 0), logicalType.getPrecision(),
                                logicalType.getScale()).toString()));
                fieldValueMap.put(fieldName, decimal);
            }
            else {
                // default is String (VARCHAR)
                setStringAsDefault(group, fieldName,
                        fieldIndex, fieldValueMap);
            }
        }

        /**
         * Usually fixed len byte array represents a DECIMAL
         *
         * @param descriptor    Description of a PARQUET field
         * @param group         An instance of Group
         * @param fieldName     Name of the field
         * @param fieldIndex    Index of the field in the schema
         * @param fieldValueMap fieldValueMap A record in the form of Map of column name and column value
         */
        protected void addFixedLenByteArrayValue(ColumnDescriptor descriptor, Group group, String fieldName,
                                                 int fieldIndex, Map<String, Object> fieldValueMap)
        {
            if (setStringWhenLogicalTypeIsNull(descriptor, group, fieldName,
                    fieldIndex, fieldValueMap)) {
                return;
            }

            LogicalTypeAnnotation typeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            if (typeAnnotation.getClass().equals(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class)) {
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation logicalType =
                        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
                BigDecimal decimal = BigDecimal.valueOf(Double.parseDouble(DecimalUtils
                        .binaryToDecimal(group.getBinary(fieldIndex, 0),
                                logicalType.getPrecision(), logicalType.getScale()).toString()));
                fieldValueMap.put(fieldName, decimal);
            }
            else {
                setStringAsDefault(group, fieldName,
                        fieldIndex, fieldValueMap);
            }
        }

        /**
         * /**
         * Long and Int96 can either be simply long or Timestamp based on Primitive and Logical types
         *
         * @param descriptor    Columns description in Parquet
         * @param group         An instance of Group
         * @param fieldName     Name of the field
         * @param fieldIndex    Index of the field in the schema
         * @param fieldValueMap A record in the form of Map of column name and column value
         */
        protected void addLong(ColumnDescriptor descriptor, Group group, String fieldName,
                               int fieldIndex, Map<String, Object> fieldValueMap)
        {
            if (!setLongWhenLogicalTypeIsApplicable(descriptor, group, fieldName,
                    fieldIndex, fieldValueMap)) {
                setLongAsDefault(group, fieldName,
                        fieldIndex, fieldValueMap);
            }
        }

        protected void addInteger(ColumnDescriptor descriptor, Group group, String fieldName,
                                  int fieldIndex, Map<String, Object> fieldValueMap)
        {
            if (setIntWhenLogicalTypeIsNull(descriptor, group, fieldName,
                    fieldIndex, fieldValueMap)) {
                return;
            }
            LogicalTypeAnnotation typeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            if (typeAnnotation.getClass().equals(LogicalTypeAnnotation.TimeLogicalTypeAnnotation.class)) {
                fieldValueMap.put(fieldName, LocalTime.ofSecondOfDay(group.getLong(fieldIndex, 0)));
            }
            else {
                setIntAsDefault(group, fieldName,
                        fieldIndex, fieldValueMap);
            }
        }

        // helpers

        /**
         * Sets String value when the logical type is null
         *
         * @param descriptor    Description of a PARQUET field
         * @param group         An instance of Group
         * @param fieldName     Name of the field
         * @param fieldIndex    Index of the field in the schema
         * @param fieldValueMap fieldValueMap A record in the form of Map of column name and column value
         */
        private boolean setStringWhenLogicalTypeIsNull(ColumnDescriptor descriptor, Group group, String fieldName,
                                                       int fieldIndex, Map<String, Object> fieldValueMap)
        {
            if (descriptor.getPrimitiveType().getLogicalTypeAnnotation() == null) {
                fieldValueMap.put(fieldName, group.getString(fieldIndex, 0));
                return true;
            }
            return false;
        }

        private void setStringAsDefault(Group group, String fieldName,
                                        int fieldIndex, Map<String, Object> fieldValueMap)
        {
            fieldValueMap.put(fieldName, group.getString(fieldIndex, 0));
        }

        private void setLongAsDefault(Group group, String fieldName,
                                      int fieldIndex, Map<String, Object> fieldValueMap)
        {
            fieldValueMap.put(fieldName, group.getLong(fieldIndex, 0));
        }

        private void setIntAsDefault(Group group, String fieldName,
                                     int fieldIndex, Map<String, Object> fieldValueMap)
        {
            fieldValueMap.put(fieldName, group.getInteger(fieldIndex, 0));
        }

        /**
         * Sets field value based on underlying logical type if found as per following conditions:
         * <ol>
         *     <li>When the logical type is Time, the field is converted to VARCHAR, regardless of type being Time(millis) or Time(nanos)</li>
         *     <li>When the logical type is Timestamp, the field is converted to Timestamp</li>
         *     <li>When the logical type's bit width is less than 64, in the case of Int type, the field is converted to Integer</li>
         *     <li>When the logical type's bit width is greater than 32, in the case of Int type, the field is converted to Integer</li>
         * </ol>
         *
         * @param descriptor    Description of a PARQUET field
         * @param group         An instance of Group
         * @param fieldName     Name of the field
         * @param fieldIndex    Index of the field in the schema
         * @param fieldValueMap fieldValueMap A record in the form of Map of column name and column value
         * @return True if field is set as per logical type annotation when resent, false otherwise
         */
        private boolean setLongWhenLogicalTypeIsApplicable(ColumnDescriptor descriptor, Group group, String fieldName,
                                                           int fieldIndex, Map<String, Object> fieldValueMap)
        {
            LogicalTypeAnnotation typeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            if (typeAnnotation != null) {
                if (typeAnnotation.getClass().equals(LogicalTypeAnnotation.TimeLogicalTypeAnnotation.class)) {
                    fieldValueMap.put(fieldName, LocalTime.ofNanoOfDay(group.getLong(fieldIndex, 0)));
                }
                else if (typeAnnotation.getClass().equals(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.class)) {
                    fieldValueMap.put(fieldName, new Timestamp(group.getLong(fieldIndex, 0)));
                }
                else if (typeAnnotation.getClass().equals(LogicalTypeAnnotation.IntLogicalTypeAnnotation.class)) {
                    LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalTypeAnnotation =
                            (LogicalTypeAnnotation.IntLogicalTypeAnnotation) typeAnnotation;
                    if (intLogicalTypeAnnotation.getBitWidth() < 64) {
                        fieldValueMap.put(fieldName, group.getInteger(fieldIndex, 0));
                    }
                    else {
                        fieldValueMap.put(fieldName, group.getLong(fieldIndex, 0));
                    }
                }
                return true;
            }
            return false;
        }

        private boolean setIntWhenLogicalTypeIsNull(ColumnDescriptor descriptor, Group group, String fieldName,
                                                    int fieldIndex, Map<String, Object> fieldValueMap)
        {
            if (descriptor.getPrimitiveType().getLogicalTypeAnnotation() == null) {
                fieldValueMap.put(fieldName, group.getInteger(fieldIndex, 0));
                return true;
            }
            return false;
        }
    }

    /**
     * Helper class to resolve Arrow vector field from Parquet field descriptor
     */
    public static class FieldResolver
    {
        private final ParquetMetadata metadata;
        /**
         * List of Arrow vector field
         */
        private final ImmutableList.Builder<Field> fieldListBuilder = ImmutableList.builder();

        private FieldResolver(ParquetMetadata metadata)
        {
            this.metadata = metadata;
        }

        /**
         * Resolves a list of Arrow vector fields from Parquet's list of column descriptor
         *
         * @return A list of Field
         */
        public List<Field> resolveFields()
        {
            List<ColumnDescriptor> descriptors = metadata.getFileMetaData().getSchema().getColumns();
            StringBuilder sb = new StringBuilder();
            for (ColumnDescriptor descriptor : descriptors) {
                sb.setLength(0);
                String[] p = descriptor.getPath();
                for (String px : p) {
                    sb.append(px);
                }
                String fieldName = sb.toString();
                PrimitiveType primitiveType = descriptor.getPrimitiveType();
                switch (primitiveType.getPrimitiveTypeName()) {
                    case BOOLEAN:
                        fieldListBuilder.add(new Field(sb.toString(), FieldType.nullable(new ArrowType.Bool()),
                                null));
                        break;
                    case DOUBLE:
                        fieldListBuilder.add(new Field(sb.toString(),
                                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                        break;
                    case FIXED_LEN_BYTE_ARRAY:
                        addFixedLenByteArrayField(primitiveType, fieldName);
                        break;
                    case FLOAT:
                        fieldListBuilder.add(new Field(sb.toString(),
                                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
                        break;
                    case INT32:
                        addIntField(primitiveType, fieldName, 32, true);
                        break;
                    case INT64:
                        addIntField(primitiveType, fieldName, 64, true);
                        break;
                    case INT96:
                        addTimeStampField(fieldName);
                        break;
                    case BINARY:
                        FieldType binaryFieldType = new FieldType(true, new ArrowType.Utf8(), null,
                                Map.of("baseType", "BINARY"));
                        fieldListBuilder.add(new Field(sb.toString(),
                                binaryFieldType, null));
                        break;
                    default:
                        fieldListBuilder.add(new Field(sb.toString(),
                                FieldType.nullable(new ArrowType.Utf8()), null));
                        break;
                }
            }
            return fieldListBuilder.build();
        }

        // helpers
        /**
         * Adds in int or long field based on Primitive type
         *
         * @param primitiveType An instance of Primitive type from the Parquet column
         * @param fieldName     Name of the field
         * @param bitWidth      A bit width of the integer. 32 bit indicates an Integer, 64 bit to Long
         * @param singed        Whether the integer is signed or not (Int of UInt in some math jargon)
         */
        private void addIntField(PrimitiveType primitiveType, String fieldName,
                                 int bitWidth, boolean singed)
        {
            if (addIntFieldIfLogicalTypeIsNull(primitiveType, fieldName, bitWidth, singed)) {
                return;
            }

            if (primitiveType.getLogicalTypeAnnotation().getClass()
                    .equals(LogicalTypeAnnotation.DateLogicalTypeAnnotation.class)) {
                fieldListBuilder.add(new Field(fieldName,
                        FieldType.nullable(Types.MinorType.DATEDAY.getType()), null));
            }
            else if (primitiveType.getLogicalTypeAnnotation().getClass()
                    .equals(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.class)) {
                addTimeStampField(fieldName);
            }
            else if (primitiveType.getLogicalTypeAnnotation().getClass()
                    .equals(LogicalTypeAnnotation.TimeLogicalTypeAnnotation.class)) {
                addVarcharFieldAsDefault(fieldName);
            }
            else {
                fieldListBuilder.add(new Field(fieldName,
                        FieldType.nullable(new ArrowType.Int(bitWidth, singed)), null));
            }
        }

        /**
         * Add timestamp field
         *
         * @param fieldName Name of the field
         */
        private void addTimeStampField(String fieldName)
        {
            fieldListBuilder.add(new Field(fieldName,
                    FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null));
        }

        /**
         * Add a field of type fixed length byte array. Usually used for DECIMAL
         *
         * @param primitiveType An instance of Primitive type from the Parquet column
         * @param fieldName     Name of the field
         */
        private void addFixedLenByteArrayField(PrimitiveType primitiveType, String fieldName)
        {
            if (primitiveType.getLogicalTypeAnnotation() == null) {
                addVarcharFieldAsDefault(fieldName);
                return;
            }

            LogicalTypeAnnotation typeAnnotation = primitiveType.getLogicalTypeAnnotation();
            if (typeAnnotation.getClass().equals(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class)) {
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType
                        = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation();
                fieldListBuilder.add(new Field(fieldName,
                        FieldType.nullable(new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(),
                                decimalType.getPrecision() > 38 ? 256 : 128)), null));
            }
            else {
                addVarcharFieldAsDefault(fieldName);
            }
        }

        /**
         * Adds a VARCHAR field
         *
         * @param fieldName Name of the field
         */
        private void addVarcharFieldAsDefault(String fieldName)
        {
            fieldListBuilder.add(new Field(fieldName,
                    FieldType.nullable(new ArrowType.Utf8()), null));
        }

        /**
         * Add just an Int field when the logical type is null, when the logical type is null, does nothing otherwise
         *
         * @param primitiveType An instance of Primitive type from the Parquet column
         * @param fieldName     Name of the field
         * @param bitWidth      A bit width of the integer. 32 bit indicates an Integer, 64 bit to Long
         * @param singed        Whether the integer is signed or not (Int of UInt in some math jargon)
         * @return True if it adds an integer in case the logical type annotation is null, false otherwise
         */
        private boolean addIntFieldIfLogicalTypeIsNull(PrimitiveType primitiveType, String fieldName,
                                                       int bitWidth, boolean singed)
        {
            if (primitiveType.getLogicalTypeAnnotation() == null) {
                fieldListBuilder.add(new Field(fieldName,
                        FieldType.nullable(new ArrowType.Int(bitWidth, singed)), null));
                return true;
            }
            return false;
        }
    }

    /**
     * Typed value resolver resolves field valued during assembly when a Parquet record is read.
     * It is being called before applying filter, which is a part of custom filter push down. Before comparing a field
     * value against a parquet expression, the value needs to be converted appropriately, and TypedValueConverter does
     * this task
     */
    public static class TypedValueConverter
    {
        /**
         * Converts the supplied value based on the supplied type
         *
         * @param type  Type of the value during assembly
         * @param value Value of the field
         * @return Converted when applicable, intact value when it's not
         */
        public Object convert(Type type, Object value)
        {
            if (value == null) {
                return null;
            }

            OriginalType originalType = type.getOriginalType();
            try {
                switch (originalType) {
                    case UTF8:
                        return ((Binary) value).toStringUsingUTF8();
                    case DECIMAL:
                        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation logicalType
                                = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
                        return Double.parseDouble(DecimalUtils.binaryToDecimal((Binary) value,
                                logicalType.getPrecision(), logicalType.getScale()).toString());
                    case DATE:
                    case TIME_MILLIS:
                    case TIMESTAMP_MILLIS:
                    case UINT_8:
                    case UINT_16:
                    case UINT_32:
                    case INT_8:
                    case INT_16:
                    case INT_32:
                    case UINT_64:
                    case INT_64:
                    case TIME_MICROS:
                    case TIMESTAMP_MICROS:
                    case INTERVAL:
                        return value;
                    case MAP_KEY_VALUE:
                    case ENUM:
                    case MAP:
                    case LIST:
                    case JSON:
                    case BSON:
                    default:
                        if (value instanceof Binary) {
                            return ((Binary) value).toStringUsingUTF8();
                        }
                        return value;
                }
            }
            catch (Exception exception) {
                LOGGER.error("Unable to convert value {} for type {}", value, type, exception);
            }
            return value;
        }
    }
}
