/*-
 * #%L
 * athena-deltalake
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
package com.amazonaws.connectors.athena.deltalake.converter;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.arrow.vector.types.Types.MinorType.MAP;

/***
 * Disclaimer: This code is totally rubbish, when all functionalities would be confirmed as working, than refactoring would be needed
 * cheers ;)
 */
public class ParquetFieldResolver implements FieldResolver
{
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private final int rowNum;

    public ParquetFieldResolver(int rowNum)
    {
        this.rowNum = rowNum;
    }

    @Override
    public Object getFieldValue(Field field, Object value)
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        String fieldName = field.getName();
        Group record = (Group) value;
        if (minorType == MAP && record.getType().containsField("key_value")) {
            if (record.getFieldRepetitionCount("key_value") > 0) {
                var keyValField = field.getChildren().get(0).getChildren();
                var keyField = keyValField.get(0);
                var valField = keyValField.get(1);
                return IntStream
                    .range(0, record.getFieldRepetitionCount("key_value"))
                    .mapToObj(idx -> record.getGroup("key_value", idx))
                    .collect(HashMap::new, (m, v)->m.put(getFieldValue(keyField, v), getFieldValue(valField, v)), HashMap::putAll);
            }
            else {
//                return new HashMap<>();
                return null;
            }
        }
        else if (record.getType().containsField(fieldName) && record.getFieldRepetitionCount(fieldName) > 0) {
            switch (minorType) {
                case STRUCT:
                    return record.getGroup(fieldName, rowNum);
                case MAP:
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        var mapRecord = record.getGroup(fieldName, rowNum);
                        return mapRecord.getType().containsField("key_value")
                            && mapRecord.getFieldRepetitionCount("key_value") > 0
                                ? mapRecord
                                : null;
                    }
                    else {
                        return null;
                    }
                case LIST:
                    Group list = record.getGroup(fieldName, rowNum);
                    return IntStream
                        .range(0, list.getFieldRepetitionCount(0))
                        .mapToObj(idx -> {
                            var f = field.getChildren().get(0);
                            var arrayField = new Field("element", f.getFieldType(), List.of());
                            return getFieldValue(arrayField, list.getGroup(0, idx));
                        })
                        .collect(Collectors.toList());
                case TINYINT:
                    return (byte) record.getInteger(fieldName, rowNum);
                case SMALLINT:
                    return (short) record.getInteger(fieldName, rowNum);
                case INT:
                case DATEDAY:
                    return record.getInteger(fieldName, rowNum);
                case BIGINT:
                    return record.getLong(fieldName, rowNum);
                case BIT:
                    return record.getBoolean(fieldName, rowNum);
                case FLOAT4:
                    return record.getFloat(fieldName, rowNum);
                case FLOAT8:
                    return record.getDouble(fieldName, rowNum);
                case VARCHAR:
                    return record.getString(fieldName, rowNum);
                case DATEMILLI:
                    PrimitiveType.PrimitiveTypeName primitiveTypeName =
                        record.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
                    if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                        return record.getLong(fieldName, rowNum);
                    }
                    else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96) {
                        byte[] bytes = record.getInt96(fieldName, rowNum).getBytes();
                        long timeOfDayNanos =
                            Longs.fromBytes(
                                bytes[7],
                                bytes[6],
                                bytes[5],
                                bytes[4],
                                bytes[3],
                                bytes[2],
                                bytes[1],
                                bytes[0]
                            );
                        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);
                        return ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
                    }
                    else {
                        throw new UnsupportedOperationException("Timestamp type is not handled with parquet type: " + primitiveTypeName
                            .name());
                    }
                case DECIMAL:
                case DECIMAL256:
                    ArrowType.Decimal fieldDecimalType = ((ArrowType.Decimal) field.getType());
                    PrimitiveType.PrimitiveTypeName primitiveTypeName2 =
                        record.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
                    if (primitiveTypeName2 == PrimitiveType.PrimitiveTypeName.INT64) {
                        return BigDecimal.valueOf(record.getLong(fieldName, rowNum), fieldDecimalType.getScale());
                    }
                    else if (primitiveTypeName2 == PrimitiveType.PrimitiveTypeName.INT32) {
                        return BigDecimal.valueOf(
                            record.getInteger(fieldName, rowNum),
                            fieldDecimalType.getScale()
                        );
                    }
                    else if (primitiveTypeName2 == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                        return new BigDecimal(
                            new BigInteger(record.getBinary(fieldName, rowNum).getBytes()),
                            fieldDecimalType.getScale()
                        );
                    }
                    else {
                        throw new UnsupportedOperationException(
                            "Parquet physical type used for Decimal not supported: " + primitiveTypeName2.name());
                    }
                case VARBINARY:
                    return record.getBinary(fieldName, rowNum).getBytes();
                default:
                    throw new IllegalArgumentException("Unsupported type " + minorType);
            }
        }
        return null;
    }

    @Override
    public Object getMapValue(Field field, Object value)
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        switch (minorType) {
            case MAP:
                Group record = (Group) value;
                if (record.getFieldRepetitionCount("key_value") > 0) {
                    return value;
                }
                else {
                    return null;
                }
            default:
                return value;
        }
    }
}
