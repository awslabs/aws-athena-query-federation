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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Contains util functions to convert Parquet data into Arrow format
 */
public class ParquetConverter {

    static private class ValueHolder<T> {
        int isSet;
        T value;

        public ValueHolder(int isSet, T value) {
            this.isSet = isSet;
            this.value = value;
        }
    }

    static public Extractor getExtractor(Field field) {
        return getExtractor(field, Optional.empty());
    }

    static private <T> Function<Function<Group, T>, ValueHolder<T>> getValueHolder(Object context, String fieldName, T nullValue, Optional<Object> literalValue) {
        return (Function<Group, T> valueExtractor) -> {
            if (literalValue.isPresent()) {
                return new ValueHolder<T>(1, (T)(literalValue.get()));
            } else {
                Group record = (Group)context;
                try {
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        return new ValueHolder<T>(1, valueExtractor.apply(record));
                    }
                } catch (InvalidRecordException ignored) {}
                return new ValueHolder<T>(0, nullValue);
            }
        };
    }

    /**
     * Get a valid extractor from Parquet to Arrow for the Parquet type of the field
     * @param field The field we want the extractor for
     * @param literalValue If exists, the extractor will always return this value
     * @return The extractor with the valid Arrow type
     */
    static public Extractor getExtractor(Field field, Optional<Object> literalValue) {
        ArrowType fieldType = field.getType();
        String fieldName = field.getName();
        Types.MinorType fieldMinorType = Types.getMinorTypeForArrowType(fieldType);
        switch (fieldMinorType) {
            case TINYINT:
                return (TinyIntExtractor)(Object context, NullableTinyIntHolder dst) -> {
                    ValueHolder<Byte> valueHolder = getValueHolder(context, fieldName, (byte)0, literalValue).apply(
                        (Group record) -> (byte)record.getInteger(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
            };
            case SMALLINT:
                return (SmallIntExtractor)(Object context, NullableSmallIntHolder dst) -> {
                    ValueHolder<Short> valueHolder = getValueHolder(context, fieldName, (short)0, literalValue).apply(
                        (Group record) -> (short)record.getInteger(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
            };
            case INT:
                return (IntExtractor)(Object context, NullableIntHolder dst) -> {
                    ValueHolder<Integer> valueHolder = getValueHolder(context, fieldName, 0, literalValue).apply(
                        (Group record) -> record.getInteger(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
            };
            case BIGINT:
                return (BigIntExtractor)(Object context, NullableBigIntHolder dst) -> {
                    ValueHolder<Long> valueHolder = getValueHolder(context, fieldName, 0L, literalValue).apply(
                        (Group record) -> record.getLong(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
            };
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) -> {
                    ValueHolder<Integer> valueHolder = getValueHolder(context, fieldName, 0, literalValue).apply(
                        (Group record) -> record.getBoolean(fieldName, 0) ? 1 : 0
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            case FLOAT4:
                return (Float4Extractor)(Object context, NullableFloat4Holder dst) -> {
                    ValueHolder<Float> valueHolder = getValueHolder(context, fieldName, 0f, literalValue).apply(
                        (Group record) -> record.getFloat(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            case FLOAT8:
                return (Float8Extractor)(Object context, NullableFloat8Holder dst) -> {
                    ValueHolder<Double> valueHolder = getValueHolder(context, fieldName, 0D, literalValue).apply(
                        (Group record) -> record.getDouble(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            case VARCHAR:
                Optional<Object> literalValueCasted = literalValue.map(v -> {
                    if (v instanceof Text) return v.toString();
                    else return v;
                });
                return (VarCharExtractor) (Object context, com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder dst) -> {
                    ValueHolder<String> valueHolder = getValueHolder(context, fieldName, "", literalValueCasted).apply(
                        (Group record) -> record.getString(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) -> {
                    ValueHolder<Integer> valueHolder = getValueHolder(context, fieldName, 0, literalValue).apply(
                        (Group record) -> record.getInteger(fieldName, 0)
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) -> {
                    ValueHolder<Long> valueHolder = getValueHolder(context, fieldName, 0L, literalValue).apply(
                        (Group record) -> {
                            PrimitiveType.PrimitiveTypeName primitiveTypeName =
                                    record.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
                            dst.isSet = 1;
                            if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                                return record.getLong(fieldName, 0);
                            } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96) {
                                int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
                                long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
                                long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
                                byte[] bytes = record.getInt96(fieldName, 0).getBytes();
                                long timeOfDayNanos =
                                        Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
                                int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);
                                return ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
                            } else {
                                throw new UnsupportedOperationException("Timestamp type is not handled with parquet type: " + primitiveTypeName.name());
                            }
                        }
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            case DECIMAL: case DECIMAL256:
                ArrowType.Decimal fieldDecimalType = ((ArrowType.Decimal)fieldType);
                return (DecimalExtractor) (Object context, com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder dst) -> {
                    ValueHolder<BigDecimal> valueHolder = getValueHolder(context, fieldName, BigDecimal.ZERO, literalValue).apply(
                        (Group record) -> {
                            PrimitiveType.PrimitiveTypeName primitiveTypeName =
                                    record.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
                            if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                                return BigDecimal.valueOf(record.getLong(fieldName, 0), fieldDecimalType.getScale());
                            } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
                                return BigDecimal.valueOf(record.getInteger(fieldName, 0), fieldDecimalType.getScale());
                            } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                                return new BigDecimal(new BigInteger(record.getBinary(fieldName, 0).getBytes()), fieldDecimalType.getScale());
                            } else {
                                throw new UnsupportedOperationException("Parquet physical type used for Decimal not supported: " + primitiveTypeName.name());
                            }
                        }
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) -> {
                    ValueHolder<byte[]> valueHolder = getValueHolder(context, fieldName, new byte[]{}, literalValue).apply(
                        (Group record) -> record.getBinary(fieldName, 0).getBytes()
                    );
                    dst.isSet = valueHolder.isSet;
                    dst.value = valueHolder.value;
                };
            default:
                return new Extractor() {};
        }
    }

}
