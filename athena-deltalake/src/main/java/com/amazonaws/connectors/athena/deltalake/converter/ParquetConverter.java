package com.amazonaws.connectors.athena.deltalake.converter;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang.NotImplementedException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParquetConverter {

    static private class ValueHolder<T> {
        int isSet;
        T value;

        public ValueHolder(int isSet, T value) {
            this.isSet = isSet;
            this.value = value;
        }
    }

    static protected Extractor getNumberExtractor(int bitWidth, String fieldName, Optional<Object> literalValue) {
        if (bitWidth == 8 * NullableTinyIntHolder.WIDTH) return new TinyIntExtractor() {
            @Override
            public void extract(Object context, NullableTinyIntHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (byte)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = (byte)record.getInteger(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = Byte.parseByte("0");
                    }
                }
            }
        };
        else if (bitWidth == 8 * NullableSmallIntHolder.WIDTH) return new SmallIntExtractor() {
            @Override
            public void extract(Object context, NullableSmallIntHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (short)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = (short)record.getInteger(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = Short.parseShort("0");
                    }
                }
            }
        };
        else if (bitWidth == 8 * NullableIntHolder.WIDTH) return new IntExtractor() {
            @Override
            public void extract(Object context, NullableIntHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (int)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getInteger(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = 0;
                    }
                }
            }
        };
        else if (bitWidth == 8 * NullableBigIntHolder.WIDTH) return new BigIntExtractor() {
            @Override
            public void extract(Object context, NullableBigIntHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (long)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getLong(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = 0L;
                    }
                }
            }
        };
        else throw new IllegalArgumentException("Unsupported bitWidth: " + bitWidth);
    }

    static protected Extractor getFloatExtractor(FloatingPointPrecision precision, String fieldName, Optional<Object> literalValue) {
        if (precision == FloatingPointPrecision.SINGLE) return new Float4Extractor() {
            @Override
            public void extract(Object context, NullableFloat4Holder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (float)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getFloat(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = 0f;
                    }
                }
            }

        };
        else if (precision == FloatingPointPrecision.DOUBLE) return new Float8Extractor() {
            @Override
            public void extract(Object context, NullableFloat8Holder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (double)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getDouble(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = 0d;
                    }
                }
            }
        };
        else throw new IllegalArgumentException("Unsupported float precision: " + precision);
    }

    static protected Extractor getDateExtractor(DateUnit dateUnit, String fieldName, Optional<Object> literalValue) {
        if (dateUnit == DateUnit.DAY) return new DateDayExtractor() {
            @Override
            public void extract(Object context, NullableDateDayHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (int)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getInteger(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = 0;
                    }
                }
            }
        };
        else if (dateUnit == DateUnit.MILLISECOND) return new DateMilliExtractor() {
            @Override
            public void extract(Object context, NullableDateMilliHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (long)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        PrimitiveType.PrimitiveTypeName primitiveTypeName =
                                record.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
                        dst.isSet = 1;
                        if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                            dst.value = record.getLong(fieldName, 0);
                        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96) {
                            int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
                            long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
                            long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
                            byte[] bytes = record.getInt96(fieldName, 0).getBytes();
                            long timeOfDayNanos =
                                    Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
                            int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);
                            dst.value = ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
                        }
                    } else {
                        dst.isSet = 0;
                        dst.value = 0L;
                    }
                }
            }
        };
        else throw new IllegalArgumentException("Unsupported date unit: " + dateUnit);
    }

    static public Extractor getExtractor(Field field) {
        return getExtractor(field, Optional.empty());
    }

    static private <T> Function<Function<Group, T>, ValueHolder<T>> getExtractorValueHolder(Object context, String fieldName, T nullValue, Optional<Object> literalValue) {
        return (Function<Group, T> valueExtractor) -> {
            if (literalValue.isPresent()) {
                return new ValueHolder<T>(1, (T)(literalValue.get()));
            } else {
                Group record = (Group)context;
                if (record.getFieldRepetitionCount(fieldName) > 0) {
                    return new ValueHolder<T>(1, valueExtractor.apply(record));
                } else {
                    return new ValueHolder<T>(0, nullValue);
                }
            }
        };
    }

    static public Extractor getExtractor(Field field, Optional<Object> literalValue) {
        ArrowType fieldType = field.getType();
        String fieldName = field.getName();
        if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Int) return getNumberExtractor(((ArrowType.Int)fieldType).getBitWidth(), fieldName, literalValue);
        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Bool) return new BitExtractor(){
            @Override
            public void extract(Object context, NullableBitHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (byte)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getBoolean(fieldName, 0) ? 1 : 0;
                    } else {
                        dst.isSet = 0;
                        dst.value = 0;
                    }
                }
            }
        };
        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Utf8) return new VarCharExtractor(){
            @Override
            public void extract(Object context, com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (String)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getString(fieldName, 0);
                    } else {
                        dst.isSet = 0;
                        dst.value = "";
                    }
                }
            }
        };
        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Date) return getDateExtractor(((ArrowType.Date)fieldType).getUnit(), fieldName, literalValue);
        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.FloatingPoint) return getFloatExtractor(((ArrowType.FloatingPoint)fieldType).getPrecision(), fieldName, literalValue);
        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Decimal) return new DecimalExtractor() {
            @Override
            public void extract(Object context, com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (BigDecimal)literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        ArrowType.Decimal fieldDecimalType = ((ArrowType.Decimal)fieldType);
                        dst.isSet = 1;
                        dst.value = BigDecimal.valueOf(record.getLong(fieldName, 0), fieldDecimalType.getScale());
                    } else {
                        dst.isSet = 0;
                        dst.value = BigDecimal.valueOf(0);
                    }
                }
            }
        };
        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Binary) return new VarBinaryExtractor() {
            @Override
            public void extract(Object context, NullableVarBinaryHolder dst) throws Exception {
                if (literalValue.isPresent()) {
                    dst.isSet = 1;
                    dst.value = (byte[])literalValue.get();
                } else {
                    Group record = (Group)context;
                    if (record.getFieldRepetitionCount(fieldName) > 0) {
                        dst.isSet = 1;
                        dst.value = record.getBinary(fieldName, 0).getBytes();
                    } else {
                        dst.isSet = 0;
                        dst.value = new byte[]{};
                    }
                }
            }
        };

        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Null) return new Extractor() {};
        else return new Extractor() {};
    }


}
