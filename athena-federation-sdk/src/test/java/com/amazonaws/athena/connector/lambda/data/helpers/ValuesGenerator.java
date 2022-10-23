/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.data.helpers;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMicroTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecTZHolder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import net.jqwik.api.*;
import net.jqwik.time.api.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

public class ValuesGenerator {

    private static final Long MIN_TIME = Instant.ofEpochMilli(Long.MIN_VALUE).toEpochMilli();
    private static final Long MAX_TIME = Instant.ofEpochMilli(Long.MAX_VALUE).toEpochMilli();

    public FieldVector generateValues(Field field, FieldVector vector, CustomFieldVector customVector) {
        generateValues(field, vector, customVector, 0, false);
        return vector;
    }

    public int generateValues(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        switch (vector.getMinorType()) {
            case BIGINT:
                return setBigInt(field, vector, customVector, length, unique);
            case BIT:
                return setBit(field, vector, customVector, length, unique);
            case DATEDAY:
                return setDateDay(field, vector, customVector, length, unique);
            case DATEMILLI:
                return setDateMilli(field, vector, customVector, length, unique);
            case DECIMAL:
                return setDecimal(field, vector, customVector, length, unique);
            case FLOAT4:
                return setFloat4(field, vector, customVector, length, unique);
            case FLOAT8:
                return setFloat8(field, vector, customVector, length, unique);
            case INT:
                return setInt(field, vector, customVector, length, unique);
            case LIST:
                return setList(field, vector, customVector, length, unique);
            case MAP:
                return setMap(field, vector, customVector, length, unique);
            case SMALLINT:
                return setSmallInt(field, vector, customVector, length, unique);
            case STRUCT:
                return setStruct(field, vector, customVector, length, unique);
            case TIMESTAMPMICROTZ:
                return setTimestampMicroTz(field, vector, customVector, length, unique);
            case TIMESTAMPMILLITZ:
                return setTimestampMilliTz(field, vector, customVector, length, unique);
            case TIMESTAMPNANOTZ:
                return setTimestampNanoTz(field, vector, customVector, length, unique);
            case TIMESTAMPSECTZ:
                return setTimestampSecTz(field, vector, customVector, length, unique);
            case TINYINT:
                return setTinyInt(field, vector, customVector, length, unique);
            case UINT1:
                return setUint1(field, vector, customVector, length, unique);
            case UINT2:
                return setUint2(field, vector, customVector, length, unique);
            case UINT4:
                return setUint4(field, vector, customVector, length, unique);
            case UINT8:
                return setUint8(field, vector, customVector, length, unique);
            case VARBINARY:
                return setVarBinary(field, vector, customVector, length, unique);
            case VARCHAR:
                return setVarChar(field, vector, customVector, length, unique);
            default:
                throw new RuntimeException("Not yet implemented for typeId " + vector.getMinorType());
        }
    }

    private int getLength(int length) {
        if (length == 0) {
            length = Arbitraries.integers().between(1, 5).sample();
        }
        return length;
    }

    private boolean isNull(Field field) {
        if (!field.isNullable()) {
            return false;
        }

        // randomly determine if the value should be null or not
        return Arbitraries.integers().between(0, 5).sample() == 0 ? true : false;
    }

    /*
     * Complex Types
     */

    private int setStruct(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();
        int maxChildSize = 0;

        for (int i = 0; i < vector.getChildrenFromFields().size(); i++) {
            Field childField = field.getChildren().get(i);
            FieldVector childVector = vector.getChildrenFromFields().get(i);
            CustomFieldVector childCustomVector = new CustomFieldVector(childField);
            maxChildSize = Math.max(generateValues(childField, childVector, childCustomVector, length, false), maxChildSize);
            customVector.add(childCustomVector);
        }

        for (int i = position; i < maxChildSize; i++) {
            ((StructVector) vector).setIndexDefined(i);
        }
        vector.setValueCount(maxChildSize);
        return vector.getValueCount();
    }

    private int setList(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        assertThat(vector.getChildrenFromFields().size()).isEqualTo(1);

        int position = vector.getValueCount();
        Field childField = field.getChildren().get(0);
        FieldVector childVector = vector.getChildrenFromFields().get(0);
        length = getLength(length);

        int prevChildSize = childVector.getValueCount();
        for (int i = 0; i < length; i++) {
            CustomFieldVector childCustomVector = null;
            if (!isNull(field)) {
                childCustomVector = new CustomFieldVector(childField);
                ((ListVector) vector).startNewValue(position + i);
                int newChildSize = generateValues(childField, childVector, childCustomVector, 0, false);
                ((ListVector) vector).endValue(position + i, newChildSize - prevChildSize);
                prevChildSize = newChildSize;
            }
            customVector.add(childCustomVector);
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setMap(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        assertThat(vector.getChildrenFromFields().size()).isEqualTo(1);

        int position = vector.getValueCount();

        Field entriesField = field.getChildren().get(0);
        FieldVector entries = vector.getChildrenFromFields().get(0);

        assertThat(entries.getChildrenFromFields().size()).isEqualTo(2);
        Field keysField = entriesField.getChildren().get(0);
        FieldVector keys = entries.getChildrenFromFields().get(0);
        Field valuesField = entriesField.getChildren().get(1);
        FieldVector values = entries.getChildrenFromFields().get(1);

        length = getLength(length);

        for (int i = 0; i < length; i++) {
            ((MapVector) vector).startNewValue(position+i);
            int keysValuesLength = getLength(0);
            if (keys.getMinorType().equals(MinorType.VARBINARY) || keys.getMinorType().equals(MinorType.BIT)) {
                keysValuesLength = 2; // special case if key is of binary/bit type
            }

            CustomFieldVector keysCustomVector = new CustomFieldVector(keysField);
            CustomFieldVector valuesCustomVector = new CustomFieldVector(valuesField);
            generateValues(keysField, keys, keysCustomVector, keysValuesLength, true);
            generateValues(valuesField, values, valuesCustomVector, keysValuesLength, false);
            customVector.addMap(keysCustomVector, valuesCustomVector);
            ((MapVector) vector).endValue(position+i, keysValuesLength);
        }
        vector.setValueCount(position+length);

        for (int i = 0; i < keys.getValueCount(); i++) {
            ((StructVector) entries).setIndexDefined(i);
        }
        entries.setValueCount(keys.getValueCount());
        return vector.getValueCount();
    }

    /*
     * Primitive Types
     */

    private int setBigInt(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Long> items = unique ?
            Arbitraries.longs().list().ofSize(length).uniqueElements().sample() :
            Arbitraries.longs().list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((BigIntVector) vector).setSafe(i+position, new NullableBigIntHolder());
            } else {
                ((BigIntVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setBit(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Integer> items = unique ?
            Arbitraries.integers().between(0, 1).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.integers().between(0, 1).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((BitVector) vector).setSafe(i+position, new NullableBitHolder());
            }
            else {
                ((BitVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setDateDay(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<LocalDate> items = unique ?
            Dates.dates().list().ofSize(length).uniqueElements().sample() :
            Dates.dates().list().ofSize(length).sample();


        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((DateDayVector) vector).setSafe(i+position, new NullableDateDayHolder());
            }
            else {
                ((DateDayVector) vector).setSafe(i+position, (int) items.get(i).toEpochDay());
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setDateMilli(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);

        List<Long> items = unique ?
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)){
                ((DateMilliVector) vector).setSafe(i+position, new NullableDateMilliHolder());
            }
            else {
                ((DateMilliVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setDecimal(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();
        length = getLength(length);
        int scale = ((DecimalVector) vector).getScale();
        int precision = ((DecimalVector) vector).getPrecision();
        double range = Math.pow(10, (precision-scale));

        List<BigDecimal> items = unique ?
            Arbitraries.bigDecimals().greaterThan(new BigDecimal(range*-1)).lessThan(new BigDecimal(range)).ofScale(scale)
                .list().ofSize(length).uniqueElements().sample() :
            Arbitraries.bigDecimals().greaterThan(new BigDecimal(range*-1)).lessThan(new BigDecimal(range)).ofScale(scale)
                .list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((DecimalVector) vector).setSafe(i+position, new NullableDecimalHolder());
            } else {
                ((DecimalVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setFloat4(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Float> items = unique ?
            Arbitraries.floats().list().ofSize(length).uniqueElements().sample() :
            Arbitraries.floats().list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((Float4Vector) vector).setSafe(i+position, new NullableFloat4Holder());
            } else {
                ((Float4Vector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setFloat8(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Float> items = unique ?
            Arbitraries.floats().list().ofSize(length).uniqueElements().sample() :
            Arbitraries.floats().list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((Float8Vector) vector).setSafe(i+position, new NullableFloat8Holder());
            } else {
                ((Float8Vector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    protected int setInt(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Integer> items = unique ?
            Arbitraries.integers().list().ofSize(length).uniqueElements().sample() :
            Arbitraries.integers().list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((IntVector) vector).setSafe(i+position, new NullableIntHolder());
            }
            else {
                ((IntVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setSmallInt(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Short> items = unique ?
            Arbitraries.shorts().list().ofSize(length).uniqueElements().sample() :
            Arbitraries.shorts().list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((SmallIntVector) vector).setSafe(i+position, new NullableSmallIntHolder());
            }
            else {
                ((SmallIntVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setTimestampMicroTz(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Long> items = unique ?
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((TimeStampMicroTZVector) vector).setSafe(i+position, new NullableTimeStampMicroTZHolder());
            } else {
                ((TimeStampMicroTZVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setTimestampMilliTz(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Long> items = unique ?
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((TimeStampMilliTZVector) vector).setSafe(i+position, new NullableTimeStampMilliTZHolder());
            } else {
                ((TimeStampMilliTZVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setTimestampNanoTz(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Long> items = unique ?
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((TimeStampNanoTZVector) vector).setSafe(i+position, new NullableTimeStampNanoTZHolder());
            } else {
                ((TimeStampNanoTZVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setTimestampSecTz(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Long> items = unique ?
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.longs().greaterOrEqual(MIN_TIME).lessOrEqual(MAX_TIME).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((TimeStampSecTZVector) vector).setSafe(i+position, new NullableTimeStampSecTZHolder());
            } else {
                ((TimeStampSecTZVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setTinyInt(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Byte> items = unique ?
            Arbitraries.bytes().list().ofSize(length).uniqueElements().sample() :
            Arbitraries.bytes().list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((TinyIntVector) vector).setSafe(i+position, new NullableTinyIntHolder());
            }
            else {
                ((TinyIntVector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setVarChar(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<String> items = unique ?
            Arbitraries.strings().ofMinLength(1).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.strings().ofMinLength(1).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((VarCharVector) vector).setSafe(i+position, new NullableVarCharHolder());
            }
            else {
                ((VarCharVector) vector).setSafe(i+position, items.get(i).getBytes(StandardCharsets.UTF_8));
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setUint1(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Byte> items = unique ?
            Arbitraries.bytes().greaterOrEqual((byte) 0).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.bytes().greaterOrEqual((byte) 0).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((UInt1Vector) vector).setSafe(i+position, new NullableUInt1Holder());
            } else {
                ((UInt1Vector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setUint2(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Short> items = unique ?
            Arbitraries.shorts().greaterOrEqual((short) 0).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.shorts().greaterOrEqual((short) 0).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((UInt2Vector) vector).setSafe(i+position, new NullableUInt2Holder());
            } else {
                ((UInt2Vector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setUint4(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Integer> items = unique ?
            Arbitraries.integers().greaterOrEqual(0).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.integers().greaterOrEqual(0).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((UInt4Vector) vector).setSafe(i+position, new NullableUInt4Holder());
            } else {
                ((UInt4Vector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setUint8(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<Long> items = unique ?
            Arbitraries.longs().greaterOrEqual(0).list().ofSize(length).uniqueElements().sample() :
            Arbitraries.longs().greaterOrEqual(0).list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((UInt8Vector) vector).setSafe(i+position, new NullableUInt8Holder());
            } else {
                ((UInt8Vector) vector).setSafe(i+position, items.get(i));
            }
            customVector.add(vector.getObject(i+position));
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setVarBinary(Field field, FieldVector vector, CustomFieldVector customVector, int length, boolean unique) {
        int position = vector.getValueCount();

        length = getLength(length);
        List<String> items = unique ?
            Arbitraries.strings().list().ofSize(length).uniqueElements().sample() :
            Arbitraries.strings().list().ofSize(length).sample();

        for (int i = 0; i < length; i++) {
            if (isNull(field)) {
                ((VarBinaryVector) vector).setSafe(i+position, new NullableVarBinaryHolder());
            }
            else {
                ((VarBinaryVector) vector).setSafe(i+position, items.get(i).getBytes());
            }
            customVector.add(vector.getObject(i+position));
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }
}
