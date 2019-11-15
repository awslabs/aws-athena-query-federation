package com.amazonaws.athena.connector.lambda.data.writers;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
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
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.codec.Charsets;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.UTC_ZONE_ID;
import static java.util.Objects.requireNonNull;

public class SimpleArrowValueWriter
        implements ArrowValueWriter
{
    private final Writer writer;
    private final FieldVector fieldVector;

    public SimpleArrowValueWriter(FieldVector fieldVector)
    {
        this.fieldVector = requireNonNull(fieldVector, "fieldVector is null");
        this.writer = createWriter(fieldVector.getMinorType());
    }

    private Writer createWriter(Types.MinorType minorType)
    {
        switch (minorType) {
            case DATEMILLI:
                return (vector, pos, value) -> {
                    DateMilliVector dateMilliVector = ((DateMilliVector) vector);
                    if (value == null) {
                        dateMilliVector.setNull(pos);
                        return;
                    }

                    if (value instanceof Date) {
                        dateMilliVector.setSafe(pos, ((Date) value).getTime());
                    }
                    else if (value instanceof LocalDateTime) {
                        dateMilliVector.setSafe(
                                pos,
                                ((LocalDateTime) value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli());
                    }
                    else {
                        dateMilliVector.setSafe(pos, (long) value);
                    }
                };
            case DATEDAY:
                return (vector, pos, value) -> {
                    DateDayVector dateDayVector = ((DateDayVector) vector);
                    if (value == null) {
                        dateDayVector.setNull(pos);
                        return;
                    }

                    if (value instanceof Date) {
                        org.joda.time.Days days = org.joda.time.Days.daysBetween(BlockUtils.EPOCH,
                                new org.joda.time.DateTime(((Date) value).getTime()));
                        dateDayVector.setSafe(pos, days.getDays());
                    }
                    if (value instanceof LocalDate) {
                        int days = (int) ((LocalDate) value).toEpochDay();
                        dateDayVector.setSafe(pos, days);
                    }
                    else if (value instanceof Long) {
                        dateDayVector.setSafe(pos, ((Long) value).intValue());
                    }
                    else {
                        dateDayVector.setSafe(pos, (int) value);
                    }
                };
            case FLOAT8:
                return (vector, pos, value) -> {
                    Float8Vector float8Vector = ((Float8Vector) vector);
                    if (value == null) {
                        float8Vector.setNull(pos);
                        return;
                    }

                    float8Vector.setSafe(pos, (double) value);
                };
            case FLOAT4:
                return (vector, pos, value) -> {
                    Float4Vector float4Vector = ((Float4Vector) vector);
                    if (value == null) {
                        float4Vector.setNull(pos);
                        return;
                    }

                    ((Float4Vector) vector).setSafe(pos, (float) value);
                };
            case INT:
                return (vector, pos, value) -> {
                    IntVector intVector = ((IntVector) vector);
                    if (value == null) {
                        intVector.setNull(pos);
                        return;
                    }

                    if (value != null && value instanceof Long) {
                        //This may seem odd at first but many frameworks (like Presto) use long as the preferred
                        //native java type for representing integers. We do this to keep type conversions simple.
                        intVector.setSafe(pos, ((Long) value).intValue());
                    }
                    else {
                        intVector.setSafe(pos, (int) value);
                    }
                };
            case TINYINT:
                return (vector, pos, value) -> {
                    TinyIntVector tinyIntVector = ((TinyIntVector) vector);
                    if (value == null) {
                        tinyIntVector.setNull(pos);
                        return;
                    }

                    if (value instanceof Byte) {
                        tinyIntVector.setSafe(pos, (byte) value);
                    }
                    else {
                        tinyIntVector.setSafe(pos, (int) value);
                    }
                };
            case SMALLINT:
                return (vector, pos, value) -> {
                    SmallIntVector smallIntVector = ((SmallIntVector) vector);
                    if (value == null) {
                        smallIntVector.setNull(pos);
                        return;
                    }

                    if (value instanceof Short) {
                        smallIntVector.setSafe(pos, (short) value);
                    }
                    else {
                        smallIntVector.setSafe(pos, (int) value);
                    }
                };
            case UINT1:
                return (vector, pos, value) -> {
                    UInt1Vector uInt1Vector = ((UInt1Vector) vector);
                    if (value == null) {
                        uInt1Vector.setNull(pos);
                        return;
                    }

                    ((UInt1Vector) vector).setSafe(pos, (int) value);
                };
            case UINT2:
                return (vector, pos, value) -> {
                    UInt2Vector uInt2Vector = ((UInt2Vector) vector);
                    if (value == null) {
                        uInt2Vector.setNull(pos);
                        return;
                    }

                    ((UInt2Vector) vector).setSafe(pos, (int) value);
                };
            case UINT4:
                return (vector, pos, value) -> {
                    UInt4Vector uInt4Vector = ((UInt4Vector) vector);
                    if (value == null) {
                        uInt4Vector.setNull(pos);
                        return;
                    }

                    ((UInt4Vector) vector).setSafe(pos, (int) value);
                };
            case UINT8:
                return (vector, pos, value) -> {
                    UInt8Vector uInt8Vector = ((UInt8Vector) vector);
                    if (value == null) {
                        uInt8Vector.setNull(pos);
                        return;
                    }

                    ((UInt8Vector) vector).setSafe(pos, (int) value);
                };
            case BIGINT:
                return (vector, pos, value) -> {
                    BigIntVector bigIntVector = ((BigIntVector) vector);
                    if (value == null) {
                        bigIntVector.setNull(pos);
                        return;
                    }

                    ((BigIntVector) vector).setSafe(pos, (long) value);
                };
            case VARBINARY:
                return (vector, pos, value) -> {
                    VarBinaryVector varBinaryVector = ((VarBinaryVector) vector);
                    if (value == null) {
                        varBinaryVector.setNull(pos);
                        return;
                    }

                    ((VarBinaryVector) vector).setSafe(pos, (byte[]) value);
                };
            case DECIMAL:
                return (vector, pos, value) -> {
                    DecimalVector dVector = ((DecimalVector) vector);
                    if (value == null) {
                        dVector.setNull(pos);
                        return;
                    }

                    if (value instanceof Double) {
                        BigDecimal bdVal = new BigDecimal((double) value);
                        bdVal = bdVal.setScale(dVector.getScale(), RoundingMode.HALF_UP);
                        dVector.setSafe(pos, bdVal);
                    }
                    else {
                        BigDecimal scaledValue = ((BigDecimal) value).setScale(dVector.getScale(), RoundingMode.HALF_UP);
                        ((DecimalVector) vector).setSafe(pos, scaledValue);
                    }
                };
            case VARCHAR:
                return (vector, pos, value) -> {
                    VarCharVector varCharVector = ((VarCharVector) vector);
                    if (value == null) {
                        varCharVector.setNull(pos);
                        return;
                    }

                    if (value instanceof String) {
                        varCharVector.setSafe(pos, ((String) value).getBytes(Charsets.UTF_8));
                    }
                    else {
                        varCharVector.setSafe(pos, (Text) value);
                    }
                };
            case BIT:
                return (vector, pos, value) -> {
                    BitVector bitVector = ((BitVector) vector);
                    if (value == null) {
                        bitVector.setNull(pos);
                        return;
                    }

                    if (value instanceof Integer && (int) value > 0) {
                        bitVector.setSafe(pos, 1);
                    }
                    else if (value instanceof Boolean && (boolean) value) {
                        bitVector.setSafe(pos, 1);
                    }
                    else {
                        bitVector.setSafe(pos, 0);
                    }
                };
            default:
                throw new IllegalArgumentException("Unknown type " + fieldVector.getMinorType());
        }
    }

    @Override
    public void write(int pos, Object value)
    {
        try {
            writer.write(fieldVector, pos, value);
        }
        catch (RuntimeException ex) {
            String fieldName = (fieldVector != null) ? fieldVector.getField().getName() : "null_vector";
            throw new RuntimeException("Unable to set value for field " + fieldName + " using value " + value, ex);
        }
    }

    interface Writer
    {
        void write(FieldVector vector, int pos, Object value);
    }
}
