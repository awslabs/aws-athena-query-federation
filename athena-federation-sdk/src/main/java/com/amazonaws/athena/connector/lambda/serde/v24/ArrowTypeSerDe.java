/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.serde.DelegatingSerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;

public class ArrowTypeSerDe extends DelegatingSerDe<ArrowType>
{
    public ArrowTypeSerDe()
    {
        super(ImmutableSet.<TypedSerDe<ArrowType>>builder()
                .add(new NullSerDe())
                .add(new StructSerDe())
                .add(new ListSerDe())
                .add(new FixedSizeListSerDe())
                .add(new UnionSerDe())
                .add(new IntSerDe())
                .add(new FloatingPointSerDe())
                .add(new Utf8SerDe())
                .add(new BinarySerDe())
                .add(new FixedSizeBinarySerDe())
                .add(new BoolSerDe())
                .add(new DecimalSerDe())
                .add(new DateSerDe())
                .add(new TimeSerDe())
                .add(new TimestampSerDe())
                .add(new IntervalSerDe())
                .build());
    }

    private static class NullSerDe extends TypedSerDe<ArrowType>
    {
        private NullSerDe()
        {
            super(ArrowType.Null.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            // no fields
        }

        @Override
        public ArrowType.Null doDeserialize(JsonParser jparser)
                throws IOException
        {
            // no fields
            return new ArrowType.Null();
        }
    }

    private static class StructSerDe extends TypedSerDe<ArrowType>
    {
        private StructSerDe()
        {
            super(ArrowType.Struct.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            // no fields
        }

        @Override
        public ArrowType.Struct doDeserialize(JsonParser jparser)
                throws IOException
        {
            // no fields
            return new ArrowType.Struct();
        }
    }

    private static class ListSerDe extends TypedSerDe<ArrowType>
    {
        private ListSerDe()
        {
            super(ArrowType.List.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            // no fields
        }

        @Override
        public ArrowType.List doDeserialize(JsonParser jparser)
                throws IOException
        {
            // no fields
            return new ArrowType.List();
        }
    }

    private static class FixedSizeListSerDe extends TypedSerDe<ArrowType>
    {
        private static final String LIST_SIZE_FIELD = "listSize";

        private FixedSizeListSerDe()
        {
            super(ArrowType.FixedSizeList.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.FixedSizeList fixedSizeList = (ArrowType.FixedSizeList) arrowType;
            jgen.writeNumberField(LIST_SIZE_FIELD, fixedSizeList.getListSize());
        }

        @Override
        public ArrowType.FixedSizeList doDeserialize(JsonParser jparser)
                throws IOException
        {
            int listSize = getNextIntField(jparser, LIST_SIZE_FIELD);
            return new ArrowType.FixedSizeList(listSize);
        }
    }

    private static class UnionSerDe extends TypedSerDe<ArrowType>
    {
        private static final String MODE_FIELD = "mode";
        private static final String TYPE_IDS_FIELD = "typeIds";

        private UnionSerDe()
        {
            super(ArrowType.Union.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.Union union = (ArrowType.Union) arrowType;
            jgen.writeStringField(MODE_FIELD, union.toString());
            jgen.writeArrayFieldStart(TYPE_IDS_FIELD);
            for (int typeId : union.getTypeIds()) {
                jgen.writeNumber(typeId);
            }
            jgen.writeEndArray();
        }

        @Override
        public ArrowType.Union doDeserialize(JsonParser jparser)
                throws IOException
        {
            UnionMode mode = UnionMode.valueOf(getNextStringField(jparser, MODE_FIELD));
            assertFieldName(jparser, TYPE_IDS_FIELD);
            ImmutableList.Builder<Integer> typeIds = ImmutableList.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                typeIds.add(jparser.getValueAsInt());
            }
            return new ArrowType.Union(mode, Ints.toArray(typeIds.build()));
        }
    }

    private static class IntSerDe extends TypedSerDe<ArrowType>
    {
        private static final String BIT_WIDTH_FIELD = "bitWidth";
        private static final String IS_SIGNED_FIELD = "isSigned";

        private IntSerDe()
        {
            super(ArrowType.Int.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.Int arrowInt = (ArrowType.Int) arrowType;
            jgen.writeNumberField(BIT_WIDTH_FIELD, arrowInt.getBitWidth());
            jgen.writeBooleanField(IS_SIGNED_FIELD, arrowInt.getIsSigned());
        }

        @Override
        public ArrowType.Int doDeserialize(JsonParser jparser)
                throws IOException
        {
            int bitWidth = getNextIntField(jparser, BIT_WIDTH_FIELD);
            boolean isSigned = getNextBoolField(jparser, IS_SIGNED_FIELD);
            return new ArrowType.Int(bitWidth, isSigned);
        }
    }

    private static class FloatingPointSerDe extends TypedSerDe<ArrowType>
    {
        private static final String PRECISION_FIELD = "precision";

        private FloatingPointSerDe()
        {
            super(ArrowType.FloatingPoint.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) arrowType;
            jgen.writeStringField(PRECISION_FIELD, floatingPoint.getPrecision().toString());
        }

        @Override
        public ArrowType.FloatingPoint doDeserialize(JsonParser jparser)
                throws IOException
        {
            FloatingPointPrecision precision = FloatingPointPrecision.valueOf(getNextStringField(jparser, PRECISION_FIELD));
            return new ArrowType.FloatingPoint(precision);
        }
    }

    private static class Utf8SerDe extends TypedSerDe<ArrowType>
    {
        private Utf8SerDe()
        {
            super(ArrowType.Utf8.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            // no fields
        }

        @Override
        public ArrowType.Utf8 doDeserialize(JsonParser jparser)
                throws IOException
        {
            // no fields
            return new ArrowType.Utf8();
        }
    }

    private static class BinarySerDe extends TypedSerDe<ArrowType>
    {
        private BinarySerDe()
        {
            super(ArrowType.Binary.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            // no fields
        }

        @Override
        public ArrowType.Binary doDeserialize(JsonParser jparser)
                throws IOException
        {
            // no fields
            return new ArrowType.Binary();
        }
    }

    private static class FixedSizeBinarySerDe extends TypedSerDe<ArrowType>
    {
        private static final String BYTE_WIDTH_FIELD = "byteWidth";

        private FixedSizeBinarySerDe()
        {
            super(ArrowType.FixedSizeBinary.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) arrowType;
            jgen.writeNumberField(BYTE_WIDTH_FIELD, fixedSizeBinary.getByteWidth());
        }

        @Override
        public ArrowType.FixedSizeBinary doDeserialize(JsonParser jparser)
                throws IOException
        {
            int byteWidth = getNextIntField(jparser, BYTE_WIDTH_FIELD);
            return new ArrowType.FixedSizeBinary(byteWidth);
        }
    }

    private static class BoolSerDe extends TypedSerDe<ArrowType>
    {
        private BoolSerDe()
        {
            super(ArrowType.Bool.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            // no fields
        }

        @Override
        public ArrowType.Bool doDeserialize(JsonParser jparser)
                throws IOException
        {
            // no fields
            return new ArrowType.Bool();
        }
    }

    private static class DecimalSerDe extends TypedSerDe<ArrowType>
    {
        private static final String PRECISION_FIELD = "precision";
        private static final String SCALE_FIELD = "scale";

        private DecimalSerDe()
        {
            super(ArrowType.Decimal.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
            jgen.writeNumberField(PRECISION_FIELD, decimal.getPrecision());
            jgen.writeNumberField(SCALE_FIELD, decimal.getScale());
        }

        @Override
        public ArrowType.Decimal doDeserialize(JsonParser jparser)
                throws IOException
        {
            int precision = getNextIntField(jparser, PRECISION_FIELD);
            int scale = getNextIntField(jparser, SCALE_FIELD);
            return new ArrowType.Decimal(precision, scale);
        }
    }

    private static class DateSerDe extends TypedSerDe<ArrowType>
    {
        private static final String UNIT_FIELD = "unit";

        private DateSerDe()
        {
            super(ArrowType.Date.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.Date date = (ArrowType.Date) arrowType;
            jgen.writeStringField(UNIT_FIELD, date.getUnit().toString());
        }

        @Override
        public ArrowType.Date doDeserialize(JsonParser jparser)
                throws IOException
        {
            DateUnit unit = DateUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
            return new ArrowType.Date(unit);
        }
    }

    private static class TimeSerDe extends TypedSerDe<ArrowType>
    {
        private static final String UNIT_FIELD = "unit";
        private static final String BIT_WIDTH_FIELD = "bitWidth";

        private TimeSerDe()
        {
            super(ArrowType.Time.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.Time time = (ArrowType.Time) arrowType;
            jgen.writeStringField(UNIT_FIELD, time.getUnit().toString());
            jgen.writeNumberField(BIT_WIDTH_FIELD, time.getBitWidth());
        }

        @Override
        public ArrowType.Time doDeserialize(JsonParser jparser)
                throws IOException
        {
            TimeUnit unit = TimeUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
            int bitWidth = getNextIntField(jparser, BIT_WIDTH_FIELD);
            return new ArrowType.Time(unit, bitWidth);
        }
    }

    private static class TimestampSerDe extends TypedSerDe<ArrowType>
    {
        private static final String UNIT_FIELD = "unit";
        private static final String TIMEZONE_FIELD = "timezone";

        private TimestampSerDe()
        {
            super(ArrowType.Timestamp.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) arrowType;
            jgen.writeStringField(UNIT_FIELD, timestamp.getUnit().toString());
            jgen.writeStringField(TIMEZONE_FIELD, timestamp.getTimezone());
        }

        @Override
        public ArrowType.Timestamp doDeserialize(JsonParser jparser)
                throws IOException
        {
            TimeUnit unit = TimeUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
            String timezone = getNextStringField(jparser, TIMEZONE_FIELD);
            return new ArrowType.Timestamp(unit, timezone);
        }
    }

    private static class IntervalSerDe extends TypedSerDe<ArrowType>
    {
        private static final String UNIT_FIELD = "unit";

        private IntervalSerDe()
        {
            super(ArrowType.Interval.class);
        }

        @Override
        public void doSerialize(JsonGenerator jgen, ArrowType arrowType)
                throws IOException
        {
            ArrowType.Interval interval = (ArrowType.Interval) arrowType;
            jgen.writeStringField(UNIT_FIELD, interval.getUnit().toString());
        }

        @Override
        public ArrowType.Interval doDeserialize(JsonParser jparser)
                throws IOException
        {
            IntervalUnit unit = IntervalUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
            return new ArrowType.Interval(unit);
        }
    }
}
