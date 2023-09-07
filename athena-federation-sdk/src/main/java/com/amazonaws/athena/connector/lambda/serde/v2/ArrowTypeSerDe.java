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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.serde.DelegatingDeserializer;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerializer;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;

public final class ArrowTypeSerDe
{
    private ArrowTypeSerDe() {}

    public static final class Serializer extends DelegatingSerializer<ArrowType>
    {
        public Serializer()
        {
            super(ArrowType.class, ImmutableSet.<TypedSerializer<ArrowType>>builder()
                    .add(new NullSerDe.Serializer())
                    .add(new StructSerDe.Serializer())
                    .add(new ListSerDe.Serializer())
                    .add(new FixedSizeListSerDe.Serializer())
                    .add(new UnionSerDe.Serializer())
                    .add(new IntSerDe.Serializer())
                    .add(new FloatingPointSerDe.Serializer())
                    .add(new Utf8SerDe.Serializer())
                    .add(new BinarySerDe.Serializer())
                    .add(new FixedSizeBinarySerDe.Serializer())
                    .add(new BoolSerDe.Serializer())
                    .add(new DecimalSerDe.Serializer())
                    .add(new DateSerDe.Serializer())
                    .add(new TimeSerDe.Serializer())
                    .add(new TimeStampSerDe.Serializer())
                    .add(new IntervalSerDe.Serializer())
                    .add(new MapSerDe.Serializer())
                    .build());
        }
    }

    public static final class Deserializer extends DelegatingDeserializer<ArrowType>
    {
        public Deserializer()
        {
            super(ArrowType.class, ImmutableSet.<TypedDeserializer<ArrowType>>builder()
                    .add(new NullSerDe.Deserializer())
                    .add(new StructSerDe.Deserializer())
                    .add(new ListSerDe.Deserializer())
                    .add(new FixedSizeListSerDe.Deserializer())
                    .add(new UnionSerDe.Deserializer())
                    .add(new IntSerDe.Deserializer())
                    .add(new FloatingPointSerDe.Deserializer())
                    .add(new Utf8SerDe.Deserializer())
                    .add(new BinarySerDe.Deserializer())
                    .add(new FixedSizeBinarySerDe.Deserializer())
                    .add(new BoolSerDe.Deserializer())
                    .add(new DecimalSerDe.Deserializer())
                    .add(new DateSerDe.Deserializer())
                    .add(new TimeSerDe.Deserializer())
                    .add(new TimeStampSerDe.Deserializer())
                    .add(new IntervalSerDe.Deserializer())
                    .add(new MapSerDe.Deserializer())
                    .build());
        }
    }

    private static final class NullSerDe
    {
        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Null.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                // no fields
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Null.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                return new ArrowType.Null();
            }
        }
    }

    private static final class StructSerDe
    {
        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Struct.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                // no fields
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Struct.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                return new ArrowType.Struct();
            }
        }
    }

    private static final class ListSerDe
    {
        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.List.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                // no fields
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.List.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                return new ArrowType.List();
            }
        }
    }

    private static final class FixedSizeListSerDe
    {
        private static final String LIST_SIZE_FIELD = "listSize";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.FixedSizeList.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.FixedSizeList fixedSizeList = (ArrowType.FixedSizeList) arrowType;
                jgen.writeNumberField(LIST_SIZE_FIELD, fixedSizeList.getListSize());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.FixedSizeList.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                int listSize = getNextIntField(jparser, LIST_SIZE_FIELD);
                return new ArrowType.FixedSizeList(listSize);
            }
        }
    }

    private static final class UnionSerDe
    {
        private static final String MODE_FIELD = "mode";
        private static final String TYPE_IDS_FIELD = "typeIds";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Union.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
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
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Union.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
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
    }

    private static final class IntSerDe
    {
        private static final String BIT_WIDTH_FIELD = "bitWidth";
        private static final String IS_SIGNED_FIELD = "isSigned";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Int.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.Int arrowInt = (ArrowType.Int) arrowType;
                jgen.writeNumberField(BIT_WIDTH_FIELD, arrowInt.getBitWidth());
                jgen.writeBooleanField(IS_SIGNED_FIELD, arrowInt.getIsSigned());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Int.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                int bitWidth = getNextIntField(jparser, BIT_WIDTH_FIELD);
                boolean isSigned = getNextBoolField(jparser, IS_SIGNED_FIELD);
                return new ArrowType.Int(bitWidth, isSigned);
            }
        }
    }

    private static final class FloatingPointSerDe
    {
        private static final String PRECISION_FIELD = "precision";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.FloatingPoint.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) arrowType;
                jgen.writeStringField(PRECISION_FIELD, floatingPoint.getPrecision().toString());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.FloatingPoint.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                FloatingPointPrecision precision = FloatingPointPrecision.valueOf(getNextStringField(jparser, PRECISION_FIELD));
                return new ArrowType.FloatingPoint(precision);
            }
        }
    }

    private static final class Utf8SerDe
    {
        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Utf8.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                // no fields
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Utf8.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                return new ArrowType.Utf8();
            }
        }
    }

    private static final class BinarySerDe
    {
        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Binary.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                // no fields
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Binary.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                return new ArrowType.Binary();
            }
        }
    }

    private static final class FixedSizeBinarySerDe
    {
        private static final String BYTE_WIDTH_FIELD = "byteWidth";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.FixedSizeBinary.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) arrowType;
                jgen.writeNumberField(BYTE_WIDTH_FIELD, fixedSizeBinary.getByteWidth());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.FixedSizeBinary.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                int byteWidth = getNextIntField(jparser, BYTE_WIDTH_FIELD);
                return new ArrowType.FixedSizeBinary(byteWidth);
            }
        }
    }

    private static final class BoolSerDe
    {
        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Bool.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                // no fields
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Bool.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                return new ArrowType.Bool();
            }
        }
    }

    private static final class DecimalSerDe
    {
        private static final String PRECISION_FIELD = "precision";
        private static final String SCALE_FIELD = "scale";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Decimal.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                jgen.writeNumberField(PRECISION_FIELD, decimal.getPrecision());
                jgen.writeNumberField(SCALE_FIELD, decimal.getScale());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Decimal.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                int precision = getNextIntField(jparser, PRECISION_FIELD);
                int scale = getNextIntField(jparser, SCALE_FIELD);
                return new ArrowType.Decimal(precision, scale);
            }
        }
    }

    private static final class DateSerDe
    {
        private static final String UNIT_FIELD = "unit";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Date.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.Date date = (ArrowType.Date) arrowType;
                jgen.writeStringField(UNIT_FIELD, date.getUnit().toString());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Date.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                DateUnit unit = DateUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
                return new ArrowType.Date(unit);
            }
        }
    }

    private static final class TimeSerDe
    {
        private static final String UNIT_FIELD = "unit";
        private static final String BIT_WIDTH_FIELD = "bitWidth";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Time.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.Time time = (ArrowType.Time) arrowType;
                jgen.writeStringField(UNIT_FIELD, time.getUnit().toString());
                jgen.writeNumberField(BIT_WIDTH_FIELD, time.getBitWidth());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Time.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                TimeUnit unit = TimeUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
                int bitWidth = getNextIntField(jparser, BIT_WIDTH_FIELD);
                return new ArrowType.Time(unit, bitWidth);
            }
        }
    }

    private static final class TimeStampSerDe
    {
        private static final String UNIT_FIELD = "unit";
        private static final String TIMEZONE_FIELD = "timezone";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Timestamp.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.Timestamp timestamp = (ArrowType.Timestamp) arrowType;
                jgen.writeStringField(UNIT_FIELD, timestamp.getUnit().toString());
                jgen.writeStringField(TIMEZONE_FIELD, timestamp.getTimezone());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Timestamp.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                TimeUnit unit = TimeUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
                String timezone = getNextStringField(jparser, TIMEZONE_FIELD);
                return new ArrowType.Timestamp(unit, timezone);
            }
        }
    }

    private static final class IntervalSerDe
    {
        private static final String UNIT_FIELD = "unit";

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Interval.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                ArrowType.Interval interval = (ArrowType.Interval) arrowType;
                jgen.writeStringField(UNIT_FIELD, interval.getUnit().toString());
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Interval.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                IntervalUnit unit = IntervalUnit.valueOf(getNextStringField(jparser, UNIT_FIELD));
                return new ArrowType.Interval(unit);
            }
        }
    }

    private static final class MapSerDe
    {
        //Arrow map types are represented as Map(false)<entries: Struct<key:keyType,value:valueType>>
        private static final Boolean keysSortedFalse = false;

        private static final class Serializer extends TypedSerializer<ArrowType>
        {
            private Serializer()
            {
                super(ArrowType.class, ArrowType.Map.class);
            }

            @Override
            protected void doTypedSerialize(ArrowType arrowType, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException
            {
                // no fields
            }
        }

        private static final class Deserializer extends TypedDeserializer<ArrowType>
        {
            private Deserializer()
            {
                super(ArrowType.class, ArrowType.Map.class);
            }

            @Override
            protected ArrowType doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                    throws IOException
            {
                return new ArrowType.Map(keysSortedFalse);
            }
        }
    }
}
