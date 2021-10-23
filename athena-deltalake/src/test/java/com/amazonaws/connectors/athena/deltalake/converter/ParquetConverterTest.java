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
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.connectors.athena.deltalake.converter.ParquetConverter.getExtractor;
import static org.junit.Assert.*;

public class ParquetConverterTest {

    @Test
    public void testExtractors() throws Exception {
        List<Type> fields = Arrays.asList(
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "stringField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "longField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "integerField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "shortField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "byteField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "floatField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "doubleField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "decimalIntField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "decimalLongField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "booleanField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "binaryField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "dateField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "timestampField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "timestampLegacyField"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 5, "decimalFixedLenField")
        );

        MessageType schema = new MessageType("record", fields);
        Group record = new SimpleGroupFactory(schema).newGroup();
        record.add(0, "text-test");
        record.add(1, 100_000_000_000L);
        record.add(2, 100_000_000);
        record.add(3, 10_000);
        record.add(4, 100);
        record.add(5, 100.01f);
        record.add(6, 100_000.0001d);
        record.add(7, 12345);
        record.add(8, 1234567890L);
        record.add(9, true);
        record.add(10, Binary.fromReusedByteArray(new byte[]{1, 3, 5}));
        record.add(11, 18894);
        record.add(12, 1632235944000L);
        record.add(13, new NanoTime(2459479, 11972));
        record.add(14, Binary.fromReusedByteArray(new byte[]{1, 3, 5}));
        Field stringField = Field.nullable("stringField", new ArrowType.Utf8());
        Field longField = Field.nullable("longField", new ArrowType.Int(64, true));
        Field integerField = Field.nullable("integerField", new ArrowType.Int(32, true));
        Field shortField = Field.nullable("shortField", new ArrowType.Int(16, true));
        Field byteField = Field.nullable("byteField", new ArrowType.Int(8, true));
        Field floatField = Field.nullable("floatField", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Field doubleField = Field.nullable("doubleField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        Field decimalIntField = Field.nullable("decimalIntField", new ArrowType.Decimal(8, 2, 32));
        Field decimalLongField = Field.nullable("decimalLongField", new ArrowType.Decimal(12, 2, 32));
        Field booleanField = Field.nullable("booleanField", new ArrowType.Bool());
        Field binaryField = Field.nullable("binaryField", new ArrowType.Binary());
        Field dateField = Field.nullable("dateField", new ArrowType.Date(DateUnit.DAY));
        Field timestampField = Field.nullable("timestampField", new ArrowType.Date(DateUnit.MILLISECOND));
        Field timestampLegacyField = Field.nullable("timestampLegacyField", new ArrowType.Date(DateUnit.MILLISECOND));
        Field decimalFixedLenField = Field.nullable("decimalFixedLenField", new ArrowType.Decimal(12, 2, 32));

        // String test
        Extractor stringExtractor = getExtractor(stringField);
        assertTrue(stringExtractor instanceof VarCharExtractor);
        NullableVarCharHolder stringHolder = new NullableVarCharHolder();
        ((VarCharExtractor)stringExtractor).extract(record, stringHolder);
        assertEquals(1, stringHolder.isSet);
        assertEquals("text-test", stringHolder.value);

        // Long test
        Extractor longExtractor = getExtractor(longField);
        assertTrue(longExtractor instanceof BigIntExtractor);
        NullableBigIntHolder longHolder = new NullableBigIntHolder();
        ((BigIntExtractor)longExtractor).extract(record, longHolder);
        assertEquals(1, longHolder.isSet);
        assertEquals(100_000_000_000L, longHolder.value);

        // Int test
        Extractor intExtractor = getExtractor(integerField);
        assertTrue(intExtractor instanceof IntExtractor);
        NullableIntHolder intHolder = new NullableIntHolder();
        ((IntExtractor)intExtractor).extract(record, intHolder);
        assertEquals(1, intHolder.isSet);
        assertEquals(100_000_000, intHolder.value);

        // Short test
        Extractor shortExtractor = getExtractor(shortField);
        assertTrue(shortExtractor instanceof SmallIntExtractor);
        NullableSmallIntHolder shortHolder = new NullableSmallIntHolder();
        ((SmallIntExtractor)shortExtractor).extract(record, shortHolder);
        assertEquals(1, shortHolder.isSet);
        assertEquals(10_000, shortHolder.value);

        // Byte test
        Extractor byteExtractor = getExtractor(byteField);
        assertTrue(byteExtractor instanceof TinyIntExtractor);
        NullableTinyIntHolder byteHolder = new NullableTinyIntHolder();
        ((TinyIntExtractor)byteExtractor).extract(record, byteHolder);
        assertEquals(1, byteHolder.isSet);
        assertEquals(100, byteHolder.value);

        // Float test
        Extractor floatExtractor = getExtractor(floatField);
        assertTrue(floatExtractor instanceof Float4Extractor);
        NullableFloat4Holder floatHolder = new NullableFloat4Holder();
        ((Float4Extractor)floatExtractor).extract(record, floatHolder);
        assertEquals(1, floatHolder.isSet);
        assertEquals(100.01f, floatHolder.value, 0.1);

        // Double test
        Extractor doubleExtractor = getExtractor(doubleField);
        assertTrue(doubleExtractor instanceof Float8Extractor);
        NullableFloat8Holder doubleHolder = new NullableFloat8Holder();
        ((Float8Extractor)doubleExtractor).extract(record, doubleHolder);
        assertEquals(1, doubleHolder.isSet);
        assertEquals(100_000.0001d, doubleHolder.value, 0.1);

        // Decimal int test
        Extractor decimalIntExtractor = getExtractor(decimalIntField);
        assertTrue(decimalIntExtractor instanceof DecimalExtractor);
        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder decimalIntHolder = new com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder();
        ((DecimalExtractor)decimalIntExtractor).extract(record, decimalIntHolder);
        assertEquals(1, decimalIntHolder.isSet);
        assertEquals(new BigDecimal("123.45"), decimalIntHolder.value);

        // Decimal long test
        Extractor decimalLongExtractor = getExtractor(decimalLongField);
        assertTrue(decimalLongExtractor instanceof DecimalExtractor);
        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder decimalLongHolder = new com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder();
        ((DecimalExtractor)decimalLongExtractor).extract(record, decimalLongHolder);
        assertEquals(1, decimalLongHolder.isSet);
        assertEquals(new BigDecimal("12345678.90"), decimalLongHolder.value);

        // Boolean test
        Extractor booleanExtractor = getExtractor(booleanField);
        assertTrue(booleanExtractor instanceof BitExtractor);
        NullableBitHolder booleanHolder = new NullableBitHolder();
        ((BitExtractor)booleanExtractor).extract(record, booleanHolder);
        assertEquals(1, booleanHolder.isSet);
        assertEquals(1, booleanHolder.value);

        // Binary test
        Extractor binaryExtractor = getExtractor(binaryField);
        assertTrue(binaryExtractor instanceof VarBinaryExtractor);
        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder binaryHolder = new NullableVarBinaryHolder();
        ((VarBinaryExtractor)binaryExtractor).extract(record, binaryHolder);
        assertEquals(1, binaryHolder.isSet);
        assertArrayEquals(new byte[]{1, 3, 5}, binaryHolder.value);

        // Date test
        Extractor dateExtractor = getExtractor(dateField);
        assertTrue(dateExtractor instanceof DateDayExtractor);
        NullableDateDayHolder dateHolder = new NullableDateDayHolder();
        ((DateDayExtractor)dateExtractor).extract(record, dateHolder);
        assertEquals(1, dateHolder.isSet);
        assertEquals(18894, dateHolder.value);

        // Timestamp test
        Extractor timestampExtractor = getExtractor(timestampField);
        assertTrue(timestampExtractor instanceof DateMilliExtractor);
        NullableDateMilliHolder timestampHolder = new NullableDateMilliHolder();
        ((DateMilliExtractor)timestampExtractor).extract(record, timestampHolder);
        assertEquals(1, timestampHolder.isSet);
        assertEquals(1632235944000L, timestampHolder.value);

        // Timestamp legacy test
        Extractor timestampLegacyExtractor = getExtractor(timestampLegacyField);
        assertTrue(timestampLegacyExtractor instanceof DateMilliExtractor);
        NullableDateMilliHolder timestampLegacyHolder = new NullableDateMilliHolder();
        ((DateMilliExtractor)timestampLegacyExtractor).extract(record, timestampLegacyHolder);
        assertEquals(1, timestampLegacyHolder.isSet);
        assertEquals(1632182400000L, timestampLegacyHolder.value);

        // Decimal fixed len byte array test
        Extractor decimalFixedLenExtractor = getExtractor(decimalFixedLenField);
        assertTrue(decimalFixedLenExtractor instanceof DecimalExtractor);
        NullableDecimalHolder decimalFixedLenHolder = new NullableDecimalHolder();
        ((DecimalExtractor)decimalFixedLenExtractor).extract(record, decimalFixedLenHolder);
        assertEquals(1, decimalFixedLenHolder.isSet);
        assertEquals(new BigDecimal("663.09"), decimalFixedLenHolder.value);
    }

    @Test
    public void testExtractorsWithNullValues() throws Exception {
        List<Type> fields = Collections.emptyList();

        MessageType schema = new MessageType("record", fields);
        Group record = new SimpleGroupFactory(schema).newGroup();
        Field stringField = Field.nullable("stringField", new ArrowType.Utf8());
        Field longField = Field.nullable("longField", new ArrowType.Int(64, true));
        Field integerField = Field.nullable("integerField", new ArrowType.Int(32, true));
        Field shortField = Field.nullable("shortField", new ArrowType.Int(16, true));
        Field byteField = Field.nullable("byteField", new ArrowType.Int(8, true));
        Field floatField = Field.nullable("floatField", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Field doubleField = Field.nullable("doubleField", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        Field decimalField = Field.nullable("decimalField", new ArrowType.Decimal(8, 2, 32));
        Field booleanField = Field.nullable("booleanField", new ArrowType.Bool());
        Field binaryField = Field.nullable("binaryField", new ArrowType.Binary());
        Field dateField = Field.nullable("dateField", new ArrowType.Date(DateUnit.DAY));
        Field timestampField = Field.nullable("timestampField", new ArrowType.Date(DateUnit.MILLISECOND));

        // String test
        Extractor stringExtractor = getExtractor(stringField);
        assertTrue(stringExtractor instanceof VarCharExtractor);
        NullableVarCharHolder stringHolder = new NullableVarCharHolder();
        ((VarCharExtractor)stringExtractor).extract(record, stringHolder);
        assertEquals(0, stringHolder.isSet);

        // Long test
        Extractor longExtractor = getExtractor(longField);
        assertTrue(longExtractor instanceof BigIntExtractor);
        NullableBigIntHolder longHolder = new NullableBigIntHolder();
        ((BigIntExtractor)longExtractor).extract(record, longHolder);
        assertEquals(0, longHolder.isSet);

        // Int test
        Extractor intExtractor = getExtractor(integerField);
        assertTrue(intExtractor instanceof IntExtractor);
        NullableIntHolder intHolder = new NullableIntHolder();
        ((IntExtractor)intExtractor).extract(record, intHolder);
        assertEquals(0, intHolder.isSet);

        // Short test
        Extractor shortExtractor = getExtractor(shortField);
        assertTrue(shortExtractor instanceof SmallIntExtractor);
        NullableSmallIntHolder shortHolder = new NullableSmallIntHolder();
        ((SmallIntExtractor)shortExtractor).extract(record, shortHolder);
        assertEquals(0, shortHolder.isSet);

        // Byte test
        Extractor byteExtractor = getExtractor(byteField);
        assertTrue(byteExtractor instanceof TinyIntExtractor);
        NullableTinyIntHolder byteHolder = new NullableTinyIntHolder();
        ((TinyIntExtractor)byteExtractor).extract(record, byteHolder);
        assertEquals(0, byteHolder.isSet);

        // Float test
        Extractor floatExtractor = getExtractor(floatField);
        assertTrue(floatExtractor instanceof Float4Extractor);
        NullableFloat4Holder floatHolder = new NullableFloat4Holder();
        ((Float4Extractor)floatExtractor).extract(record, floatHolder);
        assertEquals(0, floatHolder.isSet);

        // Double test
        Extractor doubleExtractor = getExtractor(doubleField);
        assertTrue(doubleExtractor instanceof Float8Extractor);
        NullableFloat8Holder doubleHolder = new NullableFloat8Holder();
        ((Float8Extractor)doubleExtractor).extract(record, doubleHolder);
        assertEquals(0, doubleHolder.isSet);

        // Decimal test
        Extractor decimalExtractor = getExtractor(decimalField);
        assertTrue(decimalExtractor instanceof DecimalExtractor);
        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder decimalHolder = new com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder();
        ((DecimalExtractor)decimalExtractor).extract(record, decimalHolder);
        assertEquals(0, decimalHolder.isSet);

        // Boolean test
        Extractor booleanExtractor = getExtractor(booleanField);
        assertTrue(booleanExtractor instanceof BitExtractor);
        NullableBitHolder booleanHolder = new NullableBitHolder();
        ((BitExtractor)booleanExtractor).extract(record, booleanHolder);
        assertEquals(0, booleanHolder.isSet);

        // Binary test
        Extractor binaryExtractor = getExtractor(binaryField);
        assertTrue(binaryExtractor instanceof VarBinaryExtractor);
        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder binaryHolder = new NullableVarBinaryHolder();
        ((VarBinaryExtractor)binaryExtractor).extract(record, binaryHolder);
        assertEquals(0, binaryHolder.isSet);

        // Date test
        Extractor dateExtractor = getExtractor(dateField);
        assertTrue(dateExtractor instanceof DateDayExtractor);
        NullableDateDayHolder dateHolder = new NullableDateDayHolder();
        ((DateDayExtractor)dateExtractor).extract(record, dateHolder);
        assertEquals(0, dateHolder.isSet);

        // Timestamp test
        Extractor timestampExtractor = getExtractor(timestampField);
        assertTrue(timestampExtractor instanceof DateMilliExtractor);
        NullableDateMilliHolder timestampHolder = new NullableDateMilliHolder();
        ((DateMilliExtractor)timestampExtractor).extract(record, timestampHolder);
        assertEquals(0, timestampHolder.isSet);
    }
}
