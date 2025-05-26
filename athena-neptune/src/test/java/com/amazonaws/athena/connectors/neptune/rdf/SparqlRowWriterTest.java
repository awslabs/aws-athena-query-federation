/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SparqlRowWriterTest {

    @Mock
    private GeneratedRowWriter.RowWriterBuilder mockRowWriterBuilder;
    private Map<String, Object> testRow;
    private static final String TEST_FIELD_NAME = "testField";
    private static final String DATE_FORMAT = "EEE MMM dd HH:mm:ss z yyyy";
    private static final int TEST_INT_VALUE = 42;
    private static final long TEST_LONG_VALUE = 42L;
    private static final float TEST_FLOAT_VALUE = 42.5f;
    private static final double TEST_DOUBLE_VALUE = 42.5d;
    private static final String BIG_INT_STRING = "42";
    private static final String BIG_DEC_STRING = "42.5";
    private static final long MILLIS_1000 = 1000L;

    @Before
    public void setup() {
        testRow = new HashMap<>();
    }

    @Test
    public void extractValue_withIntegerValue_returnsIntegerObject() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        testRow.put(TEST_FIELD_NAME, TEST_INT_VALUE);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Integer.class);
        assertEquals(TEST_INT_VALUE, result);
    }

    @Test
    public void extractValue_withBigIntegerForIntegerField_returnsConvertedInteger() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        BigInteger bigInt = new BigInteger(BIG_INT_STRING);
        testRow.put(TEST_FIELD_NAME, bigInt);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Integer.class);
        assertEquals(TEST_INT_VALUE, (result));
    }

    @Test
    public void extractValue_withLongValue_returnsLongObject() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(64, true)), null);
        testRow.put(TEST_FIELD_NAME, TEST_LONG_VALUE);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Long.class);
        assertEquals(TEST_LONG_VALUE, result);
    }

    @Test
    public void extractValue_withBigIntegerForLongField_returnsConvertedLong() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(64, true)), null);
        BigInteger bigInt = new BigInteger(BIG_INT_STRING);
        testRow.put(TEST_FIELD_NAME, bigInt);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Long.class);
        assertEquals(TEST_LONG_VALUE, result);
    }

    @Test
    public void extractValue_withFloatValue_returnsFloatObject() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        testRow.put(TEST_FIELD_NAME, TEST_FLOAT_VALUE);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Float.class);
        assertEquals(TEST_FLOAT_VALUE, result);
    }

    @Test
    public void extractValue_withBigDecimalForDoubleField_returnsConvertedDouble() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        BigDecimal bigDec = new BigDecimal(BIG_DEC_STRING);
        testRow.put(TEST_FIELD_NAME, bigDec);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Double.class);
        assertEquals(TEST_DOUBLE_VALUE, result);
    }

    @Test
    public void extractValue_withBooleanValue_returnsBooleanObject() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Bool()), null);
        testRow.put(TEST_FIELD_NAME, true);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Boolean.class);
        assertEquals(true, result);
    }

    @Test
    public void extractValue_withXmlGregorianCalendar_returnsFormattedDateString() throws Exception {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
        
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis(MILLIS_1000);
        XMLGregorianCalendar xmlCal = DatatypeFactory.newInstance().newXMLGregorianCalendar(cal);
        testRow.put(TEST_FIELD_NAME, xmlCal);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, java.util.Date.class);
        Assert.assertNotNull(result);
        assertEquals(MILLIS_1000, new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH).parse(result.toString()).getTime());
    }

    @Test
    public void extractValue_withNullValue_returnsNull() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        testRow.put(TEST_FIELD_NAME, null);
        
        Object result = SparqlRowWriter.extractValue(testRow, field, Integer.class);
        assertNull(result);
    }

    @Test(expected = ClassCastException.class)
    public void extractValue_withLongValueAndInt32Field_throwsClassCastException() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        testRow.put(TEST_FIELD_NAME, TEST_LONG_VALUE);
        
        SparqlRowWriter.extractValue(testRow, field, Integer.class);
    }

    @Test(expected = ClassCastException.class)
    public void extractValue_withIntValueAndInt64Field_throwsClassCastException() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(64, true)), null);
        testRow.put(TEST_FIELD_NAME, TEST_INT_VALUE);
        
        SparqlRowWriter.extractValue(testRow, field, Long.class);
    }

    @Test(expected = ClassCastException.class)
    public void extractValue_withDoubleValueAndFloat4Field_throwsClassCastException() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        testRow.put(TEST_FIELD_NAME, TEST_DOUBLE_VALUE);
        
        SparqlRowWriter.extractValue(testRow, field, Float.class);
    }

    @Test(expected = ClassCastException.class)
    public void extractValue_withFloatValueAndFloat8Field_throwsClassCastException() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        testRow.put(TEST_FIELD_NAME, TEST_FLOAT_VALUE);
        
        SparqlRowWriter.extractValue(testRow, field, Double.class);
    }

    @Test(expected = NullPointerException.class)
    public void extractValue_withNullContext_throwsNullPointerException() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        SparqlRowWriter.extractValue(null, field, Integer.class);
    }

    @Test
    public void extractValue_withMissingFieldInRow_returnsNull() {
        Field field = new Field("nonExistentField", FieldType.nullable(new ArrowType.Int(32, true)), null);
        testRow.put(TEST_FIELD_NAME, TEST_INT_VALUE);
        Object result = SparqlRowWriter.extractValue(testRow, field, Integer.class);
        assertNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_withNullRowWriterBuilder_throwsNullPointerException() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        SparqlRowWriter.writeRowTemplate(null, field);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_withNullField_throwsNullPointerException() {
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, null);
    }

    @Test
    public void writeRowTemplate_withBitField_registersBitExtractor() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Bool()), null);
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, field);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD_NAME), any());
    }

    @Test
    public void writeRowTemplate_withVarCharField_registersVarCharExtractor() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Utf8()), null);
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, field);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD_NAME), any());
    }

    @Test
    public void writeRowTemplate_withDateMilliField_registersDateMilliExtractor() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, field);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD_NAME), any());
    }

    @Test
    public void writeRowTemplate_withIntField_registersIntExtractor() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, field);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD_NAME), any());
    }

    @Test
    public void writeRowTemplate_withBigIntField_registersBigIntExtractor() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.Int(64, true)), null);
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, field);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD_NAME), any());
    }

    @Test
    public void writeRowTemplate_withFloat4Field_registersFloat4Extractor() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, field);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD_NAME), any());
    }

    @Test
    public void writeRowTemplate_withFloat8Field_registersFloat8Extractor() {
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        SparqlRowWriter.writeRowTemplate(mockRowWriterBuilder, field);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD_NAME), any());
    }
} 
