/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the VerticaRecordHandler read-back parsing (per-type extractors + timestamp
 * FieldWriter). Uses Mockito + reflection: allocating a real Arrow vector fails in this build
 * environment, and the private members under test carry no public surface.
 */
@RunWith(MockitoJUnitRunner.class)
public class VerticaRecordHandlerReadBackTest
{
    private static final String TEST_QUERY_ID = "test-query-id";

    // Real column names observed in the Vertica read-back; used as extractor field-name keys.
    private static final String IS_ACTIVE = "is_active";
    private static final String SALARY = "salary";
    private static final String ROW_COUNT = "row_count";
    private static final String AMOUNT = "amount";
    private static final String JOIN_DATE = "join_date";
    private static final String EMPLOYEE_ID = "employee_id";
    private static final String EMPLOYEE_NAME = "employee_name";
    private static final String EVENT_TIME = "event_time";

    // Read-back cell text forms (Vertica EXPORT casts every column to text).
    private static final String TRUE_TEXT = "true";
    private static final String FALSE_TEXT = "false";
    private static final String INVALID_BOOL_TEXT = "maybe";
    private static final String SALARY_TEXT = "51000";
    private static final String BIGINT_TEXT = "9876543210";
    private static final String AMOUNT_TEXT = "99.99";
    private static final String JOIN_DATE_TEXT = "2024-01-02";
    private static final String EMPLOYEE_ID_TEXT = "1";

    // Expected parsed values.
    private static final long EXPECTED_SALARY = 51000L;
    private static final long EXPECTED_BIGINT = 9876543210L;
    private static final double EXPECTED_AMOUNT = 99.99d;

    // Timestamp read-back: 'yyyy-MM-dd HH:mm:ss.ffffff+00' (UTC micros) -> epoch micros.
    private static final String TIMESTAMP_TEXT = "2026-06-23 21:03:59.522361+00";
    private static final long EXPECTED_TIMESTAMP_MICROS = 1782248639522361L;
    private static final String UTC_ZONE = "UTC";

    // The production target-type label embedded in the parse-failure message for a BIT/boolean cell.
    private static final String BIT_TARGET_TYPE = "BIT/boolean";

    private VerticaRecordHandler handler;

    @Before
    public void setUp()
    {
        // Mockito.mock bypasses the constructor (avoids KmsClient.create()); private methods under test
        // run their real bodies via reflection and touch no instance state.
        handler = Mockito.mock(VerticaRecordHandler.class);
    }

    // ==================== parseVerticaBoolean ====================

    @Test
    public void parseVerticaBoolean_acceptedTrueForms_returnTrue() throws Exception
    {
        for (String form : new String[] {"true", "TRUE", "True", "t", "T", "1"}) {
            assertTrue("[" + form + "] should parse as true", invokeParseVerticaBoolean(form, IS_ACTIVE));
        }
    }

    @Test
    public void parseVerticaBoolean_acceptedFalseForms_returnFalse() throws Exception
    {
        for (String form : new String[] {"false", "FALSE", "False", "f", "F", "0"}) {
            assertFalse("[" + form + "] should parse as false", invokeParseVerticaBoolean(form, IS_ACTIVE));
        }
    }

    @Test
    public void parseVerticaBoolean_invalidValue_throwsDescriptiveException()
    {
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> invokeParseVerticaBoolean(INVALID_BOOL_TEXT, IS_ACTIVE));
        String message = thrown.getMessage();
        assertTrue("message should name the offending value: " + message, message.contains(INVALID_BOOL_TEXT));
        assertTrue("message should name the column: " + message, message.contains(IS_ACTIVE));
        assertTrue("message should name the target type: " + message, message.contains(BIT_TARGET_TYPE));
    }

    // ==================== getTrimmedCellValue ====================

    @Test
    public void getTrimmedCellValue_trimsSurroundingWhitespace() throws Exception
    {
        Object context = newRowContext(EMPLOYEE_NAME, new Text("  Jane Doe  "));
        assertEquals("Jane Doe", invokeGetTrimmedCellValue(context, EMPLOYEE_NAME));
    }

    @Test
    public void getTrimmedCellValue_nullCell_returnsNull() throws Exception
    {
        Object context = newRowContext(EMPLOYEE_NAME, null);
        assertNull(invokeGetTrimmedCellValue(context, EMPLOYEE_NAME));
    }

    @Test
    public void getTrimmedCellValue_emptyOrWhitespaceOnly_returnsNull() throws Exception
    {
        assertNull("empty string should map to null",
                invokeGetTrimmedCellValue(newRowContext(EMPLOYEE_NAME, new Text("")), EMPLOYEE_NAME));
        assertNull("whitespace-only should map to null",
                invokeGetTrimmedCellValue(newRowContext(EMPLOYEE_NAME, new Text("   ")), EMPLOYEE_NAME));
    }

    // ==================== timestamp (VERTICA_TIMESTAMP_READ_FORMATTER + factory) ====================

    @Test
    public void timestampFormatter_parsesVerticaReadBackForm_toExpectedEpochMicros() throws Exception
    {
        DateTimeFormatter formatter = getTimestampFormatter();
        long micros = toEpochMicros(OffsetDateTime.parse(TIMESTAMP_TEXT, formatter).toInstant());
        assertEquals(EXPECTED_TIMESTAMP_MICROS, micros);
    }

    @Test
    public void timestampFormatter_offsetVariantsAndSeparators_yieldSameInstant() throws Exception
    {
        DateTimeFormatter formatter = getTimestampFormatter();
        String[] equivalentForms = {
            "2026-06-23 21:03:59.522361+00",     // Vertica space separator, +00 offset
            "2026-06-23 21:03:59.522361+00:00",  // +00:00 offset
            "2026-06-23 21:03:59.522361Z",       // Z (UTC) offset
            "2026-06-23T21:03:59.522361+00",     // ISO-8601 'T' separator
            "2026-06-23 21:03:59.522361",        // no offset -> defaulted to UTC
        };
        for (String form : equivalentForms) {
            long micros = toEpochMicros(OffsetDateTime.parse(form, formatter).toInstant());
            assertEquals("form [" + form + "] should parse to the same UTC epoch-micros",
                    EXPECTED_TIMESTAMP_MICROS, micros);
        }
    }

    @Test
    public void makeTimestampMicroTzFieldWriter_parsesAndWritesEpochMicros() throws Exception
    {
        // Mock the vector so the real factory + FieldWriter run without allocating an Arrow buffer
        // (which fails in this env); setSafe/getField are non-final, so mockable/verifiable.
        TimeStampMicroTZVector vector = Mockito.mock(TimeStampMicroTZVector.class);
        when(vector.getField()).thenReturn(timestampField());

        FieldWriterFactory factory = makeTimestampFactory();
        // null extractor: the factory is registered INSTEAD of an extractor; null constraint = always valid.
        FieldWriter writer = factory.create(vector, null, null);
        boolean valid = writer.write(newRowContext(EVENT_TIME, new Text(TIMESTAMP_TEXT)), 0);

        assertTrue("unconstrained row should be valid", valid);
        ArgumentCaptor<Long> writtenMicros = ArgumentCaptor.forClass(Long.class);
        verify(vector).setSafe(eq(0), writtenMicros.capture());
        assertEquals(EXPECTED_TIMESTAMP_MICROS, writtenMicros.getValue().longValue());
    }

    @Test
    public void makeTimestampMicroTzFieldWriter_nullCell_writesNull() throws Exception
    {
        TimeStampMicroTZVector vector = Mockito.mock(TimeStampMicroTZVector.class);
        when(vector.getField()).thenReturn(timestampField());

        FieldWriterFactory factory = makeTimestampFactory();
        FieldWriter writer = factory.create(vector, null, null);
        boolean valid = writer.write(newRowContext(EVENT_TIME, null), 0);

        assertTrue("unconstrained row should be valid even when the cell is null", valid);
        verify(vector).setNull(0);
        verify(vector, Mockito.never()).setSafe(Mockito.anyInt(), Mockito.anyLong());
    }

    // ==================== makeExtractor (per-type read-back parsing) ====================

    @Test
    public void makeExtractor_bit_parsesBooleanTextToBitValue() throws Exception
    {
        BitExtractor extractor = (BitExtractor) makeExtractor(IS_ACTIVE, Types.MinorType.BIT);

        NullableBitHolder trueHolder = new NullableBitHolder();
        extractor.extract(newRowContext(IS_ACTIVE, new Text(TRUE_TEXT)), trueHolder);
        assertEquals(1, trueHolder.isSet);
        assertEquals(1, trueHolder.value);

        NullableBitHolder falseHolder = new NullableBitHolder();
        extractor.extract(newRowContext(IS_ACTIVE, new Text(FALSE_TEXT)), falseHolder);
        assertEquals(1, falseHolder.isSet);
        assertEquals(0, falseHolder.value);
    }

    @Test
    public void makeExtractor_int_parsesIntegerText() throws Exception
    {
        // INT shares the long-valued BigInt holder with BIGINT, so it routes through BigIntExtractor.
        BigIntExtractor extractor = (BigIntExtractor) makeExtractor(SALARY, Types.MinorType.INT);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(newRowContext(SALARY, new Text(SALARY_TEXT)), holder);
        assertEquals(1, holder.isSet);
        assertEquals(EXPECTED_SALARY, holder.value);
    }

    @Test
    public void makeExtractor_bigInt_parsesLongText() throws Exception
    {
        // A value beyond the int range proves the BIGINT path parses via Long.parseLong.
        BigIntExtractor extractor = (BigIntExtractor) makeExtractor(ROW_COUNT, Types.MinorType.BIGINT);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(newRowContext(ROW_COUNT, new Text(BIGINT_TEXT)), holder);
        assertEquals(1, holder.isSet);
        assertEquals(EXPECTED_BIGINT, holder.value);
    }

    @Test
    public void makeExtractor_float8_parsesDoubleText() throws Exception
    {
        Float8Extractor extractor = (Float8Extractor) makeExtractor(AMOUNT, Types.MinorType.FLOAT8);
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(newRowContext(AMOUNT, new Text(AMOUNT_TEXT)), holder);
        assertEquals(1, holder.isSet);
        assertEquals(EXPECTED_AMOUNT, holder.value, 0.0d);
    }

    @Test
    public void makeExtractor_decimal_parsesBigDecimalText() throws Exception
    {
        DecimalExtractor extractor = (DecimalExtractor) makeExtractor(AMOUNT, Types.MinorType.DECIMAL);
        NullableDecimalHolder holder = new NullableDecimalHolder();
        extractor.extract(newRowContext(AMOUNT, new Text(AMOUNT_TEXT)), holder);
        assertEquals(1, holder.isSet);
        // Production parses with new BigDecimal(value) (no pre-scaling); scale 2 is preserved.
        assertEquals(new BigDecimal(AMOUNT_TEXT), holder.value);
    }

    @Test
    public void makeExtractor_dateDay_parsesIsoDateToEpochDay() throws Exception
    {
        DateDayExtractor extractor = (DateDayExtractor) makeExtractor(JOIN_DATE, Types.MinorType.DATEDAY);
        NullableDateDayHolder holder = new NullableDateDayHolder();
        extractor.extract(newRowContext(JOIN_DATE, new Text(JOIN_DATE_TEXT)), holder);
        assertEquals(1, holder.isSet);
        // Independent oracle: the ISO date string must resolve to this exact epoch-day (2024-01-02 -> 19724).
        int expectedEpochDay = (int) LocalDate.of(2024, 1, 2).toEpochDay();
        assertEquals(expectedEpochDay, holder.value);
    }

    @Test
    public void makeExtractor_varBinary_usesUtf8Bytes() throws Exception
    {
        VarBinaryExtractor extractor = (VarBinaryExtractor) makeExtractor(EMPLOYEE_ID, Types.MinorType.VARBINARY);
        NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
        extractor.extract(newRowContext(EMPLOYEE_ID, new Text(EMPLOYEE_ID_TEXT)), holder);
        assertEquals(1, holder.isSet);
        // EXPORT renders the binary column's content as text; the fix takes its UTF-8 bytes: "1" -> {0x31}.
        assertArrayEquals(new byte[] {0x31}, holder.value);
    }

    @Test
    public void makeExtractor_nullCell_marksHolderNotSet() throws Exception
    {
        BigIntExtractor extractor = (BigIntExtractor) makeExtractor(SALARY, Types.MinorType.INT);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        holder.isSet = 1; // pre-set to prove the extractor actively clears it for a null cell
        extractor.extract(newRowContext(SALARY, null), holder);
        assertEquals(0, holder.isSet);
    }

    // ==================== reflection / construction helpers ====================

    // Reflectively invokes private static parseVerticaBoolean, unwrapping to the real production exception.
    private static boolean invokeParseVerticaBoolean(String value, String fieldName) throws Exception
    {
        Method method = VerticaRecordHandler.class.getDeclaredMethod(
                "parseVerticaBoolean", String.class, String.class);
        method.setAccessible(true);
        try {
            return (boolean) method.invoke(null, value, fieldName);
        }
        catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static String invokeGetTrimmedCellValue(Object rowContext, String fieldName) throws Exception
    {
        Method method = VerticaRecordHandler.class.getDeclaredMethod(
                "getTrimmedCellValue", Object.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, rowContext, fieldName);
    }

    private static DateTimeFormatter getTimestampFormatter() throws Exception
    {
        // Fully-qualified java.lang.reflect.Field to avoid clashing with the Arrow Field import.
        java.lang.reflect.Field field = VerticaRecordHandler.class.getDeclaredField(
                "VERTICA_TIMESTAMP_READ_FORMATTER");
        field.setAccessible(true);
        return (DateTimeFormatter) field.get(null);
    }

    // makeExtractor keys off the MinorType map + field name only, so the Arrow field type is a placeholder.
    private Extractor makeExtractor(String fieldName, Types.MinorType minorType) throws Exception
    {
        Field arrowField = Field.nullable(fieldName, Types.MinorType.VARCHAR.getType());
        HashMap<String, Types.MinorType> namesAndTypes = new HashMap<>();
        namesAndTypes.put(fieldName, minorType);
        HashMap<String, Object> cols = new HashMap<>(); // unused by makeExtractor; passed for the signature
        Method method = VerticaRecordHandler.class.getDeclaredMethod(
                "makeExtractor", Field.class, HashMap.class, HashMap.class);
        method.setAccessible(true);
        return (Extractor) method.invoke(handler, arrowField, namesAndTypes, cols);
    }

    private FieldWriterFactory makeTimestampFactory() throws Exception
    {
        Method method = VerticaRecordHandler.class.getDeclaredMethod("makeTimestampMicroTzFieldWriterFactory");
        method.setAccessible(true);
        return (FieldWriterFactory) method.invoke(handler);
    }

    // Builds a real RowContext (private nested class); the parse helpers cast their context arg to it.
    private static Object newRowContext(String fieldName, Object cellValue) throws Exception
    {
        Class<?> rowContextClass = Class.forName(
                "com.amazonaws.athena.connectors.vertica.VerticaRecordHandler$RowContext");
        Constructor<?> constructor = rowContextClass.getDeclaredConstructor(String.class);
        constructor.setAccessible(true);
        Object rowContext = constructor.newInstance(TEST_QUERY_ID);
        HashMap<String, Object> map = new HashMap<>();
        map.put(fieldName, cellValue);
        Method setNameValue = rowContextClass.getDeclaredMethod("setNameValue", HashMap.class);
        setNameValue.setAccessible(true);
        setNameValue.invoke(rowContext, map);
        return rowContext;
    }

    private static Field timestampField()
    {
        return new Field(EVENT_TIME,
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, UTC_ZONE)), null);
    }

    // Mirrors the epoch-micros math in makeTimestampMicroTzFieldWriterFactory (independent oracle).
    private static long toEpochMicros(Instant instant)
    {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
    }
}
