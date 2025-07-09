package com.amazonaws.athena.connector.lambda.handlers;

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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.UnitTestBlockUtils;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionResponse;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionType.SCALAR;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class UserDefinedFunctionHandlerTest
{
    private static final String COLUMN_PREFIX = "col_";

    private TestUserDefinedFunctionHandler handler;

    private BlockAllocatorImpl allocator;

    @Before
    public void setUp()
    {
        handler = new TestUserDefinedFunctionHandler();
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testInvocationWithBasicType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(rowCount, Integer.class, "test_scalar_udf", true, Integer.class, Integer.class);

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            int val = (int) UnitTestBlockUtils.getValue(fieldReader, pos);
            int expected = handler.test_scalar_udf(pos + 100, pos + 100);
            assertEquals(expected, val);
        }
    }

    @Test
    public void invocationWithTinyIntType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                Byte.class,
                "test_tiny_int",
                true,
                Byte.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            Object objVal = UnitTestBlockUtils.getValue(fieldReader, pos);
            byte val = objVal != null ? (byte) objVal : 0;
            byte expected = handler.test_tiny_int((byte) (pos + 1));
            assertEquals(expected, val);
        }
    }

    @Test
    public void invocationWithSmallIntType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                Short.class,
                "test_small_int",
                true,
                Short.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            Object objVal = UnitTestBlockUtils.getValue(fieldReader, pos);
            short val = objVal != null ? (short) objVal : 0;
            short expected = handler.test_small_int((short) (pos + 1));
            assertEquals(expected, val);
        }
    }

    @Test
    public void invocationWithFloat4Type()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(rowCount, Float.class, "test_float4", true, Float.class);

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            float val = fieldReader.readFloat();
            float expected = handler.test_float4((float) (pos + 100.1));
            assertEquals(expected, val, 0.0001); // Float comparison with precision
        }
    }

    @Test
    public void invocationWithFloat8Type()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                Double.class,
                "test_float8",
                true,
                Double.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            Object objVal = UnitTestBlockUtils.getValue(fieldReader, pos);
            double val = objVal != null ? (double) objVal : 0.0;
            double expected = handler.test_float8(pos + 0.5);
            assertEquals(expected, val, 0.0001); // Allowing a small delta for floating-point precision
        }
    }

    @Test
    public void invocationWithBitType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                Boolean.class,
                "test_bit",
                true,
                Boolean.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            Object objVal = UnitTestBlockUtils.getValue(fieldReader, pos);
            boolean val = objVal != null && (boolean) objVal;
            boolean expected = handler.test_bit(pos % 2 == 0); // Example logic: true for even, false for odd
            assertEquals(expected, val);
        }
    }

    @Test
    public void invocationWithBigIntType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                Long.class,
                "test_big_int",
                true,
                Long.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            Object objVal = UnitTestBlockUtils.getValue(fieldReader, pos);
            long val = objVal != null ? (long) objVal : 0L;
            long expected = handler.test_big_int(pos + 1L); // Example logic: increment by 1
            assertEquals(expected, val);
        }
    }

    @Test
    public void invocationWithVarcharType() throws Exception
    {
        int rowCount = 10;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                String.class,
                "test_varchar",
                true,
                String.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            String val = (String) UnitTestBlockUtils.getValue(fieldReader, pos);
            String expected = handler.test_varchar("row-" + pos);
            assertEquals(expected, val);
        }
    }

    @Test
    public void invocationWithVarBinaryType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                byte[].class,
                "test_var_binary",
                true,
                byte[].class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            byte[] val = (byte[]) UnitTestBlockUtils.getValue(fieldReader, pos);
            byte[] expected = handler.test_var_binary(String.format("Row-%d", pos + 1).getBytes());
            assertArrayEquals(expected, val);
        }
    }

    @Test
    public void testInvocationWithListType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(rowCount, List.class, "test_list_type", true, List.class);

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            List<Integer> result = (List) UnitTestBlockUtils.getValue(fieldReader, pos);
            List<Integer> expected = handler.test_list_type(ImmutableList.of(pos + 100, pos + 200, pos + 300));
            assertArrayEquals(expected.toArray(), result.toArray());
        }
    }

    @Test
    public void testInvocationWithStructType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(rowCount, Map.class, "test_row_type", true, Map.class);

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            Map<String, Object> actual = (Map) UnitTestBlockUtils.getValue(fieldReader, pos);

            Map<String, Object> input = ImmutableMap.of("intVal", pos + 100, "doubleVal", pos + 200.2);
            Map<String, Object> expected = handler.test_row_type(input);

            for (Map.Entry<String, Object> entry : expected.entrySet()) {
                String key = entry.getKey();
                assertTrue(actual.containsKey(key));
                assertEquals(expected.get(key), actual.get(key));
            }
        }
    }

    @Test
    public void invocationWithDecimalType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                BigDecimal.class,
                "test_decimal",
                true,
                BigDecimal.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            BigDecimal actual = (BigDecimal) UnitTestBlockUtils.getValue(fieldReader, pos);
            BigDecimal expected = handler.test_decimal(BigDecimal.valueOf(pos + 1).setScale(3)); // Scale 2 assumed
            assertEquals(String.valueOf(0), expected, actual);
        }
    }

    @Test
    public void invocationWithDateDayType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                LocalDate.class,
                "test_date_day",
                true,
                LocalDate.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            LocalDate actual = (LocalDate) UnitTestBlockUtils.getValue(fieldReader, pos);
            LocalDate expected = handler.test_date_day(LocalDate.of(2019, 12, 31).plusDays(pos + 1));
            assertEquals(String.valueOf(pos), expected, actual);
        }
    }

    @Test
    public void invocationWithDateMilliType()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                LocalDateTime.class,
                "test_date_milli",
                true,
                LocalDateTime.class
        );

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            LocalDateTime actual = (LocalDateTime) UnitTestBlockUtils.getValue(fieldReader, pos);

            LocalDateTime expected = handler.test_date_milli(LocalDateTime.parse("2019-12-31T00:00:00").plus(pos + 1, ChronoUnit.DAYS));
            assertEquals(String.valueOf(pos), expected, actual);
        }
    }

    @Test
    public void testInvocationWithNullVAlue()
            throws Exception
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(rowCount, Boolean.class, "test_scalar_function_with_null_value", false, Integer.class);

        UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
        Block responseBlock = udfResponse.getRecords();

        assertEquals(1, responseBlock.getFieldReaders().size());
        assertEquals(rowCount, responseBlock.getRowCount());

        FieldReader fieldReader = responseBlock.getFieldReaders().get(0);

        for (int pos = 0; pos < rowCount; ++pos) {
            fieldReader.setPosition(pos);
            assertTrue(fieldReader.isSet());
            Boolean expected = handler.test_scalar_function_with_null_value(null);
            Boolean actual = fieldReader.readBoolean();
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testRequestTypeValidation()
            throws Exception
    {
        FederationRequest federationRequest = new ListSchemasRequest(null, "dummy_catalog", "dummy_qid");

        ObjectMapper objectMapper = VersionedObjectMapperFactory.create(allocator);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        objectMapper.writeValue(byteArrayOutputStream, federationRequest);
        byte[] inputData = byteArrayOutputStream.toByteArray();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputData);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            handler.handleRequest(byteArrayInputStream, outputStream, null);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Expected a UserDefinedFunctionRequest but found"));
        }
    }

    @Test
    public void pingHandleRequest() throws IOException
    {
        FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
        String catalog = "catalog";
        String queryId = "queryId";
        FederationRequest pingRequest = new PingRequest(identity, catalog, queryId);
        ByteArrayOutputStream pingOutputStream = new ByteArrayOutputStream();
        ObjectMapper objectMapper = VersionedObjectMapperFactory.create(allocator);
        objectMapper.writeValue(pingOutputStream, pingRequest);
        ByteArrayInputStream pingInputStream = new ByteArrayInputStream(pingOutputStream.toByteArray());
        ByteArrayOutputStream pingTestOutputStream = new ByteArrayOutputStream();
        handler.handleRequest(pingInputStream, pingTestOutputStream, mock(Context.class));
        FederationResponse response = objectMapper.readValue(pingTestOutputStream.toByteArray(), FederationResponse.class);
        assertNotNull(response);
    }

    @Test
    public void udfHandleRequest() throws Exception
    {
        int rowCount = 5;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(
                rowCount,
                Long.class,
                "test_big_int",
                true,
                Long.class
        );

        ByteArrayOutputStream requestOutStream = new ByteArrayOutputStream();
        ObjectMapper objectMapper = VersionedObjectMapperFactory.create(allocator);
        objectMapper.writeValue(requestOutStream, udfRequest);
        ByteArrayInputStream requestInputStream = new ByteArrayInputStream(requestOutStream.toByteArray());

        ByteArrayOutputStream responseOutputStream = new ByteArrayOutputStream();

        handler.handleRequest(requestInputStream, responseOutputStream, mock(Context.class));

        FederationResponse baseResponse = objectMapper.readValue(
                responseOutputStream.toByteArray(),
                FederationResponse.class
        );

        assertNotNull(baseResponse);
        assertTrue(baseResponse instanceof UserDefinedFunctionResponse);
    }

    @Test
    public void testMethodNotFound()
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(rowCount, Integer.class, "method_that_does_not_exsit", true, Integer.class, Integer.class);

        try {
            UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
            fail("Expected function to fail due to method not found, but succeeded.");
        }
        catch (Exception e) {
            assertTrue(e instanceof AthenaConnectorException);
        }
    }

    private UserDefinedFunctionRequest createUDFRequest(int rowCount, Class returnType, String methodName, boolean nonNullData, Class... argumentTypes)
    {
        Schema inputSchema = buildSchema(argumentTypes);
        Schema outputSchema = buildSchema(returnType);

        Block block = allocator.createBlock(inputSchema);
        block.setRowCount(rowCount);
        if (nonNullData) {
            writeData(block, rowCount);
        }

        return new UserDefinedFunctionRequest(null, block, outputSchema, methodName, SCALAR);
    }

    private void writeData(Block block, int numOfRows)
    {
        for (FieldVector fieldVector : block.getFieldVectors()) {
            fieldVector.setInitialCapacity(numOfRows);
            fieldVector.allocateNew();
            fieldVector.setValueCount(numOfRows);

            for (int idx = 0; idx < numOfRows; ++idx) {
                writeColumn(fieldVector, idx);
            }
        }
    }

    private void writeColumn(FieldVector fieldVector, int idx)
    {
        if (fieldVector instanceof TinyIntVector) {
            TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
            tinyIntVector.setSafe(idx, (byte) (idx + 1));
            return;
        }

        if (fieldVector instanceof SmallIntVector) {
            SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
            smallIntVector.setSafe(idx, (short) (idx + 1));
            return;
        }

        if (fieldVector instanceof IntVector) {
            IntVector intVector = (IntVector) fieldVector;
            intVector.setSafe(idx, idx + 100);
            return;
        }

        if (fieldVector instanceof Float4Vector) {
            Float4Vector float4Vector = (Float4Vector) fieldVector;
            float4Vector.setSafe(idx, idx + 100.1f);
            return;
        }

        if (fieldVector instanceof Float8Vector) {
            Float8Vector float8Vector = (Float8Vector) fieldVector;
            float8Vector.setSafe(idx, idx + 0.5);
            return;
        }

        if (fieldVector instanceof BitVector) {
            BitVector bitVector = (BitVector) fieldVector;
            bitVector.setSafe(idx, idx % 2 == 0 ? 1 : 0); // Example logic: true for even, false for odd
            return;
        }

        if (fieldVector instanceof BigIntVector) {
            BigIntVector bigIntVector = (BigIntVector) fieldVector;
            bigIntVector.setSafe(idx, idx + 1L); // Example logic: increment by 1
            return;
        }

        if (fieldVector instanceof VarCharVector) {
            VarCharVector varCharVector = (VarCharVector) fieldVector;
            varCharVector.setSafe(idx, new Text("row-" + idx));
            return;
        }

        if (fieldVector instanceof VarBinaryVector) {
            VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
            String value = String.format("Row-%d", idx + 1); // Example value
            varBinaryVector.setSafe(idx, value.getBytes());
            return;
        }

        if (fieldVector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) fieldVector;
            BigDecimal value = BigDecimal.valueOf(idx).add(new BigDecimal("1.000"));
            decimalVector.setSafe(idx, value);
            return;
        }

        if (fieldVector instanceof DateDayVector) {
            DateDayVector dateDayVector = (DateDayVector) fieldVector;
            LocalDate localDate = LocalDate.of(2020, 1, 1).plusDays(idx);
            int daysSinceEpoch = (int) localDate.toEpochDay();
            dateDayVector.setSafe(idx, daysSinceEpoch);
            return;
        }

        if (fieldVector instanceof DateMilliVector) {
            DateMilliVector dateMilliVector = (DateMilliVector) fieldVector;
            Instant instant = Instant.parse("2020-01-01T00:00:00Z").plusMillis(idx * 86_400_000L); // 1 day = 86,400,000 ms
            long millisSinceEpoch = instant.toEpochMilli();
            dateMilliVector.setSafe(idx, millisSinceEpoch);
            return;
        }

        if (fieldVector instanceof ListVector) {
            BlockUtils.setComplexValue(fieldVector,
                    idx,
                    FieldResolver.DEFAULT,
                    ImmutableList.of(idx + 100, idx + 200, idx + 300));
            return;
        }

        if (fieldVector instanceof StructVector) {
            Map<String, Object> input = ImmutableMap.of("intVal", idx + 100, "doubleVal", idx + 200.2);
            BlockUtils.setComplexValue(fieldVector,
                    idx,
                    FieldResolver.DEFAULT,
                    input);
            return;
        }

        throw new IllegalArgumentException("Unsupported fieldVector " + fieldVector.getClass().getCanonicalName());
    }

    private Schema buildSchema(Class... types)
    {
        ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
        for (int i = 0; i < types.length; ++i) {
            String columnName = COLUMN_PREFIX + i;
            Field field = getArrowField(types[i], columnName);
            fieldsBuilder.add(field);
        }
        return new Schema(fieldsBuilder.build(), null);
    }

    private Field getArrowField(Class type, String columnName)
    {
        if (type == Byte.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Int(8, true)), null);
        }

        if (type == Short.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Int(16, true)), null);
        }

        if (type == Integer.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        }

        if (type == Float.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        }

        if (type == Double.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        }

        if (type == String.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Utf8()), null);
        }

        if (type == byte[].class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Binary()), null);
        }

        if (type == Boolean.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Bool()), null);
        }

        if (type == Long.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Int(64, true)), null);
        }

        if (type == BigDecimal.class) {
            int precision = 38;
            int scale = 3;
            return new Field(columnName, FieldType.nullable(new ArrowType.Decimal(precision, scale, 128)), null);
        }

        if (type == LocalDate.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        }

        if (type == LocalDateTime.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
        }

        if (type == List.class) {
            Field childField = new Field(columnName, FieldType.nullable(new ArrowType.Int(32, true)), null);
            return new Field(columnName, FieldType.nullable(Types.MinorType.LIST.getType()),
                    Collections.singletonList(childField));
        }

        if (type == Map.class) {
            FieldBuilder fieldBuilder = FieldBuilder.newBuilder(columnName, Types.MinorType.STRUCT.getType());

            Field childField1 = new Field("intVal", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Field childField2 = new Field("doubleVal", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

            fieldBuilder.addField(childField1);
            fieldBuilder.addField(childField2);

            return fieldBuilder.build();
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private static class TestUserDefinedFunctionHandler
            extends UserDefinedFunctionHandler
    {
        public TestUserDefinedFunctionHandler()
        {
            super("test_type");
        }

        public Integer test_scalar_udf(Integer col1, Integer col2)
        {
            return col1 + col2;
        }

        public Byte test_tiny_int(Byte input)
        {
            return (byte) (input * 2); // Example: doubling the input
        }

        public Short test_small_int(Short input)
        {
            return (short) (input * 3); // Example: tripling the input
        }

        public Float test_float4(Float col1)
        {
            return col1 + 1.5f; // Example: adding 1.5 to the input
        }

        public Double test_float8(Double input)
        {
            return input * 2.5; // Example: multiplying the input by 2.5
        }

        public Boolean test_bit(Boolean input)
        {
            return !input; // Example: invert the input
        }

        public Long test_big_int(Long input)
        {
            return input * 2; // Example: double the input
        }

        public String test_varchar(String input)
        {
            return input == null ? null : input.toUpperCase();
        }

        public byte[] test_var_binary(byte[] input)
        {
            // Example: reverse the input bytes
            if (input == null) {
                return null;
            }
            byte[] reversed = new byte[input.length];
            for (int i = 0; i < input.length; i++) {
                reversed[i] = input[input.length - 1 - i];
            }
            return reversed;
        }

        public Boolean test_scalar_function_with_null_value(Integer col1)
        {
            if (col1 == null) {
                return true;
            }
            return false;
        }

        public List<Integer> test_list_type(List<Integer> input)
        {
            return input.stream().map(val -> val + 1).collect(Collectors.toList());
        }

        public Map<String, Object> test_row_type(Map<String, Object> input)
        {
            Integer intVal = (Integer) input.get("intVal");
            Double doubleVal = (Double) input.get("doubleVal");

            return ImmutableMap.of("intVal", intVal + 1, "doubleVal", doubleVal + 1.0);
        }

        public BigDecimal test_decimal(BigDecimal input)
        {
            if (input == null) {
                return null;
            }
            return input.multiply(BigDecimal.valueOf(2)).setScale(3, RoundingMode.HALF_UP);
        }

        public LocalDate test_date_day(LocalDate input)
        {
            return input;
        }

        public LocalDateTime test_date_milli(LocalDateTime input)
        {
            return input;
        }
    }
}