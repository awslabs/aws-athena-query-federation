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
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionType.SCALAR;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

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
    public void testMethodNotFound()
    {
        int rowCount = 20;
        UserDefinedFunctionRequest udfRequest = createUDFRequest(rowCount, Integer.class, "method_that_does_not_exsit", true, Integer.class, Integer.class);

        try {
            UserDefinedFunctionResponse udfResponse = handler.processFunction(allocator, udfRequest);
            fail("Expected function to fail due to method not found, but succeeded.");
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof NoSuchMethodException);
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
            float8Vector.setSafe(idx, idx + 100.2);
            return;
        }

        if (fieldVector instanceof VarCharVector) {
            VarCharVector varCharVector = (VarCharVector) fieldVector;
            varCharVector.setSafe(idx, new Text(idx + "-my-varchar"));
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

        if (type == Boolean.class) {
            return new Field(columnName, FieldType.nullable(new ArrowType.Bool()), null);
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
            ;

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
    }
}
