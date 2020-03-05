package com.amazonaws.athena.connector.lambda.examples;

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
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.projectors.ArrowValueProjector;
import com.amazonaws.athena.connector.lambda.data.projectors.ProjectorUtils;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperUtil;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionResponse;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ExampleUserDefinedFunctionHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleUserDefinedFunctionHandlerTest.class);

    private BlockAllocatorImpl allocator;
    private ExampleUserDefinedFunctionHandler exampleUserDefinedFunctionHandler;
    private ObjectMapper mapper;

    @Before
    public void setUp()
    {
        logger.info("setUpBefore - enter");

        this.exampleUserDefinedFunctionHandler = new ExampleUserDefinedFunctionHandler();
        this.allocator = new BlockAllocatorImpl();
        this.mapper = VersionedObjectMapperFactory.create(allocator);
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void testMultiplyMethod() throws Exception
    {
        Schema inputSchema = SchemaBuilder.newBuilder()
                .addField("factor1", Types.MinorType.INT.getType())
                .addField("factor2", Types.MinorType.INT.getType())
                .build();
        Schema outputSchema = SchemaBuilder.newBuilder()
                .addField("product", Types.MinorType.INT.getType())
                .build();

        Block inputRecords = allocator.createBlock(inputSchema);
        inputRecords.setRowCount(1);
        IntVector inputVector1 = (IntVector) inputRecords.getFieldVector("factor1");
        IntVector inputVector2 = (IntVector) inputRecords.getFieldVector("factor2");
        inputVector1.setSafe(0, 2);
        inputVector2.setSafe(0, 3);

        UserDefinedFunctionResponse response = runAndAssertSerialization(inputRecords, outputSchema, "multiply");

        Block outputRecords = response.getRecords();
        assertEquals(1, outputRecords.getRowCount());
        FieldReader fieldReader = outputRecords.getFieldReader("product");
        ArrowValueProjector arrowValueProjector = ProjectorUtils.createArrowValueProjector(fieldReader);
        assertEquals(exampleUserDefinedFunctionHandler.multiply(2, 3), arrowValueProjector.project(0));
    }

    @Test
    public void testConcatenateMethod() throws Exception
    {
        Schema inputSchema = SchemaBuilder.newBuilder()
                .addListField("list", Types.MinorType.VARCHAR.getType())
                .build();
        Schema outputSchema = SchemaBuilder.newBuilder()
                .addField("string", Types.MinorType.VARCHAR.getType())
                .build();

        Block inputRecords = allocator.createBlock(inputSchema);
        inputRecords.setRowCount(1);
        FieldVector fieldVector = inputRecords.getFieldVector("list");
        List<String> value = Lists.newArrayList("a", "b");
        BlockUtils.setComplexValue(fieldVector, 0, FieldResolver.DEFAULT, value);


        UserDefinedFunctionResponse response = runAndAssertSerialization(inputRecords, outputSchema, "concatenate");

        Block outputRecords = response.getRecords();
        assertEquals(1, outputRecords.getRowCount());
        FieldReader fieldReader = outputRecords.getFieldReader("string");
        ArrowValueProjector arrowValueProjector = ProjectorUtils.createArrowValueProjector(fieldReader);
        assertEquals(exampleUserDefinedFunctionHandler.concatenate(Lists.newArrayList("a", "b")),
                arrowValueProjector.project(0));
    }

    @Test
    public void testToJsonMethod() throws Exception
    {
        Schema inputSchema = SchemaBuilder.newBuilder()
                .addStructField("struct")
                .addChildField("struct", "int", Types.MinorType.INT.getType())
                .addChildField("struct", "double", Types.MinorType.FLOAT8.getType())
                .addChildField("struct", "string", Types.MinorType.VARCHAR.getType())
                .build();
        Schema outputSchema = SchemaBuilder.newBuilder()
                .addField("json", Types.MinorType.VARCHAR.getType())
                .build();

        Block inputRecords = allocator.createBlock(inputSchema);
        inputRecords.setRowCount(1);
        FieldVector fieldVector = inputRecords.getFieldVector("struct");
        Map<String, Object> struct = new HashMap<>();
        struct.put("int", 10);
        struct.put("double", 2.3);
        struct.put("string", "test_string");
        BlockUtils.setComplexValue(fieldVector, 0, FieldResolver.DEFAULT, struct);

        UserDefinedFunctionResponse response = runAndAssertSerialization(inputRecords, outputSchema, "to_json");

        Block outputRecords = response.getRecords();
        assertEquals(1, outputRecords.getRowCount());
        FieldReader fieldReader = outputRecords.getFieldReader("json");
        ArrowValueProjector arrowValueProjector = ProjectorUtils.createArrowValueProjector(fieldReader);
        assertEquals(exampleUserDefinedFunctionHandler.to_json(struct), arrowValueProjector.project(0));
    }

    @Test
    public void testGetDefaultValueIfNullMethod() throws Exception
    {
        Schema inputSchema = SchemaBuilder.newBuilder()
                .addField("input", Types.MinorType.BIGINT.getType())
                .build();
        Schema outputSchema = SchemaBuilder.newBuilder()
                .addField("output", Types.MinorType.BIGINT.getType())
                .build();

        Block inputRecords = allocator.createBlock(inputSchema);
        inputRecords.setRowCount(2);
        BigIntVector fieldVector = (BigIntVector) inputRecords.getFieldVector("input");
        fieldVector.setSafe(0, 123l);
        fieldVector.setNull(1);

        UserDefinedFunctionResponse response = runAndAssertSerialization(inputRecords, outputSchema, "get_default_value_if_null");

        Block outputRecords = response.getRecords();
        assertEquals(2, outputRecords.getRowCount());
        FieldReader fieldReader = outputRecords.getFieldReader("output");
        ArrowValueProjector arrowValueProjector = ProjectorUtils.createArrowValueProjector(fieldReader);
        assertEquals(exampleUserDefinedFunctionHandler.get_default_value_if_null(123l), arrowValueProjector.project(0));
        assertEquals(exampleUserDefinedFunctionHandler.get_default_value_if_null(null), arrowValueProjector.project(1));
    }

    private UserDefinedFunctionResponse runAndAssertSerialization(Block inputRecords,
                                                                  Schema outputSchema,
                                                                  String methodName) throws IOException
    {
        UserDefinedFunctionRequest request = new UserDefinedFunctionRequest(IdentityUtil.fakeIdentity(),
                inputRecords,
                outputSchema,
                methodName,
                UserDefinedFunctionType.SCALAR);
        ObjectMapperUtil.assertSerialization(request);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mapper.writeValue(out, request);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(out.toByteArray());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        exampleUserDefinedFunctionHandler.handleRequest(byteArrayInputStream, outputStream, null);

        UserDefinedFunctionResponse udfResponse = (UserDefinedFunctionResponse) mapper.readValue(outputStream.toByteArray(), FederationResponse.class);
        ObjectMapperUtil.assertSerialization(udfResponse);

        return udfResponse;
    }
}
