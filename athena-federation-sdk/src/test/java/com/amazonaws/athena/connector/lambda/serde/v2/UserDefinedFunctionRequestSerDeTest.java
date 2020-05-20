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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDeTest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionType;
import com.fasterxml.jackson.core.JsonEncoding;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class UserDefinedFunctionRequestSerDeTest extends TypedSerDeTest<FederationRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(UserDefinedFunctionRequestSerDeTest.class);

    @Before
    public void beforeTest()
            throws IOException
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

        expected = new UserDefinedFunctionRequest(federatedIdentity,
                inputRecords,
                outputSchema,
                "test-method",
                UserDefinedFunctionType.SCALAR);


        String expectedSerDeFile = utils.getResourceOrFail("serde/v2", "UserDefinedFunctionRequest.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    @Test
    public void serialize()
            throws Exception
    {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        mapper.writeValue(outputStream, expected);

        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("serialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);
        expected.close();

        logger.info("serialize: exit");
    }

    @Test
    public void deserialize()
            throws IOException
    {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        UserDefinedFunctionRequest actual = (UserDefinedFunctionRequest) mapper.readValue(input, FederationRequest.class);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }
}
