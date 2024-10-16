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

import com.amazonaws.athena.connector.lambda.serde.TypedSerDeTest;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class LambdaFunctionExceptionSerDeTest
        extends TypedSerDeTest<LambdaException>
{
    private static final Logger logger = LoggerFactory.getLogger(LambdaFunctionExceptionSerDeTest.class);

    @Before
    public void beforeTest()
            throws IOException, ReflectiveOperationException
    {
        String errorType = "com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException";
        String errorMessage =
                "Requested resource not found (Service: AmazonDynamoDBv2; Status Code: 400; Error Code: ResourceNotFoundException; Request ID: RIB6NOH4BNMAK6KQG88R5VE583VV4KQNSO5AEMVJF66Q9ASUAAJG)";
        ImmutableList<ImmutableList<String>> stackTrace = ImmutableList.of(
                ImmutableList.of("com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1701)"),
                ImmutableList.of("com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1356)"));
        expected = (LambdaException) LambdaException.builder().message(errorMessage + ". Stack trace: " + stackTrace + "\nErrorType: " + errorType).build();
        String expectedSerDeFile = utils.getResourceOrFail("serde/v2", "LambdaFunctionException.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    @Override
    public void serialize()
            throws Exception
    {
        // No-op (we never serialize these exceptions)
    }

    @Test
    public void deserialize()
            throws IOException
    {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        LambdaException actual = mapper.readValue(input, LambdaException.class);

        logger.info("deserialize: deserialized[{}]", actual.toString());

        assertEquals(expected.getMessage(), actual.getMessage());
        assertEquals(expected.getCause(), actual.getCause());
        expected.fillInStackTrace();
        actual.fillInStackTrace();
        assertEquals(expected.getStackTrace().length, actual.getStackTrace().length);

        logger.info("deserialize: exit");
    }
}
