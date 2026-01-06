/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DynamoDBConnectorExceptionHandlerTest
{
    private DynamoDBConnectorExceptionHandler exceptionHandler;

    @Before
    public void setUp()
    {
        exceptionHandler = new DynamoDBConnectorExceptionHandler();
    }

    @Test
    public void testConstructor()
    {
        DynamoDBConnectorExceptionHandler handler = new DynamoDBConnectorExceptionHandler();
        assertNotNull(handler);
    }

    @Test
    public void testHandle_DynamoDbException_TransformedToAthenaConnectorException() throws Exception
    {
        DynamoDbException dynamoException = (DynamoDbException) DynamoDbException.builder()
                .message("DynamoDB error occurred")
                .build();

        AthenaConnectorException exception = assertThrows(AthenaConnectorException.class, () ->
                exceptionHandler.handle(() -> {
                    throw dynamoException;
                })
        );

        assertNotNull(exception);
        assertEquals("DynamoDB error occurred", exception.getMessage());
        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(),
                exception.getErrorDetails().errorCode());
    }

    @Test
    public void testHandle_NonDynamoDbException_NotTransformed() throws Exception
    {
        RuntimeException runtimeException = new RuntimeException("Generic error");

        Exception exception = assertThrows(RuntimeException.class, () ->
                exceptionHandler.handle(() -> {
                    throw runtimeException;
                })
        );

        assertNotNull(exception);
        assertSame(runtimeException, exception);
        assertEquals("Generic error", exception.getMessage());
    }

    @Test
    public void testHandle_SuccessfulExecution() throws Exception
    {
        final boolean[] executed = {false};

        exceptionHandler.handle(() -> {
            executed[0] = true;
        });

        assertTrue("Runnable should have been executed", executed[0]);
    }
}
