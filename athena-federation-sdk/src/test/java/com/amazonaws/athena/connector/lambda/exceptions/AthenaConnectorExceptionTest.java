/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.exceptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.ErrorDetails;

public class AthenaConnectorExceptionTest {

    private String errorMessage = "Test Message";
    private Object testResponse = "Test Response";
    private String errorCode = "TestErrorCode";
    private Exception testCause = new RuntimeException("Test Cause");
    private ErrorDetails errorDetails;

    @Before
    public void setUp()
            throws Exception {
        errorDetails = ErrorDetails.builder()
                .errorCode(errorCode)
                .errorMessage(errorMessage)
                .build();
    }

    @Test
    public void testConstructorWithResponseMessageAndErrorDetails() {
        AthenaConnectorException exception = new AthenaConnectorException(testResponse, errorMessage, errorDetails);

        assertEquals(errorMessage, exception.getMessage());  // Verify the message
        assertEquals(testResponse, exception.getResponse());  // Verify the response
        assertEquals(errorDetails, exception.getErrorDetails());  // Verify the errorDetails
    }

    @Test
    public void testConstructorWithMessageAndErrorDetails() {
        AthenaConnectorException exception = new AthenaConnectorException(errorMessage, errorDetails);

        // Assert
        assertEquals(errorMessage, exception.getMessage());  // Verify the message
        assertNull(exception.getResponse());  // Verify that the response is null
        assertEquals(errorDetails, exception.getErrorDetails());  // Verify the errorDetails
    }

    @Test
    public void testConstructorWithResponseMessageCauseAndErrorDetails() {
        AthenaConnectorException exception = new AthenaConnectorException(testResponse, errorMessage, testCause, errorDetails);

        // Assert
        assertEquals(errorMessage, exception.getMessage());  // Verify the message
        assertEquals(testCause, exception.getCause());  // Verify the cause
        assertEquals(testResponse, exception.getResponse());  // Verify the response
        assertEquals(errorDetails, exception.getErrorDetails());  // Verify the errorDetails
    }

    @Test
    public void testExceptionInheritance() {
        AthenaConnectorException exception = new AthenaConnectorException(errorMessage, errorDetails);

        // Assert
        assertTrue(exception instanceof RuntimeException);  // Verify inheritance
    }
}

