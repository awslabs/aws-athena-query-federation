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

import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.ErrorDetails;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AthenaConnectorExceptionTest
{
    private String errorMessage = "Test Message";
    private String errorCode = "TestErrorCode";
    private Object testResponse = "Test Response";
    private Exception testCause = new RuntimeException("Test Cause");

    // Assertion messages
    private String errorMessageMatchMsg = "Error message should match";
    private String responseMatchMsg = "Response should match";
    private String causeMatchMsg = "Cause should match";
    private String errorDetailsMatchMsg = "Error details should match";
    private String causeNullMsg = "Expected no cause for (message, details) constructor";
    private String responseNullMsg = "Response should be null when not provided";

    private ErrorDetails errorDetails;

    @Before
    public void setUp()
    {
        errorDetails = ErrorDetails.builder()
                .errorCode(errorCode)
                .errorMessage(errorMessage)
                .build();
    }

    @Test
    public void constructor_withResponseMessageAndErrorDetails_shouldSetFieldsCorrectly()
    {
        AthenaConnectorException exception = new AthenaConnectorException(testResponse, errorMessage, errorDetails);

        assertEquals(errorMessageMatchMsg, errorMessage, exception.getMessage());
        assertEquals(responseMatchMsg, testResponse, exception.getResponse());
        assertEquals(errorDetailsMatchMsg, errorDetails, exception.getErrorDetails());
        assertNull(causeNullMsg, exception.getCause());
    }

    @Test
    public void constructor_withMessageAndErrorDetails_shouldSetFieldsCorrectly()
    {
        AthenaConnectorException exception = new AthenaConnectorException(errorMessage, errorDetails);

        assertEquals(errorMessageMatchMsg, errorMessage, exception.getMessage());
        assertNull(responseNullMsg, exception.getResponse());
        assertEquals(errorDetailsMatchMsg, errorDetails, exception.getErrorDetails());
        assertNull(causeNullMsg, exception.getCause());
    }

    @Test
    public void constructor_withResponseMessageCauseAndErrorDetails_shouldSetFieldsCorrectly()
    {
        AthenaConnectorException exception = new AthenaConnectorException(testResponse, errorMessage, testCause, errorDetails);

        assertEquals(errorMessageMatchMsg, errorMessage, exception.getMessage());
        assertEquals(causeMatchMsg, testCause, exception.getCause());
        assertEquals(responseMatchMsg, testResponse, exception.getResponse());
        assertEquals(errorDetailsMatchMsg, errorDetails, exception.getErrorDetails());
    }
}