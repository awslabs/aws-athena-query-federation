package com.amazonaws.athena.connector.lambda.handlers;

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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.kms.model.KmsException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ConnectorExceptionHandlerTest
{
    private TestConnectorExceptionHandler exceptionHandler;

    @Before
    public void setUp()
    {
        exceptionHandler = new TestConnectorExceptionHandler();
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

    @Test
    public void testHandle_KmsException_TransformedToAthenaConnectorException() throws Exception
    {
        KmsException kmsException = (KmsException) KmsException.builder()
                .message("KMS key not found")
                .build();

        AthenaConnectorException exception = assertThrows(AthenaConnectorException.class, () ->
                exceptionHandler.handle(() -> {
                    throw kmsException;
                })
        );

        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("KMS key not found"));
        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(),
                exception.getErrorDetails().errorCode());
    }

    @Test
    public void testHandle_GenericException_DelegatedToHandleConnectorException() throws Exception
    {
        RuntimeException runtimeException = new RuntimeException("Generic error");

        AthenaConnectorException exception = assertThrows(AthenaConnectorException.class, () ->
                exceptionHandler.handle(() -> {
                    throw runtimeException;
                })
        );

        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Handled: Generic error"));
        assertEquals(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString(),
                exception.getErrorDetails().errorCode());
    }

    /**
     * Test implementation of ConnectorExceptionHandler
     */
    private static class TestConnectorExceptionHandler extends ConnectorExceptionHandler
    {
        @Override
        protected Exception handleConnectorException(Exception e)
        {
            return new AthenaConnectorException("Handled: " + e.getMessage(),
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString()).build());
        }
    }
}
