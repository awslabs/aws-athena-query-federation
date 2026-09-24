/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase.connection;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UnrecoverableExceptionTest
{
    private static final String TEST_ERROR_MESSAGE = "Test error message";

    @Test
    public void unrecoverableException_withMessage_createsExceptionWithMessage()
    {
        String message = TEST_ERROR_MESSAGE;
        UnrecoverableException exception = new UnrecoverableException(message, null);

        assertNotNull("Exception should not be null", exception);
        assertEquals("Exception message should match", message, exception.getMessage());
    }

    @Test
    public void unrecoverableException_withMessageAndCause_createsExceptionWithMessageAndCause()
    {
        String message = TEST_ERROR_MESSAGE;
        Throwable cause = new RuntimeException("Root cause");
        UnrecoverableException exception = new UnrecoverableException(message, cause);

        assertNotNull("Exception should not be null", exception);
        assertEquals("Exception message should match", message, exception.getMessage());
        assertEquals("Exception cause should match", cause, exception.getCause());
    }
}

