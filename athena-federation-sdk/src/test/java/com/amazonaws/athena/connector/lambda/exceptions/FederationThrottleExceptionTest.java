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
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class FederationThrottleExceptionTest {

    private String errorMessage = "An error occurred";

    @Test
    public void testWithDefaultConstructor() {
        FederationThrottleException actualFederationThrottleException = new FederationThrottleException();

        assertNull(actualFederationThrottleException.getMessage());
        assertNull(actualFederationThrottleException.getCause());
        assertEquals(0, actualFederationThrottleException.getSuppressed().length);
    }

    @Test
    public void testWithErrorMessage() {
        FederationThrottleException actualFederationThrottleException = new FederationThrottleException(
                errorMessage);

        assertEquals(errorMessage, actualFederationThrottleException.getMessage());
        assertNull(actualFederationThrottleException.getCause());
        assertEquals(0, actualFederationThrottleException.getSuppressed().length);
    }

    @Test
    public void testWithErrorMessageAndThrowable() {
        Throwable cause = new Throwable();

        FederationThrottleException actualFederationThrottleException = new FederationThrottleException(errorMessage,
                cause);

        assertEquals(errorMessage, actualFederationThrottleException.getMessage());
        assertEquals(0, actualFederationThrottleException.getSuppressed().length);
        assertSame(cause, actualFederationThrottleException.getCause());
    }
}
