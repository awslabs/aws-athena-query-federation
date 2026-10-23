/*-
 * #%L
 * athena-influxdb
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
package com.amazonaws.athena.connectors.influxdb;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.net.http.HttpTimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class InfluxDBErrorsTest
{
    /**
     * Asserts that {@code e} is an {@link AthenaConnectorException} with the given error code.
     */
    static void assertErrorCode(final FederationSourceErrorCode expected, final Throwable e)
    {
        assertTrue("Expected AthenaConnectorException, was: " + e, e instanceof AthenaConnectorException);
        assertEquals(expected.toString(), ((AthenaConnectorException) e).getErrorDetails().errorCode());
    }

    @Test
    public void testErrorCarriesCodeMessageAndCause()
    {
        final IOException cause = new IOException("connection reset");
        final AthenaConnectorException e =
                InfluxDBErrors.error(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION, "request failed", cause);
        assertErrorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION, e);
        assertEquals("request failed", e.getMessage());
        assertSame(cause, e.getCause());
        assertEquals("connection reset", e.getErrorDetails().errorMessage());
    }

    @Test
    public void testTranslateFlightStatus()
    {
        assertErrorCode(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.UNAUTHENTICATED.toRuntimeException()));
        assertErrorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.UNAUTHORIZED.toRuntimeException()));
        assertErrorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.NOT_FOUND.toRuntimeException()));
        assertErrorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.INVALID_ARGUMENT.toRuntimeException()));
        assertErrorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.TIMED_OUT.toRuntimeException()));
        assertErrorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.RESOURCE_EXHAUSTED.toRuntimeException()));
        assertErrorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.UNIMPLEMENTED.toRuntimeException()));
        assertErrorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION,
                InfluxDBErrors.translate(CallStatus.UNAVAILABLE.toRuntimeException()));
        // Every Flight status maps to some code.
        for (final FlightStatusCode status : FlightStatusCode.values()) {
            InfluxDBErrors.flightErrorCode(status);
        }
    }

    @Test
    public void testTranslateFlightStatusInCauseChain()
    {
        final RuntimeException wrapped = new RuntimeException("wrap", CallStatus.RESOURCE_EXHAUSTED.toRuntimeException());
        final AthenaConnectorException e = InfluxDBErrors.translate(wrapped);
        assertErrorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION, e);
        assertSame(wrapped, e.getCause());
    }

    @Test
    public void testTranslateHttpStatus()
    {
        assertErrorCode(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION,
                InfluxDBErrors.translate(new InfluxDBApiHttpException("unauthorized", null, 401)));
        assertErrorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION,
                InfluxDBErrors.translate(new InfluxDBApiHttpException("forbidden", null, 403)));
        assertErrorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION,
                InfluxDBErrors.translate(new InfluxDBApiHttpException("slow down", null, 429)));
        assertErrorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION,
                InfluxDBErrors.translate(new InfluxDBApiHttpException("server error", null, 500)));
    }

    @Test
    public void testTranslateOtherExceptions()
    {
        assertErrorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION,
                InfluxDBErrors.translate(new HttpTimeoutException("request timed out")));
        assertErrorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION,
                InfluxDBErrors.translate(new IOException("connection reset")));
        assertErrorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION,
                InfluxDBErrors.translate(new IllegalStateException("boom")));
        try {
            assertErrorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION,
                    InfluxDBErrors.translate(new InterruptedException()));
            // The interrupt flag is restored for the caller.
            assertTrue(Thread.currentThread().isInterrupted());
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testTranslateKeepsAthenaConnectorException()
    {
        final AthenaConnectorException original =
                InfluxDBErrors.error(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION, "missing");
        assertSame(original, InfluxDBErrors.translate(original));
    }
}
