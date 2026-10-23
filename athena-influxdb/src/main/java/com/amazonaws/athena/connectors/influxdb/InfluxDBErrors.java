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
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.net.http.HttpTimeoutException;

import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.MAX_EXCEPTION_CAUSE_SEARCH_DEPTH;

/**
 * Builds the {@link AthenaConnectorException}s the connector throws, and translates InfluxDB client failures into
 * them, so every error reaches Athena with a {@link FederationSourceErrorCode}.
 */
final class InfluxDBErrors
{
    private InfluxDBErrors()
    {
    }

    static AthenaConnectorException error(final FederationSourceErrorCode code, final String message)
    {
        return new AthenaConnectorException(message, details(code, message));
    }

    static AthenaConnectorException error(final FederationSourceErrorCode code, final String message,
            final Exception cause)
    {
        return new AthenaConnectorException(message, cause, details(code, cause.getMessage()));
    }

    /**
     * Wraps a failed HTTP request to InfluxDB. A request timeout maps to {@code OPERATION_TIMEOUT_EXCEPTION};
     * any other I/O failure maps to {@code INTERNAL_SERVICE_EXCEPTION}.
     */
    static AthenaConnectorException ioError(final String message, final IOException cause)
    {
        final FederationSourceErrorCode code = (cause instanceof HttpTimeoutException)
                ? FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION
                : FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
        return error(code, message, cause);
    }

    /**
     * Translates a failure of an InfluxDB operation into an {@link AthenaConnectorException}, so every error
     * carries a {@link FederationSourceErrorCode}. An {@link AthenaConnectorException} is returned unchanged.
     * Otherwise the cause chain is searched for a Flight status (queries) or an HTTP status (the
     * database-listing API), which is mapped to an error code. An I/O failure is mapped by {@link #ioError}, an
     * interrupt to {@code OPERATION_TIMEOUT_EXCEPTION}, and any other exception to
     * {@code INTERNAL_SERVICE_EXCEPTION}. The original exception is kept as the cause.
     */
    static AthenaConnectorException translate(final Exception exception)
    {
        if (exception instanceof AthenaConnectorException) {
            return (AthenaConnectorException) exception;
        }
        Throwable cause = exception;
        for (int depth = 0; cause != null && depth < MAX_EXCEPTION_CAUSE_SEARCH_DEPTH; cause = cause.getCause(), depth++) {
            if (cause instanceof FlightRuntimeException) {
                final FlightStatusCode status = ((FlightRuntimeException) cause).status().code();
                return error(flightErrorCode(status), "InfluxDB query failed: " + cause.getMessage(), exception);
            }
            if (cause instanceof InfluxDBApiHttpException) {
                final int status = ((InfluxDBApiHttpException) cause).statusCode();
                return error(CompositeHandler.mapHTTPErrorCode(status),
                        "InfluxDB request failed: " + cause.getMessage(), exception);
            }
        }
        if (exception instanceof IOException) {
            return ioError("InfluxDB request failed: " + exception.getMessage(), (IOException) exception);
        }
        if (exception instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return error(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION,
                    "Interrupted while waiting for InfluxDB", exception);
        }
        return error(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION,
                "InfluxDB operation failed: " + exception.getMessage(), exception);
    }

    /**
     * Maps an Arrow Flight status code to a {@link FederationSourceErrorCode}.
     */
    static FederationSourceErrorCode flightErrorCode(final FlightStatusCode status)
    {
        switch (status) {
            case UNAUTHENTICATED:
                return FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION;
            case UNAUTHORIZED:
                return FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION;
            case NOT_FOUND:
                return FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION;
            case INVALID_ARGUMENT:
            case ALREADY_EXISTS:
                return FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
            case TIMED_OUT:
                return FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION;
            case RESOURCE_EXHAUSTED:
                return FederationSourceErrorCode.THROTTLING_EXCEPTION;
            case UNIMPLEMENTED:
                return FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION;
            default:
                // INTERNAL, UNAVAILABLE, CANCELLED, UNKNOWN, OK.
                return FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
        }
    }

    private static ErrorDetails details(final FederationSourceErrorCode code, final String message)
    {
        return ErrorDetails.builder().errorCode(code.toString()).errorMessage(message).build();
    }
}
