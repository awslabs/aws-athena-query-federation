package com.amazonaws.athena.connector.lambda.exceptions;

import com.amazonaws.services.glue.model.ErrorDetails;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Exception that should be thrown by each individual Connector when an error is encountered.
 * That error will be using these following ErrorCode
 *
 * FederationSourceErrorCode:
 *     AccessDeniedException("AccessDeniedException"),
 *     EntityNotFoundException("EntityNotFoundException"),
 *     InvalidCredentialsException("InvalidCredentialsException"),
 *     InvalidInputException("InvalidInputException"),
 *     InvalidResponseException("InvalidResponseException"),
 *     OperationTimeoutException("OperationTimeoutException"),
 *     OperationNotSupportedException("OperationNotSupportedException"),
 *     InternalServiceException("InternalServiceException"),
 *     PartialFailureException("PartialFailureException"),
 *     ThrottlingException("ThrottlingException");
 *
 */
public class AthenaConnectorException extends RuntimeException {
    private final Object response;

    private final ErrorDetails errorDetails;

    public AthenaConnectorException(@Nonnull final Object response,
                                    @Nonnull final String message,
                                    @Nonnull final ErrorDetails errorDetails) {
        super(message);
        this.errorDetails = Objects.requireNonNull(errorDetails);
        this.response = Objects.requireNonNull(response);
        Objects.requireNonNull(message);
    }

    public AthenaConnectorException(@Nonnull final String message,
                                    @Nonnull final ErrorDetails errorDetails) {
        super(message);
        response = null;
        this.errorDetails = Objects.requireNonNull(errorDetails);
        Objects.requireNonNull(message);
    }

    public AthenaConnectorException(@Nonnull final Object response,
                                    @Nonnull final String message,
                                    @Nonnull final Exception e,
                                    @Nonnull final ErrorDetails errorDetails) {
        super(message, e);
        this.errorDetails = Objects.requireNonNull(errorDetails);
        this.response = Objects.requireNonNull(response);
        Objects.requireNonNull(message);
        Objects.requireNonNull(e);
    }

    public Object getResponse() {
        return response;
    }

    public ErrorDetails getErrorDetails() {
        return errorDetails;
    }
}