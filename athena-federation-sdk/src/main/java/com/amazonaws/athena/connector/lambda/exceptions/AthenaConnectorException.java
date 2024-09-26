/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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

import software.amazon.awssdk.services.glue.model.ErrorDetails;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

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

public class AthenaConnectorException extends RuntimeException
{
    private final Object response;

    private final ErrorDetails errorDetails;

    public AthenaConnectorException(@Nonnull final Object response,
                                    @Nonnull final String message,
                                    @Nonnull final ErrorDetails errorDetails)
    {
        super(message);
        this.errorDetails = requireNonNull(errorDetails);
        this.response = requireNonNull(response);
        requireNonNull(message);
    }

    public AthenaConnectorException(@Nonnull final String message,
                                    @Nonnull final ErrorDetails errorDetails)
    {
        super(message);
        response = null;
        this.errorDetails = requireNonNull(errorDetails);
        requireNonNull(message);
    }

    public AthenaConnectorException(@Nonnull final Object response,
                                    @Nonnull final String message,
                                    @Nonnull final Exception e,

                                    @Nonnull final ErrorDetails errorDetails)
    {
        super(message, e);
        this.errorDetails = requireNonNull(errorDetails);
        this.response = requireNonNull(response);
        requireNonNull(message);
        requireNonNull(e);
    }

    public Object getResponse()
    {
        return response;
    }

    public ErrorDetails getErrorDetails()
    {
        return errorDetails;
    }
}
