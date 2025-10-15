/*-
 * #%L
 * athena-larkbase
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.lark.base.throttling;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.model.TooManyRequestsException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.ConcurrentRunsExceededException;
import software.amazon.awssdk.services.glue.model.ConflictException;
import software.amazon.awssdk.services.glue.model.IntegrationConflictOperationException;
import software.amazon.awssdk.services.glue.model.IntegrationQuotaExceededException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;
import software.amazon.awssdk.services.glue.model.ResourceNotReadyException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.ThrottlingException;

/**
 * Used by {@link ThrottlingInvoker} to determine which LarkBase exceptions are thrown for throttling.
 */
public class BaseExceptionFilter
        implements ThrottlingInvoker.ExceptionFilter
{
    private static final Logger logger = LoggerFactory.getLogger(BaseExceptionFilter.class);

    public static final BaseExceptionFilter EXCEPTION_FILTER = new BaseExceptionFilter();

    public BaseExceptionFilter()
    {
    }

    @Override
    public boolean isMatch(Exception ex)
    {
        // Check AWS SDK throttling exceptions first (before checking message)
        if (ex instanceof TooManyRequestsException ||
                ex instanceof ConcurrentModificationException ||
                ex instanceof ConcurrentRunsExceededException ||
                ex instanceof ConflictException ||
                ex instanceof IntegrationConflictOperationException ||
                ex instanceof IntegrationQuotaExceededException ||
                ex instanceof OperationTimeoutException ||
                ex instanceof ResourceNotReadyException ||
                ex instanceof ResourceNumberLimitExceededException ||
                ex instanceof ThrottlingException) {
            logger.info("Throttling detected: {}", ex.getClass().getSimpleName());
            return true;
        }

        String message = ex.getMessage();
        if (message == null) {
            return false;
        }

        // Direct throttling from Lark
        if (message.contains("1254290") || message.contains("TooManyRequest")) {
            logger.info("Throttling detected: Too many requests");
            return true;
        }

        // Write conflict that might need throttling
        if (message.contains("1254291") || message.contains("Write conflict")) {
            logger.info("Throttling detected: Write conflict");
            return true;
        }

        // Timeout may be due to busy server
        if (message.contains("1255040") || message.contains("Request timed out")) {
            logger.info("Throttling detected: Request timeout");
            return true;
        }

        // Data not ready, might be due to busy server
        if (message.contains("1254607") || message.contains("Data not ready")) {
            logger.info("Throttling detected: Data not ready");
            return true;
        }

        // Base is copying
        if (message.contains("1254036") || message.contains("Base is copying")) {
            logger.info("Throttling detected: Base is copying");
            return true;
        }

        return false;
    }
}
