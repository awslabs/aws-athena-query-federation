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
package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.kms.model.KmsException;

public abstract class ConnectorExceptionHandler
{
    protected ConnectorExceptionHandler()
    {
        // Default constructor for subclasses
    }

    public void handle(ConnectorRunnable runnable) throws Exception
    {
        try {
            runnable.run();
        }
        catch (KmsException kmsException) {
            throw new AthenaConnectorException(kmsException.getMessage(),
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        catch (Exception e) {
            throw handleConnectorException(e);
        }
    }

    @FunctionalInterface
    public interface ConnectorRunnable
    {
        void run() throws Exception;
    }

    protected abstract Exception handleConnectorException(Exception e);
}
