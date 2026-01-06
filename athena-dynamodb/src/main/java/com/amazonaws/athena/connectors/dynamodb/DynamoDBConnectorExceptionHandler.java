/*-
 * #%L
 * Amazon Athena DynamoDB Connector
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.ConnectorExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

public class DynamoDBConnectorExceptionHandler extends ConnectorExceptionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBConnectorExceptionHandler.class);
    public DynamoDBConnectorExceptionHandler()
    {
        super();
    }

    @Override
    protected Exception handleConnectorException(Exception e)
    {
        if (e instanceof DynamoDbException) {
            logger.info("Handling DynamoDbException as AthenaConnectorException", e);
            return new AthenaConnectorException(e.getMessage(),
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        return e;
    }
}
