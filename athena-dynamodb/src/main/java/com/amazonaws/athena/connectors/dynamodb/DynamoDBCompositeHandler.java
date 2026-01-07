/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose DynamoDBMetadataHandler and DynamoDBRecordHandler.
 */
public class DynamoDBCompositeHandler
        extends CompositeHandler
{
    public DynamoDBCompositeHandler()
    {
        super(new DynamoDBMetadataHandler(new EnvironmentProperties().createEnvironment()), new DynamoDBRecordHandler(new EnvironmentProperties().createEnvironment()));
    }

    @Override
    public Exception handleException(Exception e)
    {
        return handleDynamoDBException(super.handleException(e));
    }

    /**
     * Transforms DynamoDB exceptions into AthenaConnectorException with appropriate error code.
     * Returns other exceptions unchanged.
     *
     * @param e The exception to handle
     * @return The transformed exception or original exception
     */
    public static Exception handleDynamoDBException(Exception e)
    {
        if (e instanceof DynamoDbException) {
            return new AthenaConnectorException(
                    e.getMessage(),
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString())
                            .build()
            );
        }
        return e;
    }
}
