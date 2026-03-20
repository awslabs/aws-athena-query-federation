/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.CredentialsProviderFactory;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.util.Map;

/**
 * Builds a {@link CredentialsProvider} for Snowflake from an already-resolved secret string.
 * OAuth uses {@link CredentialsProviderFactory} with {@link SnowflakeOAuthCredentialsProvider} (same pattern as
 * Handlers should call {@link com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler#getSecret}
 * and pass the result here from {@link com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler#createCredentialsProvider}.
 */
public final class SnowflakeConnectionCredentials
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SnowflakeConnectionCredentials()
    {
    }

    public static CredentialsProvider createProvider(
            String secretName,
            String secretString,
            CachableSecretsManager secretsManager,
            AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        try {
            Map<String, String> secretMap = OBJECT_MAPPER.readValue(secretString, Map.class);
            SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(secretMap);
            SnowflakeAuthUtils.validateCredentials(secretMap, authType);
            switch (authType) {
                case OAUTH:
                    return CredentialsProviderFactory.createCredentialProvider(
                            secretName,
                            secretsManager,
                            new SnowflakeOAuthCredentialsProvider(),
                            requestOverrideConfiguration);
                case SNOWFLAKE_JWT:
                    return new SnowflakeKeyPairCredentialsProvider(secretMap);
                case SNOWFLAKE:
                default:
                    return new DefaultCredentialsProvider(secretString);
            }
        }
        catch (IOException ioException) {
            throw new AthenaConnectorException("Could not deserialize Snowflake credentials: ",
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                            .errorMessage(ioException.getMessage())
                            .build());
        }
    }
}
