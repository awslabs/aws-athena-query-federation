/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.util.Map;

/**
 * Utility class for handling credential provider functionality.
 */
public final class DataLakeGen2CredentialProviderUtils
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private DataLakeGen2CredentialProviderUtils()
    {
    }

    /**
     * Gets the credentials provider based on the secret configuration.
     * If OAuth is configured, returns DataLakeGen2 OAuth credentials provider.
     * Otherwise, falls back to default username/password credentials.
     */
    public static CredentialsProvider getCredentialProvider(String secretName, CachableSecretsManager secretsManager)
    {
        if (StringUtils.isNotBlank(secretName)) {
            try {
                String secretString = secretsManager.getSecret(secretName);
                Map<String, String> secretMap = OBJECT_MAPPER.readValue(secretString, Map.class);

                // Check if OAuth is configured
                if (DataLakeGen2OAuthCredentialsProvider.isOAuthConfigured(secretMap)) {
                    return new DataLakeGen2OAuthCredentialsProvider(secretName, secretMap, secretsManager);
                }

                // Fall back to default credentials if OAuth is not configured
                return new DefaultCredentialsProvider(secretString);
            }
            catch (IOException ioException) {
                throw new AthenaConnectorException("Could not deserialize RDS credentials into HashMap: ",
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).errorMessage(ioException.getMessage()).build());
            }
        }

        return null;
    }
}
