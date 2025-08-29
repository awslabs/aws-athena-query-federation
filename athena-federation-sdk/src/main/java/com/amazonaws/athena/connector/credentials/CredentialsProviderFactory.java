/*-
 * #%L
 * athena-federation-sdk
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
package com.amazonaws.athena.connector.credentials;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Factory class for handling credentials provider creation.
 * This class can be used by any connector that needs to support both
 * OAuth and username/password authentication.
 */
public final class CredentialsProviderFactory
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private CredentialsProviderFactory()
    {
    }

    /**
     * Creates a credentials provider based on the secret configuration.
     * If OAuth is configured (determined by the provider's isOAuthConfigured method), creates an instance
     * of the specified OAuth provider. Otherwise, creates a default username/password credentials provider.
     *
     * @param secretName The name of the secret in AWS Secrets Manager
     * @param secretsManager The secrets manager instance
     * @param oAuthProviderClass The class of the OAuth provider to instantiate (must extend OAuthCredentialsProvider)
     * @return A new CredentialsProvider instance based on the secret configuration
     * @throws AthenaConnectorException if there are errors deserializing the secret or creating the provider
     */
    public static <T extends OAuthCredentialsProvider> CredentialsProvider createCredentialProvider(
            String secretName,
            CachableSecretsManager secretsManager,
            Class<T> oAuthProviderClass)
    {
        if (StringUtils.isNotBlank(secretName)) {
            try {
                String secretString = secretsManager.getSecret(secretName);
                Map<String, String> secretMap = OBJECT_MAPPER.readValue(secretString, Map.class);

                // Create an instance of the OAuth provider
                T provider;
                try {
                    Constructor<T> constructor = oAuthProviderClass.getConstructor(
                            String.class, Map.class, CachableSecretsManager.class);
                    provider = constructor.newInstance(secretName, secretMap, secretsManager);
                }
                catch (ReflectiveOperationException e) {
                    throw new AthenaConnectorException("Failed to create OAuth provider: " + e.getMessage(),
                            ErrorDetails.builder()
                                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                                    .errorMessage(e.getMessage())
                                    .build());
                }

                // Check if OAuth is configured
                if (provider.isOAuthConfigured(secretMap)) {
                    return provider;
                }

                // Fall back to default credentials if OAuth is not configured
                return new DefaultCredentialsProvider(secretString);
            }
            catch (IOException ioException) {
                throw new AthenaConnectorException("Could not deserialize credentials into HashMap: ",
                        ErrorDetails.builder()
                                .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                                .errorMessage(ioException.getMessage())
                                .build());
            }
        }

        return null;
    }
}
