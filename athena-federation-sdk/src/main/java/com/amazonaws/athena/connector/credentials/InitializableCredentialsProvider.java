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

import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;

import java.util.Map;

/**
 * Extended contract for credentials providers that require initialization with configuration from AWS Secrets Manager.
 * Implementations must be initialized before use and may support various authentication methods including OAuth,
 * username/password, or other configurable authentication schemes.
 */
public interface InitializableCredentialsProvider extends CredentialsProvider
{
    /**
     * Initializes this credential provider with the given configuration.
     * Must be called exactly once before any calls to getCredential().
     *
     * @param secretName The name of the secret in AWS Secrets Manager
     * @param secretMap The secret configuration containing authentication parameters
     * @param secretsManager The secrets manager instance for retrieving and updating secrets
     */
    void initialize(String secretName, Map<String, String> secretMap, CachableSecretsManager secretsManager);
}
