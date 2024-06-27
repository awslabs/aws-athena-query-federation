/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connector.integ.providers;

import com.amazonaws.athena.connector.integ.data.SecretsManagerCredentials;
import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Responsible for providing user credentials from SecretsManager.
 */
public class SecretsManagerCredentialsProvider
{
    private static final String TEST_CONFIG_SECRETS_MANAGER_SECRET = "secrets_manager_secret";

    private SecretsManagerCredentialsProvider() {}

    /**
     * Gets the SecretManager credentials obtained using a secret name stored in the test-config.json file.
     * @param testConfig Contains the test configurations from the test-config.json file.
     * @return Optional credentials object, or empty Optional if the secrets_manager_secret attribute is not in the
     * configuration file or is empty.
     * @throws RuntimeException Error encountered attempting to parse the json string returned from SecretsManager.
     */
    public static Optional<SecretsManagerCredentials> getCredentials(TestConfig testConfig)
            throws RuntimeException
    {
        Optional<String> secretsManagerSecret = testConfig.getStringItem(TEST_CONFIG_SECRETS_MANAGER_SECRET);

        if (secretsManagerSecret.isPresent()) {
            String secret = secretsManagerSecret.get();
            SecretsManagerClient secretsManager = SecretsManagerClient.create();
            try {
                GetSecretValueResponse secretValueResult = secretsManager.getSecretValue(GetSecretValueRequest.builder()
                        .secretId(secret)
                        .build());
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, String> credentials = objectMapper.readValue(secretValueResult.secretString(),
                        HashMap.class);
                return Optional.of(new SecretsManagerCredentials(secret, credentials.get("username"),
                        credentials.get("password"),  secretValueResult.arn()));
            }
            catch (IOException e) {
                throw new RuntimeException(String.format("Unable to parse SecretsManager secret (%s): %s",
                        secret, e.getMessage()), e);
            }
        }

        return Optional.empty();
    }
}
