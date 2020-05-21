/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * This class creates a REST client injected with an endpoint and credentials.
 */
public class AwsRestHighLevelClientFactory
{
    private static final Logger logger = LoggerFactory.getLogger(AwsRestHighLevelClientFactory.class);

    private final boolean useDefaultCredentials;
    private final String username;
    private final String password;

    /**
     * Constructs a new client factory (using a builder) that will create clients injected with credentials.
     * @param builder is used to initialize the client factory.
     */
    private AwsRestHighLevelClientFactory(Builder builder)
    {
        this.useDefaultCredentials = builder.useDefaultCredentials;
        this.username = builder.username;
        this.password = builder.password;
    }

    /**
     * Gets an Elasticsearch REST client injected with credentials.
     * @param endpoint is the Elasticsearch cluster's domain endpoint
     *                 (e.g. search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com).
     * @return an Elasticsearch REST client. If useDefaultCredentials = true, the client is injected
     *          with AWS credentials. If useDefaultCredentials = false and username/password are not
     *          empty, it is injected with username/password credentials. Otherwise a default client
     *          with no credentials is returned.
     */
    public AwsRestHighLevelClient getClient(String endpoint)
    {
        logger.info("getClient - enter: " + endpoint);

        if (useDefaultCredentials) {
            logger.info("Client injected with AWS credentials");

            return new AwsRestHighLevelClient.Builder(endpoint)
                    .withCredentials(new DefaultAWSCredentialsProviderChain()).build();
        }
        else if (!username.isEmpty() && !password.isEmpty()) {
            logger.info("Client injected with username/password credentials");

            return new AwsRestHighLevelClient.Builder(endpoint)
                    .withCredentials(username, password).build();
        }

        // Default client.
        return new AwsRestHighLevelClient.Builder(endpoint).build();
    }

    /**
     * Gets a default client factory that will create Elasticsearch REST clients injected with AWS Credentials.
     * @return a new default client factory.
     */
    public static AwsRestHighLevelClientFactory defaultFactory()
    {
        return new Builder().build();
    }

    /**
     * A builder for the AwsRestHighLevelClientFactory class.
     */
    public static class Builder
    {
        private boolean useDefaultCredentials;
        private String username;
        private String password;

        /**
         * A constructor for the builder. As a default behaviour, it sets the client factory to use AWS credentials
         * when constructing the client.
         */
        public Builder()
        {
            useDefaultCredentials = true;
            username = "";
            password = "";
        }

        /**
         * Sets the client factory to use username/password credentials from Amazon Secrets Manager when constructing
         * the client.
         * @param secretName is the name of the secret.
         * @return this.
         */
        public Builder withSecretCredentials(String secretName)
        {
            logger.info("withSecretCredentials - enter: " + secretName);

            if (secretName.isEmpty()) {
                logger.warn("Secret Name is empty.");
                return this;
            }

            AWSSecretsManager awsSecretsManager = AWSSecretsManagerClientBuilder.standard()
                    .withCredentials(new DefaultAWSCredentialsProviderChain()).build();

            try {
                GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
                GetSecretValueResult getSecretValueResult;
                getSecretValueResult = awsSecretsManager.getSecretValue(getSecretValueRequest);
                String secretString;

                if (getSecretValueResult.getSecretString() != null) {
                    secretString = getSecretValueResult.getSecretString();
                }
                else {
                    secretString = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
                }

                Map<String, String> secrets = new ObjectMapper().readValue(secretString, HashMap.class);
                if (secrets.size() == 1) {
                    // Secret was saved as: { <username>: <password> }
                    secrets.forEach((key, value) ->
                    {
                        username = key;
                        password = value;
                    });
                }
                else {
                    // Secret was saved as: { "username": <username>, "password": <password> }
                    username = secrets.get("username");
                    password = secrets.get("password");
                }
                useDefaultCredentials = false;
            }
            catch (Exception error) {
                logger.error("Error accessing Secrets Manager:", error);
            }
            finally {
                awsSecretsManager.shutdown();
            }

            return this;
        }

        /**
         * Builds the client factory.
         * @return a new client factory injected with the builder.
         */
        public AwsRestHighLevelClientFactory build()
        {
            return new AwsRestHighLevelClientFactory(this);
        }
    }
}
