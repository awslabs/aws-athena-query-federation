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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class creates a REST client injected with an endpoint and credentials.
 */
public class AwsRestHighLevelClientFactory
{
    private static final Logger logger = LoggerFactory.getLogger(AwsRestHighLevelClientFactory.class);

    private final boolean useDefaultCredentials;
    private static final Pattern credentialsPattern = Pattern.compile("[^/@]+@[^:]+:");

    /**
     * Constructs a new client factory (using a builder) that will create clients injected with credentials.
     * @param builder is used to initialize the client factory.
     */
    private AwsRestHighLevelClientFactory(Builder builder)
    {
        this.useDefaultCredentials = builder.useDefaultCredentials;
    }

    /**
     * Gets an Elasticsearch REST client injected with credentials.
     * @param endpoint is the Elasticsearch instance endpoint. The latter may contain username/password credentials
     *                 for Elasticsearch services that are external to Amazon.
     *                 Examples:
     *                 1) https://search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com
     *                 2) http://username@password:www.google.com
     * @return an Elasticsearch REST client. If useDefaultCredentials = true, the client is injected
     *          with AWS credentials. If useDefaultCredentials = false and username/password are not
     *          empty, it is injected with username/password credentials. Otherwise a default client
     *          with no credentials is returned.
     */
    public AwsRestHighLevelClient getClient(String endpoint)
    {
        logger.info("getClient - enter");

        if (useDefaultCredentials) {
            return new AwsRestHighLevelClient.Builder(endpoint)
                    .withCredentials(new DefaultAWSCredentialsProviderChain()).build();
        }
        else {
            Matcher credentials = credentialsPattern.matcher(endpoint);
            if (credentials.find()) {
                String usernameAndPassword = credentials.group();
                String username = usernameAndPassword.substring(0, usernameAndPassword.indexOf("@"));
                String password = usernameAndPassword.substring(usernameAndPassword.indexOf("@") + 1,
                        usernameAndPassword.lastIndexOf(":"));
                String finalEndpoint = endpoint.replace(usernameAndPassword, "");

                return new AwsRestHighLevelClient.Builder(finalEndpoint).withCredentials(username, password).build();
            }
        }

        logger.info("Default client w/o credentials");

        // Default client w/o credentials.
        return new AwsRestHighLevelClient.Builder(endpoint).build();
    }

    /**
     * Gets a default client factory that will create Elasticsearch REST clients injected with AWS Credentials.
     * @return a new default client factory.
     */
    public static AwsRestHighLevelClientFactory defaultFactory()
    {
        logger.info("defaultFactory - enter");

        return new Builder().build();
    }

    /**
     * A builder for the AwsRestHighLevelClientFactory class.
     */
    public static class Builder
    {
        private boolean useDefaultCredentials;

        /**
         * A constructor for the builder. As a default behaviour, it sets the client factory to use AWS credentials
         * when constructing the client.
         */
        public Builder()
        {
            useDefaultCredentials = true;
        }

        /**
         * Sets the client factory to use username/password credentials from Amazon Secrets Manager when constructing
         * the client.
         * @return this.
         */
        public Builder withSecretCredentials()
        {
            logger.info("withSecretCredentials - enter");

            useDefaultCredentials = false;

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
