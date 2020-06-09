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

    private final boolean useAwsCredentials;

    /**
     * credentialsPattern is used for parsing out the username/password credentials out of the endpoint with the
     * format: "username@password:" (e.g. if the endpoint is http://myusername@mypassword:www.google.com, then this
     * pattern will be used to extract the following credentials string: "myusername@mypassword:").
     */
    private static final Pattern credentialsPattern = Pattern.compile("[^/@]+@[^:]+:");

    /**
     * Constructs a new client factory (using a builder) that will create clients injected with credentials.
     * @param useAwsCredentials if true the factory create clients injected with AWC credentials.
     *                              If false, it will create clients injected with username/password credentials.
     */
    protected AwsRestHighLevelClientFactory(boolean useAwsCredentials)
    {
        this.useAwsCredentials = useAwsCredentials;
    }

    /**
     * Gets an Elasticsearch REST client injected with credentials.
     * @param endpoint is the Elasticsearch instance endpoint. The latter may contain username/password credentials
     *                 for Elasticsearch services that are external to Amazon.
     *                 Examples:
     *                 1) https://search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com
     *                 2) http://myusername@mypassword:www.google.com
     * @return an Elasticsearch REST client. If useAwsCredentials = true, the client is injected
     *          with AWS credentials. If useAwsCredentials = false and username/password are extracted using the
     *          credentialsPattern, it is injected with username/password credentials. Otherwise a default client
     *          with no credentials is returned.
     */
    public AwsRestHighLevelClient getClient(String endpoint)
    {
        if (useAwsCredentials) {
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

        logger.debug("Default client w/o credentials");

        // Default client w/o credentials.
        return new AwsRestHighLevelClient.Builder(endpoint).build();
    }
}
