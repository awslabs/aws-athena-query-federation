/*-
 * #%L
 * athena-example
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
package com.amazonaws.connectors.athena.elasticsearch;

// Elasticsearch APIs
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

// Apache APIs
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;

// AWS Credentials APIs
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

// Guava
import com.google.common.base.Splitter;

// Common
import java.util.List;

/**
 * AwsRestHighLevelClient
 *
 * This class creates a new client injected with AWS Credentials.
 */
public class AwsRestHighLevelClient
{
    private final String endpoint;
    private final Splitter domainSplitter = Splitter.on(".");
    private final AWSCredentialsProvider credentialsProvider;
    private final AWS4Signer signer;
    private final HttpRequestInterceptor interceptor;

    public AwsRestHighLevelClient(String domainEndpoint) {
        /**
         * domainEndpoint:
         * search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com
         *
         * domainTokens:
         * 0 = "search-movies-ne3fcqzfipy6jcrew2wca6kyqu"
         * 1 = "us-east-1"
         * 2 = "es"
         * 3 = "amazonaws"
         * 4 = "com"
         */
        List<String> domainTokens = domainSplitter.splitToList(domainEndpoint);
        endpoint = "https://" + domainEndpoint;
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        signer = new AWS4Signer();
        signer.setRegionName(domainTokens.get(1));
        signer.setServiceName(domainTokens.get(2));
        interceptor = new AWSRequestSigningApacheInterceptor(signer.getServiceName(), signer, credentialsProvider);
    }

    public RestHighLevelClient build() {
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(endpoint)).
                setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor)));
    }
}
