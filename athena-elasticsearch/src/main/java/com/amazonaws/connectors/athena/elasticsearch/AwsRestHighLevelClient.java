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
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

// Apache APIs
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;

// AWS Credentials APIs
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;

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
        extends RestHighLevelClient
{
    public AwsRestHighLevelClient(RestClientBuilder builder)
    {
        super(builder);
    }

    public static class Builder
    {
        private final String endpoint;
        private final Splitter domainSplitter = Splitter.on(".");
        private final AWS4Signer signer;
        private final RestClientBuilder clientBuilder;

        public Builder(String domainEndpoint)
        {
            /**
             * domainEndpoint:
             * search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com
             *
             * domainSplits:
             * 0 = "search-movies-ne3fcqzfipy6jcrew2wca6kyqu"
             * 1 = "us-east-1"
             * 2 = "es"
             * 3 = "amazonaws"
             * 4 = "com"
             */
            List<String> domainSplits = domainSplitter.splitToList(domainEndpoint);
            this.endpoint = "https://" + domainEndpoint;
            this.signer = new AWS4Signer();
            this.signer.setRegionName(domainSplits.get(1));
            this.signer.setServiceName("es");
            clientBuilder = RestClient.builder(HttpHost.create(this.endpoint));
        }

        public Builder setCredentials(AWSCredentialsProvider credentialsProvider)
        {
            HttpRequestInterceptor interceptor =
                    new AWSRequestSigningApacheInterceptor(signer.getServiceName(), signer, credentialsProvider);

            clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.
                    addInterceptorLast(interceptor));

            return this;
        }

        public Builder setCredentials(String username, String password)
        {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.
                    setDefaultCredentialsProvider(credentialsProvider));

            return this;
        }

        public AwsRestHighLevelClient build() {
            return new AwsRestHighLevelClient(clientBuilder);
        }
    }
}
