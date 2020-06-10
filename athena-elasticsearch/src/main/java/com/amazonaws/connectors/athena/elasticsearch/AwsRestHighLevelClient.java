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

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Splitter;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to create a new REST client injected with either AWS credentials or username/password credentials.
 */
public class AwsRestHighLevelClient
        extends RestHighLevelClient
{
    private static final Logger logger = LoggerFactory.getLogger(AwsRestHighLevelClient.class);

    /**
     * Constructs a new client (using a builder) injected with credentials.
     * @param builder is used to initialize the super class.
     */
    public AwsRestHighLevelClient(RestClientBuilder builder)
    {
        super(builder);
    }

    /**
     * Gets the indices from the domain.
     * @return a Set containing all the indices retrieved from the specified domain.
     * @throws IOException
     */
    public Set<String> getAliases()
            throws IOException
    {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        GetAliasesResponse getAliasesResponse = indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);
        return getAliasesResponse.getAliases().keySet();
    }

    /**
     * Gets the mapping for the specified index.
     * @param index is the index whose mapping will be retrieved.
     * @return a map containing all the mapping information for the specified index.
     * @throws IOException
     */
    public LinkedHashMap<String, Object> getMapping(String index)
            throws IOException
    {
        GetMappingsRequest mappingsRequest = new GetMappingsRequest();
        mappingsRequest.indices(index);
        GetMappingsResponse mappingsResponse = indices().getMapping(mappingsRequest, RequestOptions.DEFAULT);

        return (LinkedHashMap<String, Object>) mappingsResponse.mappings().get(index).sourceAsMap();
    }

    /**
     * Gets the Documents for the specified index and predicate.
     * @param request is the search request that includes the projection, predicate, batch size, and from position
     *                used for pagination of results.
     * @return the search response including all the document hits.
     * @throws IOException
     */
    public SearchResponse getDocuments(SearchRequest request)
            throws IOException
    {
        return search(request, RequestOptions.DEFAULT);
    }

    /**
     * Gets the Document from the search hit.
     * @param searchHit is the search hit containing the document source.
     * @return the Document as a Map object.
     */
    public Map<String, Object> getDocument(SearchHit searchHit)
    {
        return searchHit.getSourceAsMap();
    }

    /**
     * Shuts down the client.
     */
    public void shutdown()
    {
        try {
            close();
        }
        catch (IOException error) {
            logger.error("Unable to shutdown client:", error);
        }
    }

    /**
     * A builder for the AwsRestHighLevelClient class.
     */
    public static class Builder
    {
        private final String endpoint;
        private final RestClientBuilder clientBuilder;
        private final AWS4Signer signer;
        private final Splitter domainSplitter;

        /**
         * A constructor for the client builder.
         * @param endpoint is the cluster's endpoint and is injected into the builder.
         */
        public Builder(String endpoint)
        {
            this.endpoint = endpoint;
            this.clientBuilder = RestClient.builder(HttpHost.create(this.endpoint));
            this.signer = new AWS4Signer();
            this.domainSplitter = Splitter.on(".");
        }

        /**
         * Injects the client builder with AWS credentials.
         * @param credentialsProvider is the AWS credentials provider.
         * @return self.
         */
        public Builder withCredentials(AWSCredentialsProvider credentialsProvider)
        {
            /**
             * endpoint:
             * https://search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com
             *
             * domainSplits:
             * [0] = "https://search-movies-ne3fcqzfipy6jcrew2wca6kyqu"
             * [1] = "us-east-1"
             * [2] = "es"
             * [3] = "amazonaws"
             * [4] = "com"
             */
            List<String> domainSplits = domainSplitter.splitToList(endpoint);

            if (domainSplits.size() > 1) {
                signer.setRegionName(domainSplits.get(1));
                signer.setServiceName("es");
            }

            HttpRequestInterceptor interceptor =
                    new AWSRequestSigningApacheInterceptor(signer.getServiceName(), signer, credentialsProvider);

            clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                    .addInterceptorLast(interceptor));

            return this;
        }

        /**
         * Injects the client builder with username/password credentials.
         * @param username is the username used for the domain.
         * @param password is the password used for the domain.
         * @return self.
         */
        public Builder withCredentials(String username, String password)
        {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider));

            return this;
        }

        /**
         * Builds the client.
         * @return a new client injected with the builder.
         */
        public AwsRestHighLevelClient build()
        {
            return new AwsRestHighLevelClient(clientBuilder);
        }
    }
}
