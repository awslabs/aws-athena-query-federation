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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Splitter;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
     * For regular index name (table name in Athena), the index name equals to the actual index in ES.
     *
     * For data stream, data stream index does not equals to actual index names, ES created and managed indices for a data stream based on time. therefore, if we use data stream name to find mapping, we will not able to find it.
     * In addition, data stream can contains multiple indices, it is hard to aggregate all the mapping into single giant mapping, we pick up the first index mapping we can find for data stream.
     *
     * Example : non data stream : "book"
     * {
     *  "book" : {
     *    "mappings" : { .. }
     * }
     *
     * Example: data stream : "datastream"
     * {
     *  ".ds-datastream_test1-000001" : {
     *    "mappings" : {....}
     *   },
     *  ".ds-datastream_test1-12345678" : {
     *    "mappings" : {....}
     *   }
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
        // non data stream mappingMetadata will return value because index name is same as underlying index used by ES.
        MappingMetadata mappingMetadata = mappingsResponse.mappings().get(index);
        // data stream case, index name is not same as underlying index managed by ES.
        if (mappingMetadata == null) {
            logger.info("Get first available mapping for data stream, data stream name: {}", index);
            Map.Entry<String, MappingMetadata> dsmapping = mappingsResponse.mappings().entrySet()
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(String.format("Could not find mapping for data stream name: %s", index)));
            mappingMetadata = dsmapping.getValue();
        }

        return (LinkedHashMap<String, Object>) mappingMetadata.getSourceAsMap();
    }

    /**
     * Retrieves cluster-health information for shards associated with the specified index. The request will time out
     * if no results are returned after a period of time indicated by timeout.
     * @param index is used to restrict the request to a specified index.
     * @param timeout is the command timeout period in seconds.
     * @return a set of shard ids for the specified index.
     * @throws IOException if an error occurs while sending the request to the Elasticsearch instance.
     * @throws RuntimeException if the request times out, or no active-primary shards are present.
     */
    public Set<Integer> getShardIds(String index, long timeout)
            throws RuntimeException, IOException
    {
        ClusterHealthRequest request = new ClusterHealthRequest(index)
                .timeout(new TimeValue(timeout, TimeUnit.SECONDS));
        // Set request to shard-level details
        request.level(ClusterHealthRequest.Level.SHARDS);

        ClusterHealthResponse response = cluster().health(request, RequestOptions.DEFAULT);

        if (response.isTimedOut()) {
            throw new RuntimeException("Request timed out for index (" + index + ").");
        }
        else if (response.getActiveShards() == 0) {
            throw new RuntimeException("There are no active shards for index (" + index + ").");
        }
        else if (response.getStatus() == ClusterHealthStatus.RED) {
            throw new RuntimeException("Request aborted for index (" + index +
                    ") due to cluster's status (RED) - One or more primary shards are unassigned.");
        }
        else if (!response.getIndices().containsKey(index)) {
            throw new RuntimeException("Request has an invalid index (" + index + ").");
        }

        return response.getIndices().get(index).getShards().keySet();
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
