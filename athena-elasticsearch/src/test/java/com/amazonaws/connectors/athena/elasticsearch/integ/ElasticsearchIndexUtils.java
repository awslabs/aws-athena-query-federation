/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.connectors.athena.elasticsearch.integ;

import com.amazonaws.connectors.athena.elasticsearch.AwsRestHighLevelClient;
import com.amazonaws.connectors.athena.elasticsearch.AwsRestHighLevelClientFactory;
import com.amazonaws.connectors.athena.elasticsearch.ElasticsearchDomainMapProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Used to create Elasticsearch indices and insert documents for use with the Integration Tests.
 */
public class ElasticsearchIndexUtils implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchIndexUtils.class);

    private final AwsRestHighLevelClient client;
    private final String index;

    private Integer requestId;

    public ElasticsearchIndexUtils(String domainName, String index) {
        AwsRestHighLevelClientFactory clientFactory = new AwsRestHighLevelClientFactory(true);
        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(true);
        Optional<String> endpoint = Optional.ofNullable(domainMapProvider.getDomainMap(null).get(domainName));
        this.client = clientFactory.getOrCreateClient(endpoint.orElseThrow(() ->
                new RuntimeException("Encountered null endpoint trying to get client.")));
        this.index = index;
        this.requestId = 1;
    }

    /**
     * Add/Create a document in an index. The initial document inserted will also create the index.
     * @param document A map containing the data to be inserted.
     */
    public void addDocument(Map<String, Object> document)
    {
        logger.info("Updating index: {}, document: {}", index, document);
        try {
            IndexRequest request = new IndexRequest(index)
                    .id(requestId.toString())
                    .source(document)
                    .opType(requestId == 1 ? DocWriteRequest.OpType.CREATE : DocWriteRequest.OpType.INDEX);
            client.index(request, RequestOptions.DEFAULT);
            requestId++;
        }
        catch (IOException e) {
            throw new RuntimeException("Error adding document: " + e.getMessage(), e);
        }
    }

    /**
     * Closes the client to free its resources.
     */
    public void close()
    {
        try {
            client.close();
        }
        catch (IOException e) {
            logger.error("Unable to close client: ", e);
        }
    }
}
