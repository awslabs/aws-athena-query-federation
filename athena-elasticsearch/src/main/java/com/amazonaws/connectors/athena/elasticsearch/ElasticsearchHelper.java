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

import com.amazonaws.services.elasticsearch.AWSElasticsearch;
import com.amazonaws.services.elasticsearch.AWSElasticsearchClientBuilder;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsRequest;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsResult;
import com.amazonaws.services.elasticsearch.model.DomainInfo;
import com.amazonaws.services.elasticsearch.model.ElasticsearchDomainStatus;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesRequest;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesResult;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides interfaces related to parsing of the domain-names and their associated endpoints.
 * Additionally, it provides an interface for getting a REST client factory used for creating clients that can
 * communicate with a specific Elasticsearch endpoint.
 */
class ElasticsearchHelper
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchHelper.class);

    // Env. variable that indicates whether the service is with Amazon ES Service (true) and thus the domain-
    // names and associated endpoints can be auto-discovered via the AWS ES SDK. Or, the Elasticsearch service
    // is external to Amazon (false), and the domain_mapping environment variable should be used instead.
    private static final String AUTO_DISCOVER_ENDPOINT = "auto_discover_endpoint";
    private boolean autoDiscoverEndpoint;

    // Splitter for inline map properties extracted from the domain_mapping environment variable.
    private final Splitter.MapSplitter domainSplitter = Splitter.on(",").trimResults().withKeyValueSeparator("=");

    protected ElasticsearchHelper()
    {
        this.autoDiscoverEndpoint = getEnv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");
    }

    /**
     * Get an environment variable using System.getenv().
     * @param var is the environment variable.
     * @return the contents of the environment variable or an empty String if it's not defined.
     */
    protected final String getEnv(String var)
    {
        String result = System.getenv(var);

        return result == null ? "" : result;
    }

    /**
     * Gets the domainMap with domain-names and corresponding endpoints retrieved from the AWS ES SDK.
     * @param domainStatusList is a list of status objects returned by a listDomainNames request to the AWS ES SDK.
     * @return populated domainMap with domain-names and corresponding endpoints.
     */
    private Map<String, String> getDomainMapping(List<ElasticsearchDomainStatus> domainStatusList)
    {
        logger.info("setDomainMapping(List<>) - enter");

        Map<String, String> domainMap = new HashMap<>();

        for (ElasticsearchDomainStatus domainStatus : domainStatusList) {
            domainMap.put(domainStatus.getDomainName(), "https://" + domainStatus.getEndpoint());
        }

        return domainMap;
    }

    /**
     * Gets the domain-mapping from either the domain_mapping environment variable
     * (auto_discover_endpoint is false), or from the AWS ES SDK (auto_discover_endpoint is true).
     * @param domainMapping is the contents of the domain_mapping environment variable with secrets already resolved.
     *                      This parameter will be ignored (and should be empty) when auto_discover_endpoint=true.
     * @return populated domainMap with domain-names and corresponding endpoints.
     * @throws RuntimeException
     */
    protected Map<String, String> getDomainMapping(String domainMapping)
            throws RuntimeException
    {
        logger.info("setDomainMapping - enter");

        if (autoDiscoverEndpoint) {
            // Get domain mapping via the AWS ES SDK (1.x).
            // NOTE: Gets called at construction and each call to doListSchemaNames() when autoDiscoverEndpoint is true.

            // AWS ES Client for retrieving domain mapping info from the Amazon Elasticsearch Service.
            AWSElasticsearch awsEsClient = AWSElasticsearchClientBuilder.defaultClient();

            try {
                ListDomainNamesResult listDomainNamesResult = awsEsClient.listDomainNames(new ListDomainNamesRequest());
                List<String> domainNames = new ArrayList<>();
                for (DomainInfo domainInfo : listDomainNamesResult.getDomainNames()) {
                    domainNames.add(domainInfo.getDomainName());
                }

                DescribeElasticsearchDomainsRequest describeDomainsRequest = new DescribeElasticsearchDomainsRequest();
                describeDomainsRequest.setDomainNames(domainNames);
                DescribeElasticsearchDomainsResult describeDomainsResult =
                        awsEsClient.describeElasticsearchDomains(describeDomainsRequest);

                return getDomainMapping(describeDomainsResult.getDomainStatusList());
            }
            catch (Exception error) {
                logger.error("Error getting list of domain names:", error);
            }
            finally {
                awsEsClient.shutdown();
            }
        }
        else {
            // Get domain mapping from environment variable.
            if (!domainMapping.isEmpty()) {
                return domainSplitter.split(domainMapping);
            }
        }

        throw new RuntimeException("Unable to extract list of domain names and endpoints.");
    }

    /**
     * Gets a client factory that can create an Elasticsearch REST client injected with credentials.
     * @return a new client factory.
     */
    protected AwsRestHighLevelClientFactory getClientFactory()
    {
        logger.info("getClientFactory - enter");

        if (autoDiscoverEndpoint) {
            // Client factory for clients injected with AWS credentials.
            return AwsRestHighLevelClientFactory.defaultFactory();
        }
        else {
            // Client factory for clients injected with username/password credentials.
            return new AwsRestHighLevelClientFactory.Builder().withSecretCredentials().build();
        }
    }
}
