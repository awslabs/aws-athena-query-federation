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
import java.util.Set;

class ElasticsearchHelper
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchHelper.class);

    // Env. variable that indicates whether the service is with Amazon ES Service (true) and thus the domain-
    // names and associated endpoints can be auto-discovered via the AWS ES SDK. Or, the Elasticsearch service
    // is external to Amazon (false), and the domain_mapping environment variable should be used instead.
    private static final String AUTO_DISCOVER_ENDPOINT = "auto_discover_endpoint";
    private boolean autoDiscoverEndpoint;

    // Env. variable that holds the mappings of the domain-names to their respective endpoints. The contents of
    // this environment variable is fed into the domainSplitter to populate the domainMap where the key = domain-name,
    // and the value = endpoint.
    private static final String DOMAIN_MAPPING = "domain_mapping";
    private String domainMapping;

    // Splitter for inline map properties extracted from the DOMAIN_MAPPING.
    private final Splitter.MapSplitter domainSplitter = Splitter.on(",").trimResults().withKeyValueSeparator("=");

    // A Map of the domain-names and their respective endpoints.
    private final Map<String, String> domainMap = new HashMap<>();

    // Used when autoDiscoverEndpoint is false to create a client injected with username/password credentials
    // extracted from Amazon Secrets Manager.
    private static final String SECRET_NAME = "secret_name";
    private String secretName;

    // Singleton instance variable.
    private static ElasticsearchHelper instance = null;

    private ElasticsearchHelper()
    {
        this.autoDiscoverEndpoint = getEnv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");
        this.secretName = getEnv(SECRET_NAME);
        this.domainMapping = getEnv(DOMAIN_MAPPING);

        setDomainMapping();
    }

    protected static ElasticsearchHelper getInstance()
    {
        if (instance == null) {
            synchronized (ElasticsearchHelper.class) {
                if (instance == null) {
                    instance = new ElasticsearchHelper();
                }
            }
        }

        return instance;
    }

    /**
     * Get an environment variable using System.getenv().
     * @param var is the environment variable.
     * @return the contents of the environment variable or an empty String if it's not defined.
     */
    private final String getEnv(String var)
    {
        String result = System.getenv(var);

        return result == null ? "" : result;
    }

    /**
     * @return the boolean value of autoDiscoverEndpoint.
     */
    protected final boolean isAutoDiscoverEndpoint()
    {
        return autoDiscoverEndpoint;
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
            return new AwsRestHighLevelClientFactory.Builder().withSecretCredentials(secretName).build();
        }
    }

    /**
     * Sets the domainMap with domain-names and corresponding endpoints retrieved from the AWS ES SDK.
     * @param domainStatusList is a list of status objects returned by a listDomainNames request to the AWS ES SDK.
     */
    private final void setDomainMapping(List<ElasticsearchDomainStatus> domainStatusList)
    {
        logger.info("setDomainMapping(List<>) - enter");

        domainMap.clear();
        for (ElasticsearchDomainStatus domainStatus : domainStatusList) {
            domainMap.put(domainStatus.getDomainName(), domainStatus.getEndpoint());
        }

        logger.info("setDomainMapping(List<>) - exit: " + domainMap);
    }

    /**
     * Sets the domainMap with domain-names and corresponding endpoints stored in a Map object.
     * @param mapping is a map of domain-names and their corresponding endpoints.
     */
    protected final void setDomainMapping(Map<String, String> mapping)
    {
        logger.info("setDomainMapping(Map<>) - enter");

        domainMap.clear();
        domainMap.putAll(mapping);

        logger.info("setDomainMapping(Map<>) - exit: " + domainMap);
    }

    /**
     * Populates the domainMap with domain-mapping from either the domain_mapping environment variable
     * (auto_discover_endpoint is false), or from the AWS ES SDK (auto_discover_endpoint is true).
     * This method is called from the constructor(s) and from doListSchemaNames(). The call from the latter is used to
     * refresh the domainMap with the underlying assumption that domain(s) could have been added or deleted.
     */
    protected void setDomainMapping()
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

                setDomainMapping(describeDomainsResult.getDomainStatusList());
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
                setDomainMapping(domainSplitter.split(domainMapping));
            }
        }

        logger.info("setDomainMapping - exit");
    }

    /**
     * Gets a list of domain names.
     * @return a list of domain-names stored in the domainMap.
     */
    protected final Set<String> getDomainList()
    {
        return domainMap.keySet();
    }

    /**
     * Gets the domain's endpoint.
     * @param domainName is the name associated with the endpoint.
     * @return a String containing the endpoint associated with the domainName, or an empty String if the domain-
     * name doesn't exist in the map.
     */
    protected final String getDomainEndpoint(String domainName)
    {
        String endpoint = "";

        if (domainMap.containsKey(domainName)) {
            endpoint = domainMap.get(domainName);
        }

        return endpoint;
    }
}
