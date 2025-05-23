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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.google.common.base.Splitter;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.elasticsearch.ElasticsearchClient;
import software.amazon.awssdk.services.elasticsearch.model.DescribeElasticsearchDomainsRequest;
import software.amazon.awssdk.services.elasticsearch.model.DescribeElasticsearchDomainsResponse;
import software.amazon.awssdk.services.elasticsearch.model.ListDomainNamesResponse;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides a method for creating a map between the domain-names and their associated endpoints.
 * The method of creating the map depends on the autoDiscoverEndpoint parameter passed in at construction. When
 * autoDiscoverEndpoint=true, the getDomainMapping() method will send list/describe commands via the AWS ES SDK
 * to create the map. When auto_discover_endpoint=false, the map will be derived from the domainMapping string
 * passed in as an argument.
 */
public class ElasticsearchDomainMapProvider
{
    // Splitter for inline map properties extracted from the domain_mapping environment variable.
    private static final Splitter.MapSplitter domainSplitter = Splitter.on(",").trimResults().withKeyValueSeparator("=");

    private static final String endpointPrefix = "https://";

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchDomainMapProvider.class);

    // Env. variable that indicates whether the service is with Amazon ES Service (true) and thus the domain-
    // names and associated endpoints can be auto-discovered via the AWS ES SDK. Or, the Elasticsearch service
    // is external to Amazon (false), and the domain_mapping environment variable should be used instead.
    private final boolean autoDiscoverEndpoint;
    private final AwsElasticsearchFactory awsElasticsearchFactory;

    public ElasticsearchDomainMapProvider(boolean autoDiscoverEndpoint)
    {
        this(autoDiscoverEndpoint, new AwsElasticsearchFactory());
    }

    @VisibleForTesting
    protected ElasticsearchDomainMapProvider(boolean autoDiscoverEndpoint,
                                             AwsElasticsearchFactory awsElasticsearchFactory)
    {
        this.autoDiscoverEndpoint = autoDiscoverEndpoint;
        this.awsElasticsearchFactory = awsElasticsearchFactory;
    }

    /**
     * Gets a map of the domain names and their associated endpoints based on the autoDiscoverEndpoint flag. When
     * autoDiscoverEndpoint=true, this method will send list/describe commands via the AWS ES SDK to create the map.
     * When auto_discover_endpoint=false, the map will be derived from the domainMapping string passed in as argument.
     * @param domainMapping The contents of the domain_mapping environment variable with secrets already resolved.
     *                      This parameter will be ignored when autoDiscoverEndpoint=true.
     * @return Populated domainMap with domain-names and corresponding endpoints.
     * @throws RuntimeException The domain map cannot be created due to an error with the AWS ES SDK or an invalid
     * domainMapping variable (empty, null, or contain invalid information that cannot be parsed successfully).
     */
    public Map<String, String> getDomainMap(String domainMapping)
            throws RuntimeException
    {
        if (autoDiscoverEndpoint) {
            // Get domain mapping via the AWS ES SDK (1.x).
            return getDomainMapFromAmazonElasticsearch();
        }
        else {
            // Get domain mapping from the domainMapping variable.
            return getDomainMapFromEnvironmentVar(domainMapping);
        }
    }

    /**
     * Gets a map of the domain names and their associated endpoints from the Amazon Elasticsearch Service.
     * @return Populated domainMap with domain-names and corresponding endpoints.
     * @throws RuntimeException when the domain map cannot be created due to an error with the AWS ES SDK.
     */
    private Map<String, String> getDomainMapFromAmazonElasticsearch()
            throws RuntimeException
    {
        final ElasticsearchClient awsEsClient = awsElasticsearchFactory.getClient();
        final Map<String, String> domainMap = new HashMap<>();

        try {
            ListDomainNamesResponse listDomainNamesResponse = awsEsClient.listDomainNames();
            List<String> domainNames = new ArrayList<>();
            listDomainNamesResponse.domainNames().forEach(domainInfo ->
                    domainNames.add(domainInfo.domainName()));

            int startDomainNameIndex = 0;
            int endDomainNameIndex;
            final int maxDomainNames = domainNames.size();

            while (startDomainNameIndex < maxDomainNames) {
                // DescribeElasticsearchDomains - Describes the domain configuration for up to five specified Amazon
                // ES domains. Create multiple requests when list of Domain Names > 5.
                endDomainNameIndex = Math.min(startDomainNameIndex + 5, maxDomainNames);
                DescribeElasticsearchDomainsRequest describeDomainsRequest = DescribeElasticsearchDomainsRequest
                        .builder().domainNames(domainNames.subList(startDomainNameIndex, endDomainNameIndex)).build();
                DescribeElasticsearchDomainsResponse describeDomainsResult =
                        awsEsClient.describeElasticsearchDomains(describeDomainsRequest);
                describeDomainsResult.domainStatusList().forEach(domainStatus -> {
                        String domainEndpoint = (domainStatus.endpoint() == null) ? domainStatus.endpoints().get("vpc") : domainStatus.endpoint();
                        domainMap.put(domainStatus.domainName(), endpointPrefix + domainEndpoint);
                });
                startDomainNameIndex = endDomainNameIndex;
            }

            if (domainMap.isEmpty()) {
                throw new AthenaConnectorException("Amazon Elasticsearch Service has no domain information for user.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
            }

            return domainMap;
        }
        finally {
            awsEsClient.close();
        }
    }

    /**
     * Gets a map of the domain names and their associated endpoints derived from the domainMapping string.
     * @param domainMapping The contents of the domain_mapping environment variable with secrets already resolved.
     *                      This parameter will be ignored when autoDiscoverEndpoint=true.
     * @return populated domainMap with domain-names and corresponding endpoints.
     * @throws RuntimeException Invalid domainMapping variable (empty, null, or contain invalid information that cannot
     * be parsed successfully).
     */
    private Map<String, String> getDomainMapFromEnvironmentVar(String domainMapping)
    {
        if (domainMapping == null || domainMapping.isEmpty()) {
            throw new AthenaConnectorException("Unable to create domain map: Empty or null value found in DomainMapping.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        Map<String, String> domainMap;
        try {
            domainMap = domainSplitter.split(domainMapping);
        }
        catch (Exception error) {
            // Intentional obfuscation of error message as it may contain sensitive info (e.g. username/password).
            throw new AthenaConnectorException("Unable to create domain map: DomainMapping Parsing error.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        if (domainMap.isEmpty()) {
            // Intentional obfuscation of error message: domainMapping contains sensitive info (e.g. username/password).
            throw new AthenaConnectorException("Unable to create domain map: Invalid DomainMapping value.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        return domainMap;
    }
}
