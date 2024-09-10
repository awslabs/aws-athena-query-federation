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
package com.amazonaws.athena.connectors.elasticsearch;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.elasticsearch.ElasticsearchClient;
import software.amazon.awssdk.services.elasticsearch.model.DescribeElasticsearchDomainsRequest;
import software.amazon.awssdk.services.elasticsearch.model.DescribeElasticsearchDomainsResponse;
import software.amazon.awssdk.services.elasticsearch.model.DomainInfo;
import software.amazon.awssdk.services.elasticsearch.model.ElasticsearchDomainStatus;
import software.amazon.awssdk.services.elasticsearch.model.ListDomainNamesResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;

/**
 * This class is used to test the ElasticsearchDomainMapProvider class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchDomainMapProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchDomainMapProviderTest.class);

    /**
     * Tests that a domain mapping string is parsed correctly into a domain map containing domain names and their
     * associated endpoints.
     */
    @Test
    public void getDomainMapFromEnvironmentVarTest()
    {
        logger.info("getDomainMapFromEnvironmentVarTest - enter");

        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);
        String domainMapping = "domain1=myusername@password:www.endpoint1.com,domain2=myusername@password:www.endpoint2.com";
        Map<String, String> domainMap = domainMapProvider.getDomainMap(domainMapping);

        logger.info("Domain map: {}", domainMap);

        assertEquals("Invalid number of elements in map:", 2, domainMap.size());
        assertEquals("Invalid value for domain1:", "myusername@password:www.endpoint1.com",
                domainMap.get("domain1"));
        assertEquals("Invalid value for domain2:", "myusername@password:www.endpoint2.com",
                domainMap.get("domain2"));

        logger.info("getDomainMapFromEnvironmentVarTest - exit");
    }

    @Test
    public void getDomainMapFromAwsElasticsearchTest()
    {
        logger.info("getDomainMapFromAwsElasticsearchTest - enter");

        AwsElasticsearchFactory mockElasticsearchFactory = mock(AwsElasticsearchFactory.class);
        ElasticsearchDomainMapProvider domainProvider =
                new ElasticsearchDomainMapProvider(true, mockElasticsearchFactory);
        ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
        ListDomainNamesResponse mockDomainInfo = mock(ListDomainNamesResponse.class);

        List<String> domainNames = ImmutableList.of("domain1", "domain2", "domain3", "domain4", "domain5", "domain6");
        List<DomainInfo> domainInfo = new ArrayList<>();
        List<ElasticsearchDomainStatus> domainStatus = new ArrayList<>();

        domainNames.forEach(domainName -> {
            domainInfo.add(DomainInfo.builder().domainName(domainName).build());
            domainStatus.add(ElasticsearchDomainStatus.builder()
                    .domainName(domainName)
                    .endpoint("www.domain." + domainName).build());
        });

        when(mockElasticsearchFactory.getClient()).thenReturn(mockClient);
        when(mockClient.listDomainNames()).thenReturn(mockDomainInfo);
        when(mockDomainInfo.domainNames()).thenReturn(domainInfo);

        when(mockClient.describeElasticsearchDomains(DescribeElasticsearchDomainsRequest.builder().
                domainNames(domainNames.subList(0, 5)).build()))
                .thenReturn(DescribeElasticsearchDomainsResponse.builder()
                        .domainStatusList(domainStatus.subList(0, 5)).build());
        when(mockClient.describeElasticsearchDomains(DescribeElasticsearchDomainsRequest.builder()
                .domainNames(domainNames.subList(5, 6)).build()))
                .thenReturn(DescribeElasticsearchDomainsResponse.builder()
                        .domainStatusList(domainStatus.subList(5, 6)).build());

        Map domainMap = domainProvider.getDomainMap(null);
        logger.info("Domain Map: {}", domainMap);

        verify(mockClient).describeElasticsearchDomains(DescribeElasticsearchDomainsRequest.builder()
                .domainNames(domainNames.subList(0, 5)).build());
        verify(mockClient).describeElasticsearchDomains(DescribeElasticsearchDomainsRequest.builder()
                .domainNames(domainNames.subList(5, 6)).build());

        assertEquals("Invalid number of domains.", domainNames.size(), domainMap.size());
        domainNames.forEach(domainName -> {
            assertTrue("Domain does not exist in Map.", domainMap.containsKey(domainName));
            assertEquals("Domain info mismatch.", "https://www.domain." + domainName,
                    domainMap.get(domainName));
        });

        logger.info("getDomainMapFromAwsElasticsearchTest - exit");
    }
}
