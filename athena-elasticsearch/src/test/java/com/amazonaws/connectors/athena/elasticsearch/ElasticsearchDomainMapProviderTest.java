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

import com.amazonaws.services.elasticsearch.AWSElasticsearch;
import com.amazonaws.services.elasticsearch.model.DomainInfo;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsRequest;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsResult;
import com.amazonaws.services.elasticsearch.model.ElasticsearchDomainStatus;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesResult;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
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
        AWSElasticsearch mockClient = mock(AWSElasticsearch.class);
        ListDomainNamesResult mockDomainInfo = mock(ListDomainNamesResult.class);

        List<String> domainNames = ImmutableList.of("domain1", "domain2", "domain3", "domain4", "domain5", "domain6");
        List<DomainInfo> domainInfo = new ArrayList<>();
        List<ElasticsearchDomainStatus> domainStatus = new ArrayList<>();

        domainNames.forEach(domainName -> {
            domainInfo.add(new DomainInfo().withDomainName(domainName));
            domainStatus.add(new ElasticsearchDomainStatus()
                    .withDomainName(domainName)
                    .withEndpoint("www.domain." + domainName));
        });

        when(mockElasticsearchFactory.getClient()).thenReturn(mockClient);
        when(mockClient.listDomainNames(any())).thenReturn(mockDomainInfo);
        when(mockDomainInfo.getDomainNames()).thenReturn(domainInfo);

        when(mockClient.describeElasticsearchDomains(new DescribeElasticsearchDomainsRequest()
                .withDomainNames(domainNames.subList(0, 5))))
                .thenReturn(new DescribeElasticsearchDomainsResult()
                        .withDomainStatusList(domainStatus.subList(0, 5)));
        when(mockClient.describeElasticsearchDomains(new DescribeElasticsearchDomainsRequest()
                .withDomainNames(domainNames.subList(5, 6))))
                .thenReturn(new DescribeElasticsearchDomainsResult()
                        .withDomainStatusList(domainStatus.subList(5, 6)));

        Map domainMap = domainProvider.getDomainMap(null);
        logger.info("Domain Map: {}", domainMap);

        verify(mockClient).describeElasticsearchDomains(new DescribeElasticsearchDomainsRequest()
                .withDomainNames(domainNames.subList(0, 5)));
        verify(mockClient).describeElasticsearchDomains(new DescribeElasticsearchDomainsRequest()
                .withDomainNames(domainNames.subList(5, 6)));

        assertEquals("Invalid number of domains.", domainNames.size(), domainMap.size());
        domainNames.forEach(domainName -> {
            assertTrue("Domain does not exist in Map.", domainMap.containsKey(domainName));
            assertEquals("Domain info mismatch.", "https://www.domain." + domainName,
                    domainMap.get(domainName));
        });

        logger.info("getDomainMapFromAwsElasticsearchTest - exit");
    }
}
