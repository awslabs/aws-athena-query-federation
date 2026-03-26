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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void getDomainMap_withEnvironmentVar_returnsDomainMap()
    {
        logger.info("getDomainMap_withEnvironmentVar_returnsDomainMap - enter");

        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);
        String domainMapping = "domain1=myusername@password:www.endpoint1.com,domain2=myusername@password:www.endpoint2.com";
        Map<String, String> domainMap = domainMapProvider.getDomainMap(domainMapping);

        logger.info("Domain map: {}", domainMap);

        assertEquals("Invalid number of elements in map:", 2, domainMap.size());
        assertEquals("Invalid value for domain1:", "myusername@password:www.endpoint1.com",
                domainMap.get("domain1"));
        assertEquals("Invalid value for domain2:", "myusername@password:www.endpoint2.com",
                domainMap.get("domain2"));

        logger.info("getDomainMap_withEnvironmentVar_returnsDomainMap - exit");
    }

    @Test
    public void getDomainMap_fromAwsElasticsearch_returnsDomainMap()
    {
        logger.info("getDomainMap_fromAwsElasticsearch_returnsDomainMap - enter");

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

        logger.info("getDomainMap_fromAwsElasticsearch_returnsDomainMap - exit");
    }

    @Test
    public void getDomainMap_withEmptyString_throwsAthenaConnectorException()
    {
        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);

        try {
            domainMapProvider.getDomainMap("");
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Empty or null value",
                    ex.getMessage().contains("Empty or null value found in DomainMapping"));
        }
    }

    @Test
    public void getDomainMap_withNullString_throwsAthenaConnectorException()
    {
        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);

        try {
            domainMapProvider.getDomainMap(null);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Empty or null value",
                    ex.getMessage().contains("Empty or null value found in DomainMapping"));
        }
    }

    @Test
    public void getDomainMap_withSingleDomainMapping_returnsSingleEntryMap()
    {
        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);
        String domainMapping = "domain1=myusername@password:www.endpoint1.com";
        Map<String, String> domainMap = domainMapProvider.getDomainMap(domainMapping);

        assertEquals("Invalid number of elements in map:", 1, domainMap.size());
        assertEquals("Invalid value for domain1:", "myusername@password:www.endpoint1.com",
                domainMap.get("domain1"));
    }

    @Test
    public void getDomainMap_withParsingError_throwsAthenaConnectorException()
    {
        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);
        String invalidDomainMapping = "invalid=format=with=too=many=equals";

        try {
            domainMapProvider.getDomainMap(invalidDomainMapping);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Parsing error",
                    ex.getMessage().contains("DomainMapping Parsing error"));
        }
    }

    @Test
    public void getDomainMap_withInvalidFormat_returnsMapWithEmptyKeys()
    {
        // ElasticsearchDomainMapProvider.getDomainMap currently produces a map {"": ""} for input "=".
        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);
        String invalidDomainMapping = "=";

        Map<String, String> domainMap = domainMapProvider.getDomainMap(invalidDomainMapping);

        assertNotNull("Domain map should not be null for invalid format", domainMap);
        assertFalse("Domain map should not be empty for single equals input", domainMap.isEmpty());
        assertTrue("Domain map should contain empty key", domainMap.containsKey(""));
        assertEquals("Domain map value for empty key should be empty string", "", domainMap.get(""));
    }

    @Test
    public void getDomainMap_fromAwsElasticsearchWhenDescribeDomainsThrows_propagatesException()
    {
        AwsElasticsearchFactory mockElasticsearchFactory = mock(AwsElasticsearchFactory.class);
        ElasticsearchDomainMapProvider domainProvider =
                new ElasticsearchDomainMapProvider(true, mockElasticsearchFactory);
        ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
        ListDomainNamesResponse mockDomainInfo = mock(ListDomainNamesResponse.class);
        List<DomainInfo> domainInfo = new ArrayList<>();
        domainInfo.add(DomainInfo.builder().domainName("domain1").build());

        when(mockElasticsearchFactory.getClient()).thenReturn(mockClient);
        when(mockClient.listDomainNames()).thenReturn(mockDomainInfo);
        when(mockDomainInfo.domainNames()).thenReturn(domainInfo);
        when(mockClient.describeElasticsearchDomains(any(DescribeElasticsearchDomainsRequest.class)))
                .thenThrow(new RuntimeException("Describe domains failed"));

        try {
            domainProvider.getDomainMap(null);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException ex) {
            assertTrue("Exception message should contain Describe domains failed",
                    ex.getMessage().contains("Describe domains failed"));
        }
    }

    @Test
    public void getDomainMap_fromAwsElasticsearchWithEmptyDomainList_throwsAthenaConnectorException()
    {
        AwsElasticsearchFactory mockElasticsearchFactory = mock(AwsElasticsearchFactory.class);
        ElasticsearchDomainMapProvider domainProvider =
                new ElasticsearchDomainMapProvider(true, mockElasticsearchFactory);
        ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
        ListDomainNamesResponse mockDomainInfo = mock(ListDomainNamesResponse.class);

        when(mockElasticsearchFactory.getClient()).thenReturn(mockClient);
        when(mockClient.listDomainNames()).thenReturn(mockDomainInfo);
        when(mockDomainInfo.domainNames()).thenReturn(ImmutableList.of());

        try {
            domainProvider.getDomainMap(null);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain no domain information",
                    ex.getMessage().contains("no domain information"));
        }
    }

    @Test
    public void getDomainMap_fromAwsElasticsearchWithNullEndpoint_usesVpcEndpoint()
    {
        AwsElasticsearchFactory mockElasticsearchFactory = mock(AwsElasticsearchFactory.class);
        ElasticsearchDomainMapProvider domainProvider =
                new ElasticsearchDomainMapProvider(true, mockElasticsearchFactory);
        ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
        ListDomainNamesResponse mockDomainInfo = mock(ListDomainNamesResponse.class);

        List<String> domainNames = ImmutableList.of("domain1");
        List<DomainInfo> domainInfo = new ArrayList<>();
        List<ElasticsearchDomainStatus> domainStatus = new ArrayList<>();

        domainInfo.add(DomainInfo.builder().domainName("domain1").build());
        domainStatus.add(ElasticsearchDomainStatus.builder()
                .domainName("domain1")
                .endpoint(null)
                .endpoints(java.util.Map.of("vpc", "vpc-domain1"))
                .build());

        when(mockElasticsearchFactory.getClient()).thenReturn(mockClient);
        when(mockClient.listDomainNames()).thenReturn(mockDomainInfo);
        when(mockDomainInfo.domainNames()).thenReturn(domainInfo);

        when(mockClient.describeElasticsearchDomains(DescribeElasticsearchDomainsRequest.builder()
                .domainNames(domainNames).build()))
                .thenReturn(DescribeElasticsearchDomainsResponse.builder()
                        .domainStatusList(domainStatus).build());

        Map<String, String> domainMap = domainProvider.getDomainMap(null);

        assertEquals("Invalid number of domains.", 1, domainMap.size());
        assertTrue("Domain map should contain domain1", domainMap.containsKey("domain1"));
        assertEquals("Endpoint should use VPC endpoint when endpoint is null",
                "https://vpc-domain1", domainMap.get("domain1"));
    }
}
