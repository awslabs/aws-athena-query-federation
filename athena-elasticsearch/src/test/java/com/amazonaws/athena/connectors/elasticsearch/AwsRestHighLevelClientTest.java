/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.ClusterClient;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the AwsRestHighLevelClient class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AwsRestHighLevelClientTest
{
    private static final String TEST_INDEX = "test-index";
    private static final String ERROR_MESSAGE = "Test error message";
    private static final String TEST_ENDPOINT = "https://search-test.us-east-1.es.amazonaws.com";
    private static final String ALIAS1 = "alias1";
    private static final String ALIAS2 = "alias2";
    private static final String PROPERTIES = "properties";
    private static final int MAX_SHARDS = 10;

    @Mock
    private RestClientBuilder mockRestClientBuilder;

    @Mock
    private IndicesClient mockIndicesClient;

    @Mock
    private GetMappingsResponse mockGetMappingsResponse;

    @Mock
    private GetAliasesResponse mockGetAliasesResponse;

    @Mock
    private MappingMetadata mockMappingMetadata;

    @Mock
    private SearchResponse mockSearchResponse;

    @Mock
    private SearchHit mockSearchHit;

    @Mock
    private ClusterClient mockClusterClient;

    @Mock
    private ClusterHealthResponse mockClusterHealthResponse;

    private AwsRestHighLevelClient client;

    @Before
    public void setUp()
    {
        RestClient mockRestClient = org.mockito.Mockito.mock(RestClient.class);
        when(mockRestClientBuilder.build()).thenReturn(mockRestClient);
        client = org.mockito.Mockito.spy(new AwsRestHighLevelClient(mockRestClientBuilder));
        when(client.indices()).thenReturn(mockIndicesClient);
    }


    @Test
    public void getMapping_withRegularIndex_returnsMapping() throws Exception
    {
        LinkedHashMap<String, Object> expectedMapping = new LinkedHashMap<>();
        expectedMapping.put(PROPERTIES, new LinkedHashMap<>());

        Map<String, MappingMetadata> mappingsMap = new LinkedHashMap<>();
        when(mockMappingMetadata.getSourceAsMap()).thenReturn(expectedMapping);
        mappingsMap.put(TEST_INDEX, mockMappingMetadata);

        when(mockIndicesClient.getMapping(any(GetMappingsRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockGetMappingsResponse);
        when(mockGetMappingsResponse.mappings()).thenReturn(mappingsMap);

        LinkedHashMap<String, Object> result = client.getMapping(TEST_INDEX);

        assertNotNull("Mapping for regular index should not be null", result);
        assertEquals("Mapping for regular index should match expected", expectedMapping, result);
    }

    @Test
    public void getMapping_withDataStream_returnsFirstAvailableMapping() throws Exception
    {
        LinkedHashMap<String, Object> expectedMapping = new LinkedHashMap<>();
        expectedMapping.put(PROPERTIES, new LinkedHashMap<>());

        Map<String, MappingMetadata> mappingsMap = new LinkedHashMap<>();
        MappingMetadata dsMappingMetadata = org.mockito.Mockito.mock(MappingMetadata.class);
        when(dsMappingMetadata.getSourceAsMap()).thenReturn(expectedMapping);
        mappingsMap.put(".ds-test-datastream-000001", dsMappingMetadata);

        when(mockIndicesClient.getMapping(any(GetMappingsRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockGetMappingsResponse);
        when(mockGetMappingsResponse.mappings()).thenReturn(mappingsMap);

        LinkedHashMap<String, Object> result = client.getMapping("test-datastream");

        assertNotNull("Mapping for data stream should not be null", result);
        assertEquals("Mapping for data stream should match expected", expectedMapping, result);
    }

    @Test
    public void getMapping_whenNoMappingFound_throwsAthenaConnectorException() throws IOException
    {
        when(mockIndicesClient.getMapping(any(GetMappingsRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockGetMappingsResponse);
        when(mockGetMappingsResponse.mappings()).thenReturn(Collections.emptyMap());

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                () -> client.getMapping(TEST_INDEX));
        assertTrue("Exception message should contain Could not find mapping",
                ex.getMessage().contains("Could not find mapping for data stream name"));
    }

    @Test
    public void getAliases_withValidRequest_returnsAliases() throws Exception
    {
        Map<String, Set<AliasMetadata>> aliasesMap = new LinkedHashMap<>();
        aliasesMap.put(ALIAS1, Collections.emptySet());
        aliasesMap.put(ALIAS2, Collections.emptySet());

        when(mockIndicesClient.getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockGetAliasesResponse);
        when(mockGetAliasesResponse.getAliases()).thenReturn(aliasesMap);

        Set<String> aliases = client.getAliases();

        assertNotNull("Aliases from valid request should not be null", aliases);
        assertEquals("Should have 2 aliases from valid request", 2, aliases.size());
        assertTrue("Should contain alias1 from valid request", aliases.contains(ALIAS1));
        assertTrue("Should contain alias2 from valid request", aliases.contains(ALIAS2));
    }

    @Test
    public void getAliases_whenExceptionOccurs_throwsAthenaConnectorException() throws IOException
    {
        when(mockIndicesClient.getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT)))
                .thenThrow(new IOException(ERROR_MESSAGE));

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                () -> client.getAliases());
        assertTrue("Exception message should contain error message",
                ex.getMessage().contains(ERROR_MESSAGE));
    }

    @Test
    public void getDocuments_withValidRequest_returnsSearchResponse() throws Exception
    {
        SearchRequest request = new SearchRequest(TEST_INDEX);
        doReturn(mockSearchResponse).when(client).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

        SearchResponse response = client.getDocuments(request);

        assertNotNull("Search response from valid request should not be null", response);
        assertEquals("Search response from valid request should match expected", mockSearchResponse, response);
    }

    @Test
    public void getDocument_withSearchHit_returnsDocumentMap()
    {
        Map<String, Object> expectedDocument = new LinkedHashMap<>();
        expectedDocument.put("field1", "value1");
        expectedDocument.put("field2", "value2");

        when(mockSearchHit.getSourceAsMap()).thenReturn(expectedDocument);

        Map<String, Object> document = client.getDocument(mockSearchHit);

        assertNotNull("Document from search hit should not be null", document);
        assertEquals("Document from search hit should match expected", expectedDocument, document);
    }

    @Test
    public void builder_constructor_createsBuilder()
    {
        AwsRestHighLevelClient.Builder builder = new AwsRestHighLevelClient.Builder(TEST_ENDPOINT);

        assertNotNull("Builder from constructor should not be null", builder);
    }

    @Test
    public void builder_withAwsCredentials_returnsBuilderWithAwsSigningInterceptor()
    {
        AwsRestHighLevelClient.Builder builder = new AwsRestHighLevelClient.Builder(TEST_ENDPOINT);
        software.amazon.awssdk.auth.credentials.AwsCredentialsProvider credentialsProvider =
                mock(software.amazon.awssdk.auth.credentials.AwsCredentialsProvider.class);

        AwsRestHighLevelClient.Builder result = builder.withCredentials(credentialsProvider);

        assertNotNull("Result should not be null", result);
        assertEquals("Builder with AWS credentials should return self", builder, result);
    }

    @Test
    public void builder_withAwsCredentials_returnsSameBuilderInstance()
    {
        String endpoint = "https://test";
        AwsRestHighLevelClient.Builder builder = new AwsRestHighLevelClient.Builder(endpoint);
        software.amazon.awssdk.auth.credentials.AwsCredentialsProvider credentialsProvider =
                mock(software.amazon.awssdk.auth.credentials.AwsCredentialsProvider.class);

        AwsRestHighLevelClient.Builder result = builder.withCredentials(credentialsProvider);

        assertNotNull("Builder withCredentials result should not be null", result);
        assertEquals("withCredentials should return same builder instance", builder, result);
    }

    @Test
    public void builder_withUsernamePassword_returnsBuilderWithBasicAuthProvider()
    {
        AwsRestHighLevelClient.Builder builder = new AwsRestHighLevelClient.Builder(TEST_ENDPOINT);

        AwsRestHighLevelClient.Builder result = builder.withCredentials("testuser", "testpass");

        assertNotNull("Result from builder with username and password should not be null", result);
        assertEquals("Builder with username and password should return self", builder, result);
    }

    @Test
    public void builder_build_returnsClient()
    {
        AwsRestHighLevelClient.Builder builder = new AwsRestHighLevelClient.Builder(TEST_ENDPOINT);

        AwsRestHighLevelClient client = builder.build();

        assertNotNull("Client from builder build should not be null", client);
    }

    @Test
    public void getShardIds_whenClusterHealthTimesOut_throwsAthenaConnectorException()
            throws Exception
    {
        when(client.cluster()).thenReturn(mockClusterClient);
        when(mockClusterClient.health(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockClusterHealthResponse);
        when(mockClusterHealthResponse.isTimedOut()).thenReturn(true);

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                () -> client.getShardIds(TEST_INDEX, MAX_SHARDS));
        assertTrue("Exception message should contain Request timed out",
                ex.getMessage().contains("Request timed out"));
    }

    @Test
    public void getShardIds_whenActiveShardsZero_throwsAthenaConnectorException()
            throws Exception
    {
        when(client.cluster()).thenReturn(mockClusterClient);
        when(mockClusterClient.health(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockClusterHealthResponse);
        when(mockClusterHealthResponse.isTimedOut()).thenReturn(false);
        when(mockClusterHealthResponse.getActiveShards()).thenReturn(0);

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                () -> client.getShardIds(TEST_INDEX, MAX_SHARDS));
        assertTrue("Exception message should contain no active shards",
                ex.getMessage().contains("no active shards"));
    }

    @Test
    public void getShardIds_whenClusterStatusRed_throwsAthenaConnectorException()
            throws Exception
    {
        when(client.cluster()).thenReturn(mockClusterClient);
        when(mockClusterClient.health(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockClusterHealthResponse);
        when(mockClusterHealthResponse.isTimedOut()).thenReturn(false);
        when(mockClusterHealthResponse.getActiveShards()).thenReturn(5);
        when(mockClusterHealthResponse.getStatus()).thenReturn(ClusterHealthStatus.RED);

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                () -> client.getShardIds(TEST_INDEX, MAX_SHARDS));
        assertTrue("Exception message should contain RED",
                ex.getMessage().contains("RED"));
    }

    @Test
    public void getShardIds_whenIndexNotFound_throwsAthenaConnectorException()
            throws Exception
    {
        when(client.cluster()).thenReturn(mockClusterClient);
        when(mockClusterClient.health(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockClusterHealthResponse);
        when(mockClusterHealthResponse.isTimedOut()).thenReturn(false);
        when(mockClusterHealthResponse.getActiveShards()).thenReturn(5);
        when(mockClusterHealthResponse.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
        when(mockClusterHealthResponse.getIndices()).thenReturn(Collections.emptyMap());

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                () -> client.getShardIds(TEST_INDEX, MAX_SHARDS));
        assertTrue("Exception message should contain invalid index",
                ex.getMessage().contains("invalid index"));
    }

    @Test
    public void getShardIds_withValidIndex_returnsShardIds()
            throws Exception
    {
        Map<Integer, ClusterShardHealth> shards = new LinkedHashMap<>();
        ClusterShardHealth shard1 = org.mockito.Mockito.mock(ClusterShardHealth.class);
        ClusterShardHealth shard2 = org.mockito.Mockito.mock(ClusterShardHealth.class);
        shards.put(0, shard1);
        shards.put(1, shard2);

        ClusterIndexHealth indexHealth = org.mockito.Mockito.mock(ClusterIndexHealth.class);
        when(indexHealth.getShards()).thenReturn(shards);

        Map<String, ClusterIndexHealth> indices = new LinkedHashMap<>();
        indices.put(TEST_INDEX, indexHealth);

        when(client.cluster()).thenReturn(mockClusterClient);
        when(mockClusterClient.health(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockClusterHealthResponse);
        when(mockClusterHealthResponse.isTimedOut()).thenReturn(false);
        when(mockClusterHealthResponse.getActiveShards()).thenReturn(5);
        when(mockClusterHealthResponse.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
        when(mockClusterHealthResponse.getIndices()).thenReturn(indices);

        Set<Integer> result = client.getShardIds(TEST_INDEX, MAX_SHARDS);

        assertNotNull("Shard IDs from valid index should not be null", result);
        assertEquals("Should have 2 shards from valid index", 2, result.size());
        assertTrue("Should contain shard 0 from valid index", result.contains(0));
        assertTrue("Should contain shard 1 from valid index", result.contains(1));
    }
}
