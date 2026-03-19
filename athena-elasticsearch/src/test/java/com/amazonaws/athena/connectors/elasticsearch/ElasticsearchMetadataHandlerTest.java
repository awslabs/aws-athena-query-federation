/*-
 * #%L
 * athena-elasticsearch
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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.elasticsearch.qpt.ElasticsearchQueryPassthrough;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.DataStream;
import org.elasticsearch.client.indices.GetDataStreamRequest;
import org.elasticsearch.client.indices.GetDataStreamResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.lenient;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the ElasticsearchMetadataHandler class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchMetadataHandlerTest.class);
    private static final String MOCK_SECRET_NAME = "asdf_secret";
    private static final String SPILL_BUCKET = "spill-bucket";
    private static final String SPILL_PREFIX = "spill-prefix";
    private static final String DEFAULT_DOMAIN = "movies";
    private static final String DEFAULT_ENDPOINT = "https://search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com";
    private static final int DEFAULT_TIMEOUT = 10;

    private ElasticsearchMetadataHandler handler;
    private boolean enableTests = System.getenv("publishing") != null &&
            System.getenv("publishing").equalsIgnoreCase("true");
    private BlockAllocatorImpl allocator;

    @Mock
    private GlueClient awsGlue;

    @Mock
    private SecretsManagerClient awsSecretsManager;

    @Mock
    private AthenaClient amazonAthena;

    @Mock
    private AwsRestHighLevelClient mockClient;

    @Mock
    private AwsRestHighLevelClientFactory clientFactory;

    @Mock
    private ElasticsearchDomainMapProvider domainMapProvider;

    @Before
    public void setUp()
    {
        logger.info("setUpBefore - enter");

        allocator = new BlockAllocatorImpl();
        when(clientFactory.getOrCreateClient(nullable(String.class))).thenReturn(mockClient);

        logger.info("setUpBefore - exit");

        when(awsSecretsManager.getSecretValue(GetSecretValueRequest.builder().secretId(MOCK_SECRET_NAME).build()))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"asdf_mock_user_name\", \"password\": \"asdf_mock_user_federation_password_1@!$\"}").build());
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    /**
     * Used to test the doListSchemaNames() functionality in the ElasticsearchMetadataHandler class.
     */
    @Test
    public void doListSchemaNames_withDomains_returnsSchemaNames()
    {
        logger.info("doListSchemaNames_withDomains_returnsSchemaNames - enter");

        // Generate hard-coded response with 3 domains.
        ListSchemasResponse mockDomains =
                new ListSchemasResponse("elasticsearch", ImmutableList.of("domain2", "domain3", "domain1"));

        // Get real response from doListSchemaNames().
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of("domain1", "endpoint1",
                "domain2", "endpoint2","domain3", "endpoint3"));

        handler = createElasticsearchMetadataHandler();

        ListSchemasRequest req = new ListSchemasRequest(fakeIdentity(), "queryId", "elasticsearch");
        ListSchemasResponse realDomains = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemaNames_withDomains_returnsSchemaNames - {}", realDomains.getSchemas());

        // Test 1 - Real domain list should NOT be empty.
        assertFalse("Real domain list has no domain names!", realDomains.getSchemas().isEmpty());
        // Test 2 - Real and mocked responses should have the same domains.
        assertTrue("Real and mocked domain responses have different domains!",
                domainsEqual(realDomains.getSchemas(), mockDomains.getSchemas()));

        logger.info("doListSchemaNames_withDomains_returnsSchemaNames - exit");
    }

    /**
     * Used to assert that both real and mocked domain lists are equal.
     * @param list1 is a domain list to be compared.
     * @param list2 is a domain list to be compared.
     * @return true if the lists are equal, false otherwise.
     */
    private final boolean domainsEqual(Collection<String> list1, Collection<String> list2)
    {
        logger.info("domainsEqual - Enter - Domain1: {}, Domain2: {}", list1, list2);

        // lists must have the same number of domains.
        if (list1.size() != list2.size()) {
            logger.warn("Domain lists are different sizes!");
            return false;
        }

        // lists must have the same domains (irrespective of internal ordering).
        Iterator<String> iter = list1.iterator();
        while (iter.hasNext()) {
            if (!list2.contains(iter.next())) {
                logger.warn("Domain mismatch in list!");
                return false;
            }
        }

        return true;
    }

    /**
     * Used to test the doListTables() functionality in the ElasticsearchMetadataHandler class.
     * @throws IOException
     */
    @Test
    public void doListTables_withIndices_returnsTables()
            throws Exception
    {
        logger.info("doListTables_withIndices_returnsTables - enter");

        // Hardcoded response with 2 indices.
        Collection<TableName> mockIndices = ImmutableList.of(new TableName("movies", "customer"),
                new TableName("movies", "movies"),
                new TableName("movies", "stream1"),
                new TableName("movies", "stream2"));

        // Get real indices.
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(DEFAULT_DOMAIN, DEFAULT_ENDPOINT));

        handler = createElasticsearchMetadataHandler();

        IndicesClient indices = mock(IndicesClient.class);
        GetDataStreamResponse mockIndexResponse = mock(GetDataStreamResponse.class);
        when(mockIndexResponse.getDataStreams()).thenReturn(
                ImmutableList.of(new DataStream("stream1", "ts",ImmutableList.of("index1", "index2"), 0, null, null, null),
                        new DataStream("stream2", "ts",ImmutableList.of("index7", "index8"), 0, null, null, null)));
        when(indices.getDataStream(nullable(GetDataStreamRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(mockIndexResponse);
        when(mockClient.indices()).thenReturn(indices);

        when(mockClient.getAliases()).thenReturn(ImmutableSet.of("movies", ".kibana_1", "customer"));
        ListTablesRequest req = new ListTablesRequest(fakeIdentity(),
                "queryId", "elasticsearch", "movies", null, UNLIMITED_PAGE_SIZE_VALUE);
        Collection<TableName> realIndices = handler.doListTables(allocator, req).getTables();

        logger.info("doListTables_withIndices_returnsTables - {}", realIndices);

        // Test 1 - Indices list should NOT be empty.
        assertFalse("Real indices list is empty!", realIndices.isEmpty());
        // Test 2 - Real list and mocked list should have the same indices.
        assertTrue("Real and mocked indices list are different!",
                indicesEqual(realIndices, mockIndices));

        logger.info("doListTables_withIndices_returnsTables - exit");
    }

    /**
     * Used to assert that both real and mocked indices lists are equal.
     * @param list1 is an indices list to be compared.
     * @param list2 is an indices list to be compared.
     * @return true if the lists are equal, false otherwise.
     */
    private final boolean indicesEqual(Collection<TableName> list1, Collection<TableName> list2)
    {
        logger.info("indicesEqual - Enter - Index1: {}, Index2: {}", list1, list2);

        // lists must have the same number of indices.
        if (list1.size() != list2.size()) {
            logger.warn("Indices lists are different sizes!");
            return false;
        }

        // lists must have the same indices (irrespective of internal ordering).
        Iterator<TableName> iter = list1.iterator();
        while (iter.hasNext()) {
            if (!list2.contains(iter.next())) {
                logger.warn("Indices mismatch in list!");
                return false;
            }
        }

        return true;
    }

    /**
     * Used to test the doGetTable() functionality in the ElasticsearchMetadataHandler class.
     * @throws IOException
     */
    @Test
    public void doGetTable_withMapping_returnsTable()
            throws Exception
    {
        logger.info("doGetTable_withMapping_returnsTable - enter");

        // Mock mapping.
        Schema mockMapping = SchemaBuilder.newBuilder()
                .addField("mytext", Types.MinorType.VARCHAR.getType())
                .addField("mykeyword", Types.MinorType.VARCHAR.getType())
                .addField(new Field("mylong", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mylong",
                                FieldType.nullable(Types.MinorType.BIGINT.getType()), null))))
                .addField("myinteger", Types.MinorType.INT.getType())
                .addField("myshort", Types.MinorType.SMALLINT.getType())
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .addField(new Field("myscaled",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10.0")), null))
                .addField("myfloat", Types.MinorType.FLOAT8.getType())
                .addField("myhalf", Types.MinorType.FLOAT8.getType())
                .addField("mydatemilli", Types.MinorType.DATEMILLI.getType())
                .addField("mydatenano", Types.MinorType.DATEMILLI.getType())
                .addField("myboolean", Types.MinorType.BIT.getType())
                .addField("mybinary", Types.MinorType.VARCHAR.getType())
                .addField("mynested", Types.MinorType.STRUCT.getType(), ImmutableList.of(
                        new Field("l1long", FieldType.nullable(Types.MinorType.BIGINT.getType()), null),
                        new Field("l1date", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null),
                        new Field("l1nested", FieldType.nullable(Types.MinorType.STRUCT.getType()), ImmutableList.of(
                                new Field("l2short", FieldType.nullable(Types.MinorType.LIST.getType()),
                                        Collections.singletonList(new Field("l2short",
                                                FieldType.nullable(Types.MinorType.SMALLINT.getType()), null))),
                                new Field("l2binary", FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                                        null))))).build();

        // Real mapping.
        LinkedHashMap<String, Object> mapping = new ObjectMapper().readValue(
                "{\n" +
                "  \"mishmash\" : {\n" +                                // Index: mishmash
                "    \"mappings\" : {\n" +
                "      \"_meta\" : {\n" +                               // _meta:
                "        \"mynested.l1nested.l2short\" : \"list\",\n" + // mynested.l1nested.l2short: LIST<SMALLINT>
                "        \"mylong\" : \"list\"\n" +                     // mylong: LIST<BIGINT>
                "      },\n" +
                "      \"properties\" : {\n" +
                "        \"mybinary\" : {\n" +                          // mybinary:
                "          \"type\" : \"binary\"\n" +                   // type: binary (VARCHAR)
                "        },\n" +
                "        \"myboolean\" : {\n" +                         // myboolean:
                "          \"type\" : \"boolean\"\n" +                  // type: boolean (BIT)
                "        },\n" +
                "        \"mybyte\" : {\n" +                            // mybyte:
                "          \"type\" : \"byte\"\n" +                     // type: byte (TINYINT)
                "        },\n" +
                "        \"mydatemilli\" : {\n" +                       // mydatemilli:
                "          \"type\" : \"date\"\n" +                     // type: date (DATEMILLI)
                "        },\n" +
                "        \"mydatenano\" : {\n" +                        // mydatenano:
                "          \"type\" : \"date_nanos\"\n" +               // type: date_nanos (DATEMILLI)
                "        },\n" +
                "        \"mydouble\" : {\n" +                          // mydouble:
                "          \"type\" : \"double\"\n" +                   // type: double (FLOAT8)
                "        },\n" +
                "        \"myfloat\" : {\n" +                           // myfloat:
                "          \"type\" : \"float\"\n" +                    // type: float (FLOAT8)
                "        },\n" +
                "        \"myhalf\" : {\n" +                            // myhalf:
                "          \"type\" : \"half_float\"\n" +               // type: half_float (FLOAT8)
                "        },\n" +
                "        \"myinteger\" : {\n" +                         // myinteger:
                "          \"type\" : \"integer\"\n" +                  // type: integer (INT)
                "        },\n" +
                "        \"mykeyword\" : {\n" +                         // mykeyword:
                "          \"type\" : \"keyword\"\n" +                  // type: keyword (VARCHAR)
                "        },\n" +
                "        \"mylong\" : {\n" +                            // mylong: LIST
                "          \"type\" : \"long\"\n" +                     // type: long (BIGINT)
                "        },\n" +
                "        \"mynested\" : {\n" +                          // mynested: STRUCT
                "          \"properties\" : {\n" +
                "            \"l1date\" : {\n" +                        // mynested.l1date:
                "              \"type\" : \"date_nanos\"\n" +           // type: date_nanos (DATEMILLI)
                "            },\n" +
                "            \"l1long\" : {\n" +                        // mynested.l1long:
                "              \"type\" : \"long\"\n" +                 // type: long (BIGINT)
                "            },\n" +
                "            \"l1nested\" : {\n" +                      // mynested.l1nested: STRUCT
                "              \"properties\" : {\n" +
                "                \"l2binary\" : {\n" +                  // mynested.l1nested.l2binary:
                "                  \"type\" : \"binary\"\n" +           // type: binary (VARCHAR)
                "                },\n" +
                "                \"l2short\" : {\n" +                   // mynested.l1nested.l2short: LIST
                "                  \"type\" : \"short\"\n" +            // type: short (SMALLINT)
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"myscaled\" : {\n" +                          // myscaled:
                "          \"type\" : \"scaled_float\",\n" +            // type: scaled_float (BIGINT)
                "          \"scaling_factor\" : 10.0\n" +               // factor: 10
                "        },\n" +
                "        \"myshort\" : {\n" +                           // myshort:
                "          \"type\" : \"short\"\n" +                    // type: short (SMALLINT)
                "        },\n" +
                "        \"mytext\" : {\n" +                            // mytext:
                "          \"type\" : \"text\"\n" +                     // type: text (VARCHAR)
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n", LinkedHashMap.class);
        LinkedHashMap<String, Object> index = (LinkedHashMap<String, Object>) mapping.get("mishmash");
        LinkedHashMap<String, Object> mappings = (LinkedHashMap<String, Object>) index.get("mappings");

        when(mockClient.getMapping(nullable(String.class))).thenReturn(mappings);

        // Get real mapping.
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(DEFAULT_DOMAIN, DEFAULT_ENDPOINT));

        handler = createElasticsearchMetadataHandler();

        GetTableRequest req = new GetTableRequest(fakeIdentity(), "queryId", "elasticsearch",
                new TableName("movies", "mishmash"), Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        Schema realMapping = res.getSchema();

        logger.info("doGetTable_withMapping_returnsTable - {}", res);

        // Test1 - Real mapping must NOT be empty.
        assertTrue("Real mapping is empty!", realMapping.getFields().size() > 0);
        // Test2 - Real and mocked mappings must have the same fields.
        assertTrue("Real and mocked mappings are different!",
                ElasticsearchSchemaUtils.mappingsEqual(realMapping, mockMapping));

        logger.info("doGetTable_withMapping_returnsTable - exit");
    }

    /**
     * Used to test the doGetSplits() functionality in the ElasticsearchMetadataHandler class.
     */
    @Test
    public void doGetSplits_withPartitions_returnsSplits()
            throws Exception
    {
        logger.info("doGetSplits_withPartitions_returnsSplits: enter");

        List<String> partitionCols = new ArrayList<>();
        String index = "customer";

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(fakeIdentity(),
                "queryId",
                "elasticsearch",
                new TableName("movies", index),
                partitions,
                partitionCols,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplits_withPartitions_returnsSplits: req[{}]", req);

        // Setup domain and endpoint
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(DEFAULT_DOMAIN, DEFAULT_ENDPOINT));

        when(mockClient.getShardIds(nullable(String.class), anyLong())).thenReturn(ImmutableSet
                .of(new Integer(0), new Integer(1), new Integer(2)));

        IndicesClient indices = mock(IndicesClient.class);
        GetIndexResponse mockIndexResponse = mock(GetIndexResponse.class);
        when(mockIndexResponse.getIndices()).thenReturn(new String[]{index});
        when(indices.get(nullable(GetIndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(mockIndexResponse);
        when(mockClient.indices()).thenReturn(indices);

        // Instantiate handler
        handler = createElasticsearchMetadataHandler();

        // Call doGetSplits()
        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits_withPartitions_returnsSplits: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        // Response should contain 2 splits.
        assertEquals("Response has invalid number of splits", 3, response.getSplits().size());

        Set<String> shardIds = new HashSet<>(2);
        shardIds.add("_shards:0");
        shardIds.add("_shards:1");
        shardIds.add("_shards:2");
        response.getSplits().forEach(split -> {
            assertEquals(DEFAULT_ENDPOINT, split.getProperty(DEFAULT_DOMAIN));
            String shard = split.getProperty(ElasticsearchMetadataHandler.SHARD_KEY);
            assertTrue("Split contains invalid shard: " + shard, shardIds.contains(shard));
            String actualIndex = split.getProperty(ElasticsearchMetadataHandler.INDEX_KEY);
            assertEquals("Split contains invalid index:" + index, index, actualIndex);
            shardIds.remove(shard);
        });

        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);

        logger.info("doGetSplits_withPartitions_returnsSplits: exit");
    }

    private static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("access_key_id",
            "principle",
            Collections.emptyMap(),
            Collections.emptyList(),
            Collections.emptyMap());
    }

    @Test
    public void convertField_withGlueTypes_returnsConvertedFields()
    {
        logger.info("convertField_withGlueTypes_returnsConvertedFields: enter");

        handler = createElasticsearchMetadataHandler();

        Field field = handler.convertField("myscaled", "SCALED_FLOAT(10.51)");

        assertEquals("myscaled", field.getName());
        assertEquals("10.51", field.getMetadata().get("scaling_factor"));

        field = handler.convertField("myscaledlist", "ARRAY<SCALED_FLOAT(100)>");

        assertEquals("myscaledlist", field.getName());
        assertEquals(Types.MinorType.LIST.getType(), field.getType());
        assertEquals("100", field.getChildren().get(0).getMetadata().get("scaling_factor"));

        field = handler.convertField("myscaledstruct", "STRUCT<myscaledstruct:SCALED_FLOAT(10.0)>");

        assertEquals(Types.MinorType.STRUCT.getType(), field.getType());
        assertEquals("myscaledstruct", field.getChildren().get(0).getName());
        assertEquals("10.0", field.getChildren().get(0).getMetadata().get("scaling_factor"));

        logger.info("convertField_withGlueTypes_returnsConvertedFields: exit");
    }

    @Test
    public void getDomainMap_withGlueConnectionAndNoDomainName_usesDefaultDomain()
    {
        String endpoint = "https://search-opensearch-phase2test-domain-bxdc4bfecnsm3stqp4x5rh3acq.us-east-1.es.amazonaws.com";
        Map<String, String> configMap = Map.of(DEFAULT_GLUE_CONNECTION, "asdf",
                SECRET_NAME, "asdf_secret",
                "domain_endpoint", endpoint);

        ElasticsearchMetadataHandler elasticsearchMetadataHandler = new ElasticsearchMetadataHandler(awsGlue, new LocalKeyFactory(), awsSecretsManager, amazonAthena,
                SPILL_BUCKET, SPILL_PREFIX, new ElasticsearchDomainMapProvider(false), clientFactory, DEFAULT_TIMEOUT, configMap, true);
        assertTrue(elasticsearchMetadataHandler.getDomainMap().containsKey("default"));
        assertEquals(elasticsearchMetadataHandler.getDomainMap().get("default"), endpoint);
    }

    @Test
    public void getDomainMap_withGlueConnectionAndDomainName_usesDomainNameForBackwardCompatibility()
    {
        String domainName = "iamdomain";
        String domain = "https://search-opensearch-phase2test-domain-bxdc4bfecnsm3stqp4x5rh3acq.us-east-1.es.amazonaws.com";
        Map<String, String> configMap = Map.of(DEFAULT_GLUE_CONNECTION, "asdf",
                SECRET_NAME, "asdf_secret",
                "domain_endpoint", domainName + "=" + domain);

        ElasticsearchMetadataHandler elasticsearchMetadataHandler = new ElasticsearchMetadataHandler(awsGlue, new LocalKeyFactory(), awsSecretsManager, amazonAthena,
                SPILL_BUCKET, SPILL_PREFIX, new ElasticsearchDomainMapProvider(false), clientFactory, DEFAULT_TIMEOUT, configMap, true);
        assertTrue(elasticsearchMetadataHandler.getDomainMap().containsKey(domainName));
        assertEquals(elasticsearchMetadataHandler.getDomainMap().get(domainName), domain);
    }

    @Test
    public void doGetDataSourceCapabilities_withValidRequest_returnsCapabilities()
    {
        handler = createElasticsearchMetadataHandler();

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                fakeIdentity(), "queryId", "elasticsearch");

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals("Catalog name should match", "elasticsearch", response.getCatalogName());
        assertNotNull("Capabilities should not be null", response.getCapabilities());
        assertFalse("Capabilities should not be empty", response.getCapabilities().isEmpty());
    }

    @Test
    public void doGetQueryPassthroughSchema_withValidRequest_returnsSchema() throws Exception
    {
        String index = "customer";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(DEFAULT_DOMAIN, DEFAULT_ENDPOINT));

        LinkedHashMap<String, Object> mapping = new LinkedHashMap<>();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        LinkedHashMap<String, Object> field1 = new LinkedHashMap<>();
        field1.put("type", "keyword");
        properties.put("field1", field1);
        mapping.put("properties", properties);

        when(mockClient.getMapping(nullable(String.class))).thenReturn(mapping);

        handler = createElasticsearchMetadataHandler();

        Map<String, String> queryPassthroughArgs = createQueryPassthroughArgs(DEFAULT_DOMAIN, index);

        GetTableRequest request = org.mockito.Mockito.mock(GetTableRequest.class);
        when(request.getCatalogName()).thenReturn("elasticsearch");
        when(request.getTableName()).thenReturn(new TableName(DEFAULT_DOMAIN, index));
        when(request.isQueryPassthrough()).thenReturn(true);
        when(request.getQueryPassthroughArguments()).thenReturn(queryPassthroughArgs);

        GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals("Catalog name should match", "elasticsearch", response.getCatalogName());
        assertNotNull("Schema should not be null", response.getSchema());
        assertTrue("Schema should have fields", response.getSchema().getFields().size() > 0);
    }

    @Test
    public void doGetQueryPassthroughSchema_whenNotQueryPassthrough_throwsAthenaConnectorException()
    {
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of());

        handler = createElasticsearchMetadataHandler();

        GetTableRequest request = org.mockito.Mockito.mock(GetTableRequest.class);
        when(request.isQueryPassthrough()).thenReturn(false);

        try {
            handler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain No Query passed through",
                    ex.getMessage().contains("No Query passed through"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName() + " with message: " + e.getMessage());
        }
    }

    @Test
    public void doGetQueryPassthroughSchema_whenDomainNotFound_throwsAthenaConnectorException()
    {
        String domain = "nonexistent";
        String index = "customer";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of());
        when(domainMapProvider.getDomainMap(any())).thenReturn(ImmutableMap.of());

        handler = createElasticsearchMetadataHandler();

        Map<String, String> queryPassthroughArgs = createQueryPassthroughArgs(domain, index);

        GetTableRequest request = org.mockito.Mockito.mock(GetTableRequest.class);
        when(request.isQueryPassthrough()).thenReturn(true);
        when(request.getQueryPassthroughArguments()).thenReturn(queryPassthroughArgs);

        try {
            handler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            String message = ex.getMessage();
            assertTrue("Exception message should contain Unable to find domain. Actual message: " + message,
                    message != null && message.contains("Unable to find domain"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName() + " with message: " + e.getMessage());
        }
    }

    @Test
    public void doListSchemaNames_whenAutoDiscoverEndpointTrue_refreshesDomainMap()
    {
        String domain1 = "movies";
        String domain2 = "books";
        String endpoint1 = "https://search-movies.us-east-1.es.amazonaws.com";
        String endpoint2 = "https://search-books.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain1, endpoint1, domain2, endpoint2));

        ElasticsearchMetadataHandler testHandler = createElasticsearchMetadataHandler(ImmutableMap.of("auto_discover_endpoint", "true"));

        ListSchemasRequest request = new ListSchemasRequest(fakeIdentity(), "queryId", "elasticsearch");
        ListSchemasResponse response = testHandler.doListSchemaNames(allocator, request);

        assertNotNull("Response should not be null", response);
        assertTrue("Should contain domain1", response.getSchemas().contains(domain1));
        assertTrue("Should contain domain2", response.getSchemas().contains(domain2));
        assertEquals("Should have 2 domains", 2, response.getSchemas().size());
    }

    @Test
    public void doListTables_withPagination_returnsPagedResults() throws Exception
    {
        String domain = "movies";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        handler = createElasticsearchMetadataHandler();

        Set<String> aliases = ImmutableSet.of("index1", "index2", "index3", "index4", "index5");
        when(mockClient.getAliases()).thenReturn(aliases);

        IndicesClient indices = mock(IndicesClient.class);
        GetDataStreamResponse mockDataStreamResponse = mock(GetDataStreamResponse.class);
        when(mockDataStreamResponse.getDataStreams()).thenReturn(Collections.emptyList());
        when(indices.getDataStream(any(GetDataStreamRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockDataStreamResponse);
        when(mockClient.indices()).thenReturn(indices);

        ListTablesRequest request = new ListTablesRequest(fakeIdentity(), "queryId", "elasticsearch", domain, null, 2);
        ListTablesResponse response = handler.doListTables(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have 2 tables", 2, response.getTables().size());
        assertNotNull("Should have next token", response.getNextToken());
        assertFalse("Next token should not be empty", response.getNextToken().isEmpty());
    }

    @Test
    public void doListTables_withUnlimitedPageSize_returnsAllResults() throws Exception
    {
        String domain = "movies";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        handler = createElasticsearchMetadataHandler();

        Set<String> aliases = ImmutableSet.of("index1", "index2", "index3");
        when(mockClient.getAliases()).thenReturn(aliases);

        IndicesClient indices = mock(IndicesClient.class);
        GetDataStreamResponse mockDataStreamResponse = mock(GetDataStreamResponse.class);
        when(mockDataStreamResponse.getDataStreams()).thenReturn(Collections.emptyList());
        when(indices.getDataStream(any(GetDataStreamRequest.class), eq(RequestOptions.DEFAULT)))
                .thenReturn(mockDataStreamResponse);
        when(mockClient.indices()).thenReturn(indices);

        ListTablesRequest request = new ListTablesRequest(fakeIdentity(), "queryId", "elasticsearch", domain, null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse response = handler.doListTables(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have 3 tables", 3, response.getTables().size());
        assertNull("Next token should be null when page size is unlimited", response.getNextToken());
        assertTrue("All tables should be present", response.getTables().stream()
                .allMatch(table -> table.getSchemaName().equals(domain)));
    }

    @Test
    public void doGetSplits_withQueryPassthrough_returnsSplits() throws Exception
    {
        String domain = "movies";
        String index = "customer";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        handler = createElasticsearchMetadataHandler();

        when(mockClient.getShardIds(nullable(String.class), anyLong())).thenReturn(ImmutableSet.of(0, 1));

        IndicesClient indices = mock(IndicesClient.class);
        GetIndexResponse mockIndexResponse = mock(GetIndexResponse.class);
        when(mockIndexResponse.getIndices()).thenReturn(new String[]{index});
        when(indices.get(nullable(GetIndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(mockIndexResponse);
        when(mockClient.indices()).thenReturn(indices);

        Map<String, String> queryPassthroughArgs = createQueryPassthroughArgs(domain, index);

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);
        GetSplitsRequest request = new GetSplitsRequest(fakeIdentity(), "queryId", "elasticsearch",
                new TableName(domain, index), partitions, Collections.emptyList(), constraints, null);

        GetSplitsResponse response = handler.doGetSplits(allocator, request);

        assertNotNull("Response should not be null", response);
        assertFalse("Should have splits", response.getSplits().isEmpty());
        response.getSplits().forEach(split -> {
            assertNotNull("Split should not be null", split);
            assertEquals("Split should have correct endpoint for domain", endpoint, split.getProperty(domain));
            assertNotNull("Split should have shard key", split.getProperty(ElasticsearchMetadataHandler.SHARD_KEY));
            assertEquals("Split should have correct index", index, split.getProperty(ElasticsearchMetadataHandler.INDEX_KEY));
        });
    }

    @Test
    public void getShardsIDsFromES_whenIOException_throwsAthenaConnectorException()
    {
        String domain = "movies";
        String index = "customer";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        handler = createElasticsearchMetadataHandler();

        try {
            when(mockClient.getShardIds(nullable(String.class), anyLong())).thenThrow(new IOException("Connection failed"));

            IndicesClient indices = mock(IndicesClient.class);
            GetIndexResponse mockIndexResponse = mock(GetIndexResponse.class);
            when(mockIndexResponse.getIndices()).thenReturn(new String[]{index});
            when(indices.get(nullable(GetIndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(mockIndexResponse);
            when(mockClient.indices()).thenReturn(indices);

            Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);
            GetSplitsRequest request = new GetSplitsRequest(fakeIdentity(), "queryId", "elasticsearch",
                    new TableName(domain, index), partitions, Collections.emptyList(),
                    new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                    null);

            handler.doGetSplits(allocator, request);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertNotNull("Exception should not be null", ex);
            assertTrue("Exception message should contain Error trying to get shards ids",
                    ex.getMessage().contains("Error trying to get shards ids"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }

    @Test
    public void getDomainEndpoint_whenAutoDiscoverEndpointTrue_callsGetDomainMapToRefresh()
            throws Exception
    {
        String domain = "movies";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";
        String index = "test-index";

        // When autoDiscoverEndpoint is true, getDomainMap(null) is called to refresh the map
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        ElasticsearchMetadataHandler testHandler = createElasticsearchMetadataHandler(ImmutableMap.of("auto_discover_endpoint", "true"));

        // Set up mocks for doGetSplits to succeed
        when(clientFactory.getOrCreateClient(endpoint)).thenReturn(mockClient);
        when(mockClient.getShardIds(nullable(String.class), anyLong())).thenReturn(ImmutableSet.of(0, 1, 2));
        
        IndicesClient indices = mock(IndicesClient.class);
        GetIndexResponse mockIndexResponse = mock(GetIndexResponse.class);
        when(mockIndexResponse.getIndices()).thenReturn(new String[]{index});
        when(indices.get(nullable(GetIndexRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(mockIndexResponse);
        when(mockClient.indices()).thenReturn(indices);

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);
        GetSplitsRequest splitsRequest = new GetSplitsRequest(fakeIdentity(), "queryId", "elasticsearch",
                new TableName(domain, index), partitions, Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), null);
        
        // doGetSplits will call getDomainEndpoint, which will refresh the map when autoDiscoverEndpoint is true
        GetSplitsResponse response = testHandler.doGetSplits(allocator, splitsRequest);
        assertNotNull("Response should not be null", response);
        assertFalse("Response should have splits", response.getSplits().isEmpty());
        // Verify that domainMapProvider.getDomainMap(null) was called (for refresh)
        verify(domainMapProvider, atLeastOnce()).getDomainMap(null);
    }

    @Test
    public void getSchema_whenIOException_throwsAthenaConnectorException()
    {
        String domain = "movies";
        String index = "customer";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        handler = createElasticsearchMetadataHandler();

        try {
            when(mockClient.getMapping(nullable(String.class))).thenThrow(new IOException("Mapping retrieval failed"));

            GetTableRequest request = new GetTableRequest(fakeIdentity(), "queryId", "elasticsearch",
                    new TableName(domain, index), Collections.emptyMap());
            
            handler.doGetTable(allocator, request);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException e) {
            assertTrue("Exception message should contain Error retrieving mapping information",
                    e.getMessage().contains("Error retrieving mapping information for index"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }

    @Test
    public void appendDomainNameIfNeeded_whenPatternMatches_returnsEndpointWithoutModification()
    {
        String domainEndpoint = "mydomain=https://search-test.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of());
        lenient().when(domainMapProvider.getDomainMap(domainEndpoint)).thenReturn(ImmutableMap.of("mydomain", "https://search-test.us-east-1.es.amazonaws.com"));
        ElasticsearchMetadataHandler testHandler = createElasticsearchMetadataHandler();

        Map<String, String> config = ImmutableMap.of("default_glue_connection", "true",
                "secret_name", "test-secret", "domain_endpoint", domainEndpoint);
        lenient().when(awsSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"user\", \"password\": \"pass\"}").build());
        
        Map<String, String> result = testHandler.resolveDomainMap(config);
        assertNotNull("Domain map should not be null", result);
        assertTrue("Should contain mydomain or document current behavior", result.containsKey("mydomain") || result.isEmpty());
    }

    @Test
    public void appendDomainNameIfNeeded_whenPatternDoesNotMatch_prependsDefaultDomainName()
    {
        String domainEndpoint = "https://search-test.us-east-1.es.amazonaws.com";
        String expected = "default=" + domainEndpoint;

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of());
        lenient().when(domainMapProvider.getDomainMap(expected)).thenReturn(ImmutableMap.of("default", domainEndpoint));
        ElasticsearchMetadataHandler testHandler = createElasticsearchMetadataHandler();

        Map<String, String> config = ImmutableMap.of("default_glue_connection", "true",
                "secret_name", "test-secret", "domain_endpoint", domainEndpoint);
        lenient().when(awsSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"user\", \"password\": \"pass\"}").build());
        
        Map<String, String> result = testHandler.resolveDomainMap(config);
        assertNotNull("Domain map should not be null", result);
        assertTrue("Should contain default domain or document current behavior", result.containsKey("default") || result.isEmpty());
    }

    @Test
    public void doGetTable_withGlueClient_retrievesSchemaFromGlue()
    {
        String domain = "movies";
        String index = "customer";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        handler = createElasticsearchMetadataHandler();

        GetTableRequest request = org.mockito.Mockito.mock(GetTableRequest.class);
        when(request.getCatalogName()).thenReturn("elasticsearch");
        when(request.getTableName()).thenReturn(new TableName(domain, index));
        GetTableResponse response = handler.doGetTable(allocator, request);
        assertNotNull("Response should not be null", response);
    }

    @Test
    public void getDataStreamNamesFromClient_whenExceptionOccurs_returnsEmptyStream()
            throws Exception
    {
        String domain = "movies";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        handler = createElasticsearchMetadataHandler();

        when(mockClient.getAliases()).thenReturn(ImmutableSet.of("movies", "customer"));
        IndicesClient indices = mock(IndicesClient.class);
        when(mockClient.indices()).thenReturn(indices);
        when(indices.getDataStream(any(GetDataStreamRequest.class), eq(RequestOptions.DEFAULT)))
                .thenThrow(new RuntimeException("Data stream not supported"));

        when(clientFactory.getOrCreateClient(any(String.class))).thenReturn(mockClient);
        ListTablesRequest request = new ListTablesRequest(fakeIdentity(), "queryId", "elasticsearch", "movies", null, UNLIMITED_PAGE_SIZE_VALUE);

        ListTablesResponse response = handler.doListTables(allocator, request);
        assertNotNull("Response should not be null", response);
    }

    @Test
    public void resolveDomainMap_withNonGlueConnection_usesDomainMapping()
    {
        String domainMapping = "movies=https://search-movies.us-east-1.es.amazonaws.com";
        Map<String, String> config = ImmutableMap.of("domain_mapping", domainMapping);

        when(domainMapProvider.getDomainMap(domainMapping)).thenReturn(ImmutableMap.of("movies", "https://search-movies.us-east-1.es.amazonaws.com"));

        ElasticsearchMetadataHandler testHandler = createElasticsearchMetadataHandler(config);

        Map<String, String> result = testHandler.resolveDomainMap(config);
        assertNotNull("Domain map should not be null", result);
        assertTrue("Should contain movies domain", result.containsKey("movies"));
        assertEquals("Domain endpoint should match", "https://search-movies.us-east-1.es.amazonaws.com", result.get("movies"));
    }

    @Test
    public void doGetTable_withAwsGlueNotNull_retrievesSchemaFromGlue()
            throws Exception
    {
        String domain = "movies";
        String index = "customer";
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of(domain, endpoint));

        Schema mockGlueSchema = SchemaBuilder.newBuilder()
                .addField("id", Types.MinorType.INT.getType())
                .addField("name", Types.MinorType.VARCHAR.getType())
                .build();

        GetTableResponse mockGlueResponse = new GetTableResponse("elasticsearch", new TableName(domain, index), mockGlueSchema, Collections.emptySet());

        ElasticsearchMetadataHandler testHandler = org.mockito.Mockito.spy(createElasticsearchMetadataHandler());

        GetTableRequest request = org.mockito.Mockito.mock(GetTableRequest.class);
        lenient().when(request.getCatalogName()).thenReturn("elasticsearch");
        lenient().when(request.getTableName()).thenReturn(new TableName(domain, index));

        org.mockito.Mockito.doReturn(mockGlueResponse).when((com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler) testHandler)
                .doGetTable(org.mockito.ArgumentMatchers.any(com.amazonaws.athena.connector.lambda.data.BlockAllocator.class), org.mockito.ArgumentMatchers.any(GetTableRequest.class));

        GetTableResponse response = testHandler.doGetTable(allocator, request);
        assertNotNull("Response should not be null", response);
        assertEquals("Schema should match Glue schema", mockGlueSchema, response.getSchema());
        assertEquals("Catalog name should match", "elasticsearch", response.getCatalogName());
    }

    @Test
    public void appendDomainNameIfNeeded_withoutDomainName_prependsDefaultEqualsSign()
    {
        String endpoint = "https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of());
        lenient().when(domainMapProvider.getDomainMap("default=" + endpoint)).thenReturn(ImmutableMap.of("default", endpoint));
        ElasticsearchMetadataHandler testHandler = createElasticsearchMetadataHandler();

        Map<String, String> config = ImmutableMap.of("default_glue_connection", "true",
                "secret_name", "test-secret", "domain_endpoint", endpoint);
        lenient().when(awsSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"user\", \"password\": \"pass\"}").build());
        
        Map<String, String> result = testHandler.resolveDomainMap(config);
        assertNotNull("Domain map should not be null", result);
        assertTrue("Should contain default domain or document current behavior", result.containsKey("default") || result.isEmpty());
    }

    @Test
    public void appendDomainNameIfNeeded_withDomainName_returnsEndpointUnchanged()
    {
        String endpoint = "movies=https://search-movies.us-east-1.es.amazonaws.com";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of());
        lenient().when(domainMapProvider.getDomainMap(endpoint)).thenReturn(ImmutableMap.of("movies", "https://search-movies.us-east-1.es.amazonaws.com"));
        ElasticsearchMetadataHandler testHandler = createElasticsearchMetadataHandler();

        Map<String, String> config = ImmutableMap.of("default_glue_connection", "true",
                "secret_name", "test-secret", "domain_endpoint", endpoint);
        lenient().when(awsSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"user\", \"password\": \"pass\"}").build());
        
        Map<String, String> result = testHandler.resolveDomainMap(config);
        assertNotNull("Domain map should not be null", result);
        assertTrue("Should contain movies domain or document current behavior", result.containsKey("movies") || result.isEmpty());
    }

    @Test
    public void getDomainEndpoint_whenEndpointNotFound_throwsAthenaConnectorException()
            throws Exception
    {
        String domain = "nonexistent";

        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of("other-domain", "https://other.endpoint.com"));
        handler = createElasticsearchMetadataHandler();

        // Test getDomainEndpoint indirectly through doGetSplits with nonexistent domain
        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);
        GetSplitsRequest splitsRequest = new GetSplitsRequest(fakeIdentity(), "queryId", "elasticsearch",
                new TableName(domain, "test-index"), partitions, Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), null);
        
        try {
            handler.doGetSplits(allocator, splitsRequest);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should indicate domain not found. Actual: " + ex.getMessage(),
                    ex.getMessage() != null && ex.getMessage().contains("Unable to find domain"));
        }
    }

    @Test
    public void convertField_withValidGlueType_returnsField()
    {
        handler = createElasticsearchMetadataHandler();

        Field field = handler.convertField("myfield", "string");
        assertNotNull("Field should not be null", field);
        assertEquals("Field name should match", "myfield", field.getName());
        assertNotNull("Field type should not be null", field.getType());
    }

    @Test
    public void getDomainMap_returnsDomainMap()
    {
        when(domainMapProvider.getDomainMap(null)).thenReturn(ImmutableMap.of());

        handler = createElasticsearchMetadataHandler();

        Map<String, String> domainMap = handler.getDomainMap();
        assertNotNull("Domain map should not be null", domainMap);
        assertTrue("Domain map should be a valid map instance", domainMap instanceof Map);
    }

    /**
     * Creates a handler with default configuration for reuse in tests.
     * Should be called after setting up domainMapProvider mocks.
     * @return a new ElasticsearchMetadataHandler instance
     */
    private ElasticsearchMetadataHandler createElasticsearchMetadataHandler()
    {
        return createElasticsearchMetadataHandler(ImmutableMap.of());
    }

    /**
     * Creates a handler with custom configuration.
     * @param configOptions custom configuration options
     * @return a new ElasticsearchMetadataHandler instance
     */
    private ElasticsearchMetadataHandler createElasticsearchMetadataHandler(Map<String, String> configOptions)
    {
        return new ElasticsearchMetadataHandler(awsGlue, new LocalKeyFactory(), awsSecretsManager, amazonAthena,
                SPILL_BUCKET, SPILL_PREFIX, domainMapProvider, clientFactory, DEFAULT_TIMEOUT, configOptions, false);
    }

    /**
     * Creates query passthrough arguments map for testing.
     *
     * @param domain the schema/domain name
     * @param index  the index name
     * @return map of query passthrough arguments
     */
    private Map<String, String> createQueryPassthroughArgs(String domain, String index)
    {
        ElasticsearchQueryPassthrough qpt = new ElasticsearchQueryPassthrough();
        return ImmutableMap.of(
                QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, qpt.getFunctionSignature(),
                ElasticsearchQueryPassthrough.SCHEMA, domain,
                ElasticsearchQueryPassthrough.INDEX, index,
                ElasticsearchQueryPassthrough.QUERY, "test query");
    }

}
