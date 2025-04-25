/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.redis.lettuce.RedisCommandsWrapper;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionFactory;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionWrapper;
import com.amazonaws.athena.connectors.redis.qpt.RedisQueryPassthrough;
import com.amazonaws.athena.connectors.redis.util.MockKeyScanCursor;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.Range;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.DEFAULT_REDIS_DB_NUMBER;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_PREFIX_TABLE_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.QPT_CLUSTER_ENV_VAR;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.QPT_COLUMN_NAME;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.QPT_DB_NUMBER_ENV_VAR;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.QPT_ENDPOINT_ENV_VAR;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.QPT_SSL_ENV_VAR;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_CLUSTER_FLAG;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_DB_NUMBER;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_ENDPOINT_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_SSL_FLAG;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.VALUE_TYPE_TABLE_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.ZSET_KEYS_TABLE_PROP;
import static com.amazonaws.athena.connectors.redis.qpt.RedisQueryPassthrough.NAME;
import static com.amazonaws.athena.connectors.redis.qpt.RedisQueryPassthrough.SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisMetadataHandlerTest
    extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedisMetadataHandlerTest.class);

    private static final String LITERAL_VALUE_TYPE = "literal";
    private static final String PREFIX_1 = "prefix1-*";
    private static final String PREFIXES_MULTIPLE = "prefix1-*,prefix2-*, prefix3-*";
    private static final String TRUE_STRING = "true";
    private static final String DB_NUMBER_ONE = "1";

    private String endpoint = "${endpoint}";
    private String decodedEndpoint = "endpoint:123";
    private RedisMetadataHandler handler;
    private BlockAllocator allocator;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private RedisConnectionWrapper<String, String> mockConnection;

    @Mock
    private RedisCommandsWrapper<String, String> mockSyncCommands;

    @Mock
    private GlueClient mockGlue;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Mock
    private RedisConnectionFactory mockFactory;

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());
        Map<String, String> configOptions = ImmutableMap.<String, String>builder()
                .put(QPT_SSL_ENV_VAR, "true")
                .put(QPT_ENDPOINT_ENV_VAR, "localhost:6379")
                .put(QPT_CLUSTER_ENV_VAR, "false")
                .put(QPT_DB_NUMBER_ENV_VAR, "0")
                .build();

        when(mockFactory.getOrCreateConn(eq(decodedEndpoint), anyBoolean(), anyBoolean(), nullable(String.class))).thenReturn(mockConnection);
        when(mockConnection.sync()).thenReturn(mockSyncCommands);

        handler = new RedisMetadataHandler(mockGlue, new LocalKeyFactory(), mockSecretsManager, mockAthena, mockFactory, "bucket", "prefix", configOptions);
        allocator = new BlockAllocatorImpl();

        when(mockSecretsManager.getSecretValue(nullable(GetSecretValueRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    GetSecretValueRequest request = invocation.getArgument(0, GetSecretValueRequest.class);
                    if ("endpoint".equalsIgnoreCase(request.secretId())) {
                        return GetSecretValueResponse.builder().secretString(decodedEndpoint).build();
                    }
                    throw new RuntimeException("Unknown secret " + request.secretId());
                });
    }

    @After
    public void tearDown()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doGetTableLayout_withValidRequest_returnsPartitionsWithExpectedFields()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG,
                TABLE_NAME,
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                schema,
                new HashSet<>());

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout_withValidRequest_returnsPartitionsWithExpectedFields - {}", res);
        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout_withValidRequest_returnsPartitionsWithExpectedFields:{} {}", row, BlockUtils.rowToString(partitions, row));
        }

        assertTrue(partitions.getRowCount() > 0);
        assertEquals(7, partitions.getFields().size());

        logger.info("doGetTableLayout_withValidRequest_returnsPartitionsWithExpectedFields: partitions[{}]", partitions.getRowCount());
    }

    @Test
    public void doGetSplits_withZsetValueTypeAndMultiplePrefixes_returnsExpectedSplits()
    {
        //3 prefixes for this table
        //4 zsets per prefix
        when(mockSyncCommands.scan(nullable(ScanCursor.class), nullable(ScanArgs.class))).then((InvocationOnMock invocationOnMock) -> {
            ScanCursor cursor = (ScanCursor) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.getCursor().equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("1");
                scanCursor.setKeys(result);
                return scanCursor;
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("0");
                scanCursor.setKeys(result);
                scanCursor.setFinished(true);
                return scanCursor;
            }
        });

        //100 keys per zset
        when(mockSyncCommands.zcount(nullable(String.class), nullable(Range.class))).thenReturn(200L);

        List<String> partitionCols = new ArrayList<>();

        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, endpoint, LITERAL_VALUE_TYPE,
                null, PREFIXES_MULTIPLE, null, null, null);

        String continuationToken;
        GetSplitsRequest originalReq = new GetSplitsRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                partitionCols,
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);

        logger.info("doGetSplits_withZsetValueTypeAndMultiplePrefixes_returnsExpectedSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits_withZsetValueTypeAndMultiplePrefixes_returnsExpectedSplits: continuationToken[{}] - numSplits[{}]",
                continuationToken, response.getSplits().size());

        assertEquals("Continuation criteria violated", 120, response.getSplits().size());
        assertNull("Continuation criteria violated", response.getContinuationToken());

        verify(mockSyncCommands, times(6)).scan(nullable(ScanCursor.class), nullable(ScanArgs.class));
    }

    @Test
    public void doGetSplits_withKeyPrefixes_returnsOneSplitPerPrefix()
    {
        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, endpoint, LITERAL_VALUE_TYPE,
                PREFIXES_MULTIPLE, null, null, null, null);

        String continuationToken;
        GetSplitsRequest originalReq = new GetSplitsRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                new ArrayList<>(),
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);

        logger.info("doGetSplits_withKeyPrefixes_returnsOneSplitPerPrefix: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits_withKeyPrefixes_returnsOneSplitPerPrefix: continuationToken[{}] - numSplits[{}]",
                continuationToken, response.getSplits().size());
        
        assertEquals("Continuation criteria violated", 3, response.getSplits().size());
        assertNull("Continuation criteria violated", response.getContinuationToken());
    }

    @Test(expected = IllegalArgumentException.class)
    public void doGetQueryPassthroughSchema_whenNotQueryPassthrough_throwsIllegalArgumentException() throws Exception
    {
        GetTableRequest request = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG,
                TABLE_NAME, Collections.emptyMap());

        handler.doGetQueryPassthroughSchema(allocator, request);
    }

    @Test
    public void doGetQueryPassthroughSchema_withQPTEnabled_returnsSchemaWithQPTColumn() throws Exception
    {
        GetTableRequest request = getGetTableRequest();

        GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        assertEquals(new TableName(DEFAULT_SCHEMA, TEST_TABLE), response.getTableName());

        Schema schema = response.getSchema();
        assertNotNull("Schema should not be null", schema);

        // QPT column field check
        Field field = schema.findField(QPT_COLUMN_NAME);
        assertNotNull("QPT column should exist", field);
        assertEquals(Types.MinorType.VARCHAR.getType(), field.getType());

        // Metadata checks
        Map<String, String> metadata = schema.getCustomMetadata();
        assertEquals(LITERAL_VALUE_TYPE, metadata.get(VALUE_TYPE_TABLE_PROP));
        assertEquals("*", metadata.get(KEY_PREFIX_TABLE_PROP));
        assertEquals(TRUE_STRING, metadata.get(REDIS_SSL_FLAG));
        assertEquals("localhost:6379", metadata.get(REDIS_ENDPOINT_PROP));
        assertEquals("false", metadata.get(REDIS_CLUSTER_FLAG));
        assertEquals("0", metadata.get(REDIS_DB_NUMBER));
    }

    @Test
    public void doGetDataSourceCapabilities_withQPTEnabled_returnsCapabilities()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                IDENTITY, QUERY_ID, DEFAULT_CATALOG);

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        assertNotNull("Capabilities should not be null", response.getCapabilities());
    }

    @Test
    public void doGetDataSourceCapabilities_withoutQPTEnabled_returnsEmptyCapabilities()
    {
        Map<String, String> configOptionsWithoutQPT = ImmutableMap.of();
        RedisMetadataHandler handlerWithoutQPT = new RedisMetadataHandler(
                mockGlue, new LocalKeyFactory(), mockSecretsManager, mockAthena,
                mockFactory, "bucket", "prefix", configOptionsWithoutQPT);

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                IDENTITY, QUERY_ID, DEFAULT_CATALOG);

        GetDataSourceCapabilitiesResponse response = handlerWithoutQPT.doGetDataSourceCapabilities(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        assertNotNull("Capabilities should not be null", response.getCapabilities());
    }
    
    @Test(expected = RuntimeException.class)
    public void doGetSplits_withMissingEndpoint_throwsRuntimeException()
    {
        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, null, LITERAL_VALUE_TYPE,
                PREFIX_1, null, null, null, null);

        GetSplitsRequest req = createGetSplitsRequest(partitions);

        handler.doGetSplits(allocator, req);
    }

    @Test(expected = RuntimeException.class)
    public void doGetSplits_withMissingValueType_throwsRuntimeException()
    {
        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, endpoint, null,
                PREFIX_1, null, null, null, null);

        GetSplitsRequest req = createGetSplitsRequest(partitions);

        handler.doGetSplits(allocator, req);
    }

    @Test(expected = RuntimeException.class)
    public void doGetSplits_withMissingKeyPrefixAndZset_throwsRuntimeException()
    {
        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, endpoint, LITERAL_VALUE_TYPE,
                null, null, null, null, null);

        GetSplitsRequest req = createGetSplitsRequest(partitions);

        handler.doGetSplits(allocator, req);
    }

    @Test(expected = RuntimeException.class)
    public void doGetSplits_withMultiplePartitions_throwsRuntimeException()
    {
        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, endpoint, LITERAL_VALUE_TYPE,
                PREFIX_1, null, null, null, null);
        partitions.setRowCount(2); // Multiple partitions

        GetSplitsRequest req = createGetSplitsRequest(partitions);

        handler.doGetSplits(allocator, req);
    }

    @Test
    public void doGetSplits_withDefaultDbNumber_usesDefault()
    {
        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, endpoint, LITERAL_VALUE_TYPE,
                PREFIX_1, null, null, null, null);

        GetSplitsRequest req = createGetSplitsRequest(partitions);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertNotNull("Response should not be null", response);
        assertFalse("Should have at least one split", response.getSplits().isEmpty());
        // Verify that default db number is used in splits
        response.getSplits().forEach(split -> {
            String dbNumber = split.getProperties().get(REDIS_DB_NUMBER);
            assertEquals("Should use default db number", DEFAULT_REDIS_DB_NUMBER, dbNumber);
        });
    }
    
    @Test
    public void doGetSplits_withPrefixAndSslAndClusterFlags_setsCorrectFlags()
    {
        Schema schema = createSplitSchema();

        Block partitions = createPartitionsBlock(schema, endpoint, LITERAL_VALUE_TYPE,
                PREFIX_1, null, TRUE_STRING, TRUE_STRING, DB_NUMBER_ONE);

        GetSplitsRequest req = createGetSplitsRequest(partitions);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertNotNull("Response should not be null", response);
        assertFalse("Should have at least one split", response.getSplits().isEmpty());
        
        response.getSplits().forEach(split -> {
            Map<String, String> props = split.getProperties();
            assertEquals("SSL flag should be set", TRUE_STRING, props.get(REDIS_SSL_FLAG));
            assertEquals("Cluster flag should be set", TRUE_STRING, props.get(REDIS_CLUSTER_FLAG));
            assertEquals("DB number should be set", DB_NUMBER_ONE, props.get(REDIS_DB_NUMBER));
        });
    }

    private static GetTableRequest getGetTableRequest()
    {
        Map<String, String> queryPassthroughParameters = Map.of(
                SCHEMA_FUNCTION_NAME, "system.script",
                ENABLE_QUERY_PASSTHROUGH, "true",
                NAME, "script",
                SCHEMA, "system",
                RedisQueryPassthrough.SCRIPT, "return redis.call(\"GET\", KEYS[1])",
                RedisQueryPassthrough.KEYS, "[\"l:a\"]",
                RedisQueryPassthrough.ARGV, "[]"
        );
        return new GetTableRequest(
                IDENTITY,
                "queryId",
                DEFAULT_CATALOG,
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                queryPassthroughParameters
        );
    }

    private Schema createSplitSchema()
    {
        return SchemaBuilder.newBuilder()
                .addField("partitionId", Types.MinorType.INT.getType())
                .addStringField(REDIS_ENDPOINT_PROP)
                .addStringField(VALUE_TYPE_TABLE_PROP)
                .addStringField(KEY_PREFIX_TABLE_PROP)
                .addStringField(ZSET_KEYS_TABLE_PROP)
                .addStringField(REDIS_SSL_FLAG)
                .addStringField(REDIS_CLUSTER_FLAG)
                .addStringField(REDIS_DB_NUMBER)
                .build();
    }

    private Block createPartitionsBlock(Schema schema, String endpoint, String valueType,
                                        String keyPrefix, String zsetKeys, String sslFlag,
                                        String clusterFlag, String dbNumber)
    {
        Block partitions = allocator.createBlock(schema);
        partitions.setValue(REDIS_ENDPOINT_PROP, 0, endpoint);
        partitions.setValue(VALUE_TYPE_TABLE_PROP, 0, valueType);
        partitions.setValue(KEY_PREFIX_TABLE_PROP, 0, keyPrefix);
        partitions.setValue(ZSET_KEYS_TABLE_PROP, 0, zsetKeys);
        partitions.setValue(REDIS_SSL_FLAG, 0, sslFlag);
        partitions.setValue(REDIS_CLUSTER_FLAG, 0, clusterFlag);
        partitions.setValue(REDIS_DB_NUMBER, 0, dbNumber);
        partitions.setRowCount(1);
        return partitions;
    }

    private GetSplitsRequest createGetSplitsRequest(Block partitions)
    {
        return new GetSplitsRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG,
                TABLE_NAME, partitions, new ArrayList<>(),
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(),
                        DEFAULT_NO_LIMIT, Collections.emptyMap(), null), null);
    }
}
