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
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_PREFIX_TABLE_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_ENDPOINT_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.ZSET_KEYS_TABLE_PROP;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(RedisMetadataHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("id", "principal", "account");
    private String endpoint = "${endpoint}";
    private String decodedEndpoint = "endpoint:123";
    private RedisMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private Jedis mockClient;

    @Mock
    private AWSGlue mockGlue;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private JedisPoolFactory mockFactory;

    @Before
    public void setUp()
            throws Exception
    {
        when(mockFactory.getOrCreateConn(eq(decodedEndpoint))).thenReturn(mockClient);

        handler = new RedisMetadataHandler(mockGlue, new LocalKeyFactory(), mockSecretsManager, mockFactory, "bucket", "prefix");
        allocator = new BlockAllocatorImpl();

        when(mockSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    GetSecretValueRequest request = invocation.getArgumentAt(0, GetSecretValueRequest.class);
                    if ("endpoint".equalsIgnoreCase(request.getSecretId())) {
                        return new GetSecretValueResult().withSecretString(decodedEndpoint);
                    }
                    throw new RuntimeException("Unknown secret " + request.getSecretId());
                });
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
    }

    @Test
    public void doGetTableLayout()
    {
        logger.info("doGetTableLayout - enter");

        Map<String, String> props = new HashMap<>();
        props.put("key1", "value1");
        props.put("key2", "value2");

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity, "queryId", "default",
                new TableName("schema1", "table1"),
                new Constraints(new HashMap<>()),
                props);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res);
        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
        }

        assertTrue(partitions.getRowCount() > 0);
        assertEquals(props, partitions.getMetaData());

        logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());
    }

    @Test
    public void doGetSplitsZset()
    {
        logger.info("doGetSplitsPrefix: enter");

        //3 prefixes for this table
        String prefixes = "prefix1-*,prefix2-*, prefix3-*";

        //4 zsets per prefix
        when(mockClient.scan(anyString(), any(ScanParams.class))).then((InvocationOnMock invocationOnMock) -> {
            String cursor = (String) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("1", result);
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("0", result);
            }
        });

        //100 keys per zset
        when(mockClient.zcount(anyString(), anyString(), anyString())).thenReturn(200L);

        List<String> partitionCols = new ArrayList<>();

        Schema schema = SchemaBuilder.newBuilder()
                .addField("partitionId", Types.MinorType.INT.getType())
                .addMetadata(ZSET_KEYS_TABLE_PROP, prefixes)
                .addMetadata(REDIS_ENDPOINT_PROP, endpoint)
                .build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("partitionId"), 0, 0);
        partitions.setRowCount(1);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(new HashMap<>()),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplitsPrefix: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplitsPrefix: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        assertEquals("Continuation criteria violated", 120, response.getSplits().size());
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);

        verify(mockClient, times(6)).scan(anyString(), any(ScanParams.class));
        logger.info("doGetSplitsPrefix: exit");
    }

    @Test
    public void doGetSplitsPrefix()
    {
        logger.info("doGetSplitsPrefix: enter");

        List<String> partitionCols = new ArrayList<>();

        Schema schema = SchemaBuilder.newBuilder()
                .addField("partitionId", Types.MinorType.INT.getType())
                .addMetadata(KEY_PREFIX_TABLE_PROP, "prefix1-*,prefix2-*, prefix3-*")
                .build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("partitionId"), 0, 0);
        partitions.setRowCount(1);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(new HashMap<>()),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplitsPrefix: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplitsPrefix: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        assertTrue("Continuation criteria violated", response.getSplits().size() == 3);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);

        logger.info("doGetSplitsPrefix: exit");
    }
}
