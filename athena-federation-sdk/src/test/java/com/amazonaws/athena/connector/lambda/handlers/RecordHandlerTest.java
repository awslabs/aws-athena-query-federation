/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class RecordHandlerTest
{
    private RecordHandler recordHandler;
    private BlockAllocator blockAllocator;
    private final FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private static final String CATALOG = "catalog";
    private static final String QUERY_ID = "queryId";
    private S3BlockSpillReader spillReader;
    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    @Mock
    private S3Client mockS3;

    @Before
    public void setUp()
    {
        blockAllocator = new BlockAllocatorImpl();
        Map<String, String> configOptions = new HashMap<>();
        String bucket = "bucket";
        configOptions.put("spill_bucket", bucket);
        String prefix = "prefix";
        configOptions.put("spill_prefix", prefix);
        spillReader = new S3BlockSpillReader(mockS3, blockAllocator);

        recordHandler = new RecordHandler(mock(S3Client.class), mock(SecretsManagerClient.class), mock(AthenaClient.class), "test", configOptions) {
            @Override
            protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            {
                // no-op
            }
        };
    }

    @After
    public void after()
    {
        blockAllocator.close();
    }

    @Test
    public void handleRequest() throws IOException
    {
        FederationRequest invalidRequest = new GetDataSourceCapabilitiesRequest(identity, QUERY_ID, CATALOG);

        ByteArrayOutputStream invalidOutputStream = new ByteArrayOutputStream();
        ObjectMapper objectMapper = VersionedObjectMapperFactory.create(blockAllocator);
        objectMapper.writeValue(invalidOutputStream, invalidRequest);
        ByteArrayInputStream invalidInputStream = new ByteArrayInputStream(invalidOutputStream.toByteArray());
        ByteArrayOutputStream invalidTestOutputStream = new ByteArrayOutputStream();

        try {
            recordHandler.handleRequest(invalidInputStream, invalidTestOutputStream, mock(Context.class));
            fail("Expected AthenaConnectorException for invalid request type");
        }
        catch (AthenaConnectorException e) {
            assertTrue(e.getMessage().contains("Expected a RecordRequest but found"));
        }
    }
    @Test
    public void handleRequest_catchBlock() throws IOException
    {
        // Simulate an invalid input that causes deserialization to fail
        InputStream invalidInputStream = new ByteArrayInputStream("invalid json".getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            // Call the handleRequest method
            recordHandler.handleRequest(invalidInputStream, outputStream, mock(Context.class));
            fail("Expected RuntimeException due to invalid input format");
        }
        catch (RuntimeException e) {
            // Verify that the exception is logged and rethrown
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_current_cdemo_sk", SortedRangeSet.of(
                Range.range(blockAllocator, Types.MinorType.BIGINT.getType(), 100L, true, 100_000_000L, true)));

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName("testSchema", "testTable"),
                SchemaBuilder.newBuilder().build(),
                Split.newBuilder(S3SpillLocation.newBuilder()
                                        .withBucket(UUID.randomUUID().toString())
                                        .withSplitId(UUID.randomUUID().toString())
                                        .withQueryId(UUID.randomUUID().toString())
                                        .withIsDirectory(true)
                                        .build(),
                                keyFactory.create())
                        .add("splitNum", "0")
                        .add("totalNumberSplits", "10000")
                        .add("scaleFactor", "1")
                        .build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                1_500_000L, //~1.5MB so we should see some spill
                0
        );

        RecordResponse rawResponse = recordHandler.doReadRecords(blockAllocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }
    }

    @Test
    public void pingHandleRequest() throws IOException
    {
        FederationRequest pingRequest = new PingRequest(identity, CATALOG, QUERY_ID);
        ByteArrayOutputStream pingOutputStream = new ByteArrayOutputStream();
        ObjectMapper objectMapper = VersionedObjectMapperFactory.create(blockAllocator);
        objectMapper.writeValue(pingOutputStream, pingRequest);
        ByteArrayInputStream pingInputStream = new ByteArrayInputStream(pingOutputStream.toByteArray());
        ByteArrayOutputStream pingTestOutputStream = new ByteArrayOutputStream();
        recordHandler.handleRequest(pingInputStream, pingTestOutputStream, mock(Context.class));
        FederationResponse response = objectMapper.readValue(pingTestOutputStream.toByteArray(), FederationResponse.class);
        assertNotNull(response);
    }
}