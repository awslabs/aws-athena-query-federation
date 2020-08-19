/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneRecordHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneRecordHandlerTest.class);

    private NeptuneRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;
    private AmazonS3 amazonS3;
    private AWSSecretsManager awsSecretsManager;
    private AmazonAthena athena;
    private S3BlockSpillReader spillReader;

    @Mock
    private NeptuneConnection neptuneConnection;

    @Rule
    public TestName testName = new TestName();

    @After
    public void after() {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Before
    public void setUp() {
        logger.info("{}: enter", testName.getMethodName());

        schemaForRead = SchemaBuilder.newBuilder().addIntField("property1").addStringField("property2")
                .addFloat8Field("property3").addStringField("property4").build();

        // schemaForRead =
        // SchemaBuilder.newBuilder().addStringField("country").addStringField("country")
        // .addStringField("code").addIntField("longest").addStringField("city").addFloat8Field("lon")
        // .addStringField("type").addIntField("elev").addStringField("icao").addStringField("region")
        // .addIntField("runways").addFloat8Field("lat").addStringField("desc").build();

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);
        awsSecretsManager = mock(AWSSecretsManager.class);
        athena = mock(AmazonAthena.class);

        when(amazonS3.doesObjectExist(anyString(), anyString())).thenReturn(true);
        when(amazonS3.getObject(anyString(), anyString())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                S3Object mockObject = mock(S3Object.class);
                when(mockObject.getObjectContent())
                        .thenReturn(new S3ObjectInputStream(new ByteArrayInputStream(getFakeObject()), null));
                return mockObject;
            }
        });

        // neptuneConnection = new NeptuneConnection(System.getenv("neptune_endpoint"),
        // System.getenv("neptune_port"));

        handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection);
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @Test
    public void doReadRecordsNoSpill() throws Exception {

        GraphTraversalSource graphTraversalSource = mock(GraphTraversalSource.class);
        Client client = mock(Client.class);

        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(any(Client.class))).thenReturn(graphTraversalSource);

        // Build Tinker Pop Graph
        TinkerGraph tinkerGraph = TinkerGraph.open();
        // Create new Vertex objects to add to traversal for mock
        Vertex vertex1 = tinkerGraph.addVertex(T.label, "default");
        vertex1.property("property1", 10);
        vertex1.property("property2", "string1");
        vertex1.property("property3", 20.4f);
        vertex1.property("property4", "false");

        Vertex vertex2 = tinkerGraph.addVertex(T.label, "default");
        vertex2.property("property1", 5);
        vertex2.property("property2", "string1");
        vertex2.property("property3", 20.4f);
        vertex2.property("property4", "false");

        Vertex vertex3 = tinkerGraph.addVertex(T.label, "default");
        vertex3.property("property1", 9);
        vertex3.property("property2", "string1");
        vertex3.property("property3", 20.4f);
        vertex3.property("property4", "false");

        GraphTraversal<Vertex, Vertex> traversal = (GraphTraversal<Vertex, Vertex>) tinkerGraph.traversal().V();
        when(graphTraversalSource.V()).thenReturn(traversal);

        // 1: GREATER THAN
        // constraintsMap.put("runways",
        // SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
        // ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(),
        // 3)), false));

        // 2: LESS THAN
        // constraintsMap.put("runways",
        // SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
        // ImmutableList.of(Range.lessThan(allocator, Types.MinorType.INT.getType(),
        // 6)), false));

        // simpler one
        // constraintsMap.put("runways",
        // SortedRangeSet.of(Range.lessThan(allocator, Types.MinorType.INT.getType(),
        // 6)));

        // 3: COMBINATION OF GREATER THAN AND LESS THAN

        // SortedRangeSet intFilter = SortedRangeSet
        // .of(Range.range(allocator, Types.MinorType.INT.getType(), 3, true, 6,
        // false));

        // SortedRangeSet stringFilter = SortedRangeSet
        // .of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "US"));

        // constraintsMap.put("runways", intFilter);
        // constraintsMap.put("country", stringFilter);

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder().withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString()).withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true).build();

        HashMap<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("property1",
                SortedRangeSet.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 9)));

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY, DEFAULT_CATALOG, QUERY_ID, TABLE_NAME,
                schemaForRead, Split.newBuilder(splitLoc, null).build(), new Constraints(constraintsMap),
                100_000_000_000L, 100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue(response.getRecords().getRowCount() == 1);

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecordsSpill() throws Exception {

        GraphTraversalSource graphTraversalSource = mock(GraphTraversalSource.class);
        Client client = mock(Client.class);

        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(any(Client.class))).thenReturn(graphTraversalSource);

        // Build Tinker Pop Graph
        TinkerGraph tinkerGraph = TinkerGraph.open();
        // Create new Vertex objects to add to traversal for mock
        Vertex vertex1 = tinkerGraph.addVertex(T.label, "default");
        vertex1.property("property1", 10);
        vertex1.property("property2", "string1");
        vertex1.property("property3", 20.4f);
        vertex1.property("property4", "false");

        Vertex vertex2 = tinkerGraph.addVertex(T.label, "default");
        vertex2.property("property1", 5);
        vertex2.property("property2", "string1");
        vertex2.property("property3", 20.4f);
        vertex2.property("property4", "false");

        Vertex vertex3 = tinkerGraph.addVertex(T.label, "default");
        vertex3.property("property1", 9);
        vertex3.property("property2", "string1");
        vertex3.property("property3", 20.4f);
        vertex3.property("property4", "false");

        GraphTraversal<Vertex, Vertex> traversal = (GraphTraversal<Vertex, Vertex>) tinkerGraph.traversal().V();
        when(graphTraversalSource.V()).thenReturn(traversal);

        // 1: GREATER THAN
        // constraintsMap.put("runways",
        // SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
        // ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(),
        // 3)), false));

        // 2: LESS THAN
        // constraintsMap.put("runways",
        // SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
        // ImmutableList.of(Range.lessThan(allocator, Types.MinorType.INT.getType(),
        // 6)), false));

        // simpler one
        // constraintsMap.put("runways",
        // SortedRangeSet.of(Range.lessThan(allocator, Types.MinorType.INT.getType(),
        // 6)));

        // 3: COMBINATION OF GREATER THAN AND LESS THAN

        // SortedRangeSet intFilter = SortedRangeSet
        // .of(Range.range(allocator, Types.MinorType.INT.getType(), 3, true, 6,
        // false));

        // SortedRangeSet stringFilter = SortedRangeSet
        // .of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "US"));

        // constraintsMap.put("runways", intFilter);
        // constraintsMap.put("country", stringFilter);

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder().withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString()).withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true).build();

        HashMap<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("property1",
                SortedRangeSet.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 9)));

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY, DEFAULT_CATALOG, QUERY_ID, TABLE_NAME,
                schemaForRead, Split.newBuilder(splitLoc, null).build(), new Constraints(constraintsMap),
                100_000_000_000L, 100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue(response.getRecords().getRowCount() == 1);

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        
        // try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse)
        // rawResponse) {
        // logger.info("doReadRecordsSpill: remoteBlocks[{}]",
        // response.getRemoteBlocks().size());

        // assertTrue(response.getNumberBlocks() > 1);

        // int blockNum = 0;
        // for (SpillLocation next : response.getRemoteBlocks()) {
        // S3SpillLocation spillLocation = (S3SpillLocation) next;
        // try (Block block = spillReader.read(spillLocation,
        // response.getEncryptionKey(), response.getSchema())) {

        // logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]",
        // blockNum++, block.getRowCount());
        // // assertTrue(++blockNum < response.getRemoteBlocks().size() &&
        // block.getRowCount() > 10_000);

        // logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
        // assertNotNull(BlockUtils.rowToString(block, 0));
        // }
        // }
        // }

    }

    // TODO: Clean up
    private byte[] getFakeObject() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        // sb.append("2017,11,1,2122792308,1755604178,false,0UTIXoWnKqtQe8y+BSHNmdEXmWfQalRQH60pobsgwws=\n");
        return sb.toString().getBytes("UTF-8");
    }
}
