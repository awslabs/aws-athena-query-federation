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

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneRecordHandlerTest extends TestBase {
        private static final Logger logger = LoggerFactory.getLogger(NeptuneRecordHandlerTest.class);

        private NeptuneRecordHandler handler;
        private BlockAllocatorImpl allocator;
        private Schema schemaPGVertexForRead;
        private Schema schemaPGEdgeForRead;
        private AmazonS3 amazonS3;
        private AWSSecretsManager awsSecretsManager;
        private AmazonAthena athena;
        private S3BlockSpillReader spillReader;
        private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
        private List<ByteHolder> mockS3Storage = new ArrayList<>();

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

                schemaPGVertexForRead = SchemaBuilder
                                .newBuilder()
                                .addMetadata("componenttype", "vertex")
                                .addStringField("id")
                                .addIntField("property1")
                                .addStringField("property2")
                                .addFloat8Field("property3")
                                .addBitField("property4")
                                .addBigIntField("property5")
                                .addFloat4Field("property6")
                                .addDateMilliField("property7")
                                .addStringField("property8")
                                .build();
                                
                schemaPGEdgeForRead = SchemaBuilder
                                .newBuilder()
                                .addMetadata("componenttype", "edge")
                                .addStringField("in")
                                .addStringField("out")
                                .addStringField("id")
                                .addIntField("property1")
                                .addStringField("property2")
                                .addFloat8Field("property3")
                                .addBitField("property4")
                                .addBigIntField("property5")
                                .addFloat4Field("property6")
                                .addDateMilliField("property7")
                                .build();

                allocator = new BlockAllocatorImpl();
                amazonS3 = mock(AmazonS3.class);
                awsSecretsManager = mock(AWSSecretsManager.class);
                athena = mock(AmazonAthena.class);

                when(amazonS3.putObject(any()))
                                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                                        InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                                        ByteHolder byteHolder = new ByteHolder();
                                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                                        synchronized (mockS3Storage) {
                                                mockS3Storage.add(byteHolder);
                                                logger.info("puObject: total size " + mockS3Storage.size());
                                        }
                                        return mock(PutObjectResult.class);
                                });

                when(amazonS3.getObject(nullable(String.class), nullable(String.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
                        S3Object mockObject = mock(S3Object.class);
                        ByteHolder byteHolder;
                        synchronized (mockS3Storage) {
                                byteHolder = mockS3Storage.get(0);
                                mockS3Storage.remove(0);
                                logger.info("getObject: total size " + mockS3Storage.size());
                        }
                        when(mockObject.getObjectContent()).thenReturn(
                                        new S3ObjectInputStream(new ByteArrayInputStream(byteHolder.getBytes()), null));
                        return mockObject;
                });

                handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, com.google.common.collect.ImmutableMap.of());
                spillReader = new S3BlockSpillReader(amazonS3, allocator);
        }

        /**
         * Create Mock Graph for testing
         */
        private void buildGraphTraversal() {

                GraphTraversalSource graphTraversalSource = mock(GraphTraversalSource.class);
                Client client = mock(Client.class);

                when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
                when(neptuneConnection.getTraversalSource(nullable(Client.class))).thenReturn(graphTraversalSource);

                // Build Tinker Pop Graph
                TinkerGraph tinkerGraph = TinkerGraph.open();
                // Create new Vertex objects to add to traversal for mock
                Vertex vertex1 = tinkerGraph.addVertex(T.label, "default");
                vertex1.property("property1", 10);
                vertex1.property("property2", "string1");
                vertex1.property("property3", 12.4);
                vertex1.property("property4", true);
                vertex1.property("property5", 12379878123l);
                vertex1.property("property6", 15.45);
                vertex1.property("property7", (new Date()));
                vertex1.property("Property8", "string8");

                Vertex vertex2 = tinkerGraph.addVertex(T.label, "default");
                vertex2.property("property1", 5);
                vertex2.property("property2", "string2");
                vertex2.property("property3", 20.4);
                vertex2.property("property4", true);
                vertex2.property("property5", 12379878123l);
                vertex2.property("property6", 13.4523);
                vertex2.property("property7", (new Date()));

                Vertex vertex3 = tinkerGraph.addVertex(T.label, "default");
                vertex3.property("property1", 9);
                vertex3.property("property2", "string3");
                vertex3.property("property3", 15.4);
                vertex3.property("property4", true);
                vertex3.property("property5", 12379878123l);
                vertex3.property("property6", 13.4523);
                vertex3.property("property7", (new Date()));


                //add vertex with missing property values to check for nulls
                tinkerGraph.addVertex(T.label, "default");

                //add vertex to check for conversion from int to float,double.
                Vertex vertex4 = tinkerGraph.addVertex(T.label, "default");
                vertex4.property("property3", 15);
                vertex4.property("property6", 13);

                GraphTraversal<Vertex, Vertex> vertextTraversal = (GraphTraversal<Vertex, Vertex>) tinkerGraph.traversal().V();
                when(graphTraversalSource.V()).thenReturn(vertextTraversal);

                //add edge from vertex1 to vertex2
                tinkerGraph.traversal().addE("default").from(vertex1).to(vertex2).next();

                //add edge from vertex1 to vertex2 with attributes
                tinkerGraph.traversal().addE("default").from(vertex2).to(vertex3).property(Cardinality.single, "property1", 21, 21).next();

                GraphTraversal<Edge, Edge>  edgeTraversal = (GraphTraversal<Edge, Edge>) tinkerGraph.traversal().E();
        }

        @Test
        public void doReadRecordsSpill() throws Exception {
                S3SpillLocation splitLoc = S3SpillLocation.newBuilder().withBucket(UUID.randomUUID().toString())
                                .withSplitId(UUID.randomUUID().toString()).withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true).build();

                allocator = new BlockAllocatorImpl();

                // Greater Than filter
                HashMap<String, ValueSet> constraintsMap = new HashMap<>();
                constraintsMap.put("property1",
                                SortedRangeSet.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 9)));

                buildGraphTraversal();

                ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY, DEFAULT_CATALOG, QUERY_ID, TABLE_NAME,
                schemaPGVertexForRead, Split.newBuilder(splitLoc, keyFactory.create()).build(),
                                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT), 1_500_000L, // ~1.5MB so we should see some spill
                                0L);

                RecordResponse rawResponse = handler.doReadRecords(allocator, request);
                assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

                try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
                        logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

                        assertTrue(response.getNumberBlocks() == 1);

                        int blockNum = 0;
                        for (SpillLocation next : response.getRemoteBlocks()) {
                                S3SpillLocation spillLocation = (S3SpillLocation) next;
                                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(),
                                                response.getSchema())) {
                                        logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++,
                                                        block.getRowCount());

                                        logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                                        assertNotNull(BlockUtils.rowToString(block, 0));
                                }
                        }
                }
        }

        private class ByteHolder {
                private byte[] bytes;

                public void setBytes(byte[] bytes) {
                        this.bytes = bytes;
                }

                public byte[] getBytes() {
                        return bytes;
                }
        }
}
