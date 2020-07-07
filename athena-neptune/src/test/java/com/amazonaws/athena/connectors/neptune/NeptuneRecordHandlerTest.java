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
import com.google.common.collect.ImmutableList;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class NeptuneRecordHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneRecordHandlerTest.class);

    private NeptuneRecordHandler handler;
    private boolean enableTests = System.getenv("publishing") != null
            && System.getenv("publishing").equalsIgnoreCase("true");

    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;
    private AmazonS3 amazonS3;
    private AWSSecretsManager awsSecretsManager;
    private AmazonAthena athena;
    private S3BlockSpillReader spillReader;

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

        schemaForRead = SchemaBuilder.newBuilder().addIntField("year").addStringField("country").addStringField("code")
                .addIntField("longest").addStringField("city").addFloat8Field("lon").addStringField("type")
                .addIntField("elev").addStringField("icao").addStringField("region").addIntField("runways")
                .addFloat8Field("lat").addStringField("desc").build();

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
                        .thenReturn(new S3ObjectInputStream(
                            new ByteArrayInputStream(getFakeObject()), null)
                        );
                return mockObject;
            }
        });

        neptuneConnection = new NeptuneConnection(System.getenv("neptune_endpoint"), System.getenv("neptune_port"));
        handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection);
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @Test
    public void doReadRecordsNoSpill() throws Exception {
        if (!enableTests) {
            // We do this because until you complete the tutorial these tests will fail.
            // When you attempt to publis
            // using ../toos/publish.sh ... it will set the publishing flag and force these
            // tests. This is how we
            // avoid breaking the build but still have a useful tutorial. We are also
            // duplicateing this block
            // on purpose since this is a somewhat odd pattern.
            logger.info(": Tests are disabled, to enable them set the 'publishing' environment variable "
                    + "using maven clean install -Dpublishing=true");
            return;
        }

        // Sample constraints map code to test code locally

        HashMap<String, ValueSet> constraintsMap = new HashMap<>();

        // 1: GREATER THAN
        // constraintsMap.put("runways",
        // SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
        // ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(),
        // 3)), false));

        // constraintsMap.put("runways",
        // SortedRangeSet.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(),
        // 3)));

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

        SortedRangeSet sortedRangeSet = SortedRangeSet
                .of(Range.range(allocator, Types.MinorType.INT.getType(), 3, true, 6, false));

        constraintsMap.put("runways", sortedRangeSet);

        logger.info("testing constraint map: " + constraintsMap.toString());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder().withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString()).withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true).build();

        // read request for neptune request
        ReadRecordsRequest request = new ReadRecordsRequest(
            IDENTITY, 
            DEFAULT_CATALOG,
            QUERY_ID, 
            TABLE_NAME, 
            schemaForRead,
            Split.newBuilder(splitLoc, null).build(), new Constraints(constraintsMap), 
            100_000_000_000L,
            100_000_000_000L
        );

        // internally calls readRecorsWithContrains
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() > 0);
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));

    }

    @Test
    public void doReadRecordsSpill() throws Exception {
        if (!enableTests) {
            // We do this because until you complete the tutorial these tests will fail.
            // When you attempt to publis
            // using ../toos/publish.sh ... it will set the publishing flag and force these
            // tests. This is how we
            // avoid breaking the build but still have a useful tutorial. We are also
            // duplicateing this block
            // on purpose since this is a somewhat odd pattern.
            logger.info(": Tests are disabled, to enable them set the 'publishing' environment variable "
                    + "using maven clean install -Dpublishing=true");
            return;
        }

        // Sample constraints map code to test code locally

        HashMap<String, ValueSet> constraintsMap = new HashMap<>();

        // 1: GREATER THAN
        // constraintsMap.put("runways",
        // SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
        // ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(),
        // 3)), false));

        // constraintsMap.put("runways",
        // SortedRangeSet.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(),
        // 3)));

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

        SortedRangeSet sortedRangeSet = SortedRangeSet
                .of(Range.range(allocator, Types.MinorType.INT.getType(), 3, true, 6, false));

        constraintsMap.put("runways", sortedRangeSet);

        logger.info("testing constraint map: " + constraintsMap.toString());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder().withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString()).withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true).build();

        // read request for neptune request
        ReadRecordsRequest request = new ReadRecordsRequest(
            IDENTITY, 
            DEFAULT_CATALOG,
            QUERY_ID, 
            TABLE_NAME, 
            schemaForRead,
            Split.newBuilder(splitLoc, null).build(), new Constraints(constraintsMap), 
            100_000_000_000L,
            100_000_000_000L
        );

        // internally calls readRecorsWithContrains
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);

        // try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
        //     logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

        //     assertTrue(response.getNumberBlocks() > 1);

        //     int blockNum = 0;
        //     for (SpillLocation next : response.getRemoteBlocks()) {
        //         S3SpillLocation spillLocation = (S3SpillLocation) next;
        //         try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

        //             logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
        //             // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

        //             logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
        //             assertNotNull(BlockUtils.rowToString(block, 0));
        //         }
        //     }
        // }

    }

    //TODO: Clean up 
    private byte[] getFakeObject() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        // sb.append("2017,11,1,2122792308,1755604178,false,0UTIXoWnKqtQe8y+BSHNmdEXmWfQalRQH60pobsgwws=\n");
        return sb.toString().getBytes("UTF-8");
    }
}
