/*-
 * #%L
 * athena-example
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
package com.amazonaws.athena.connectors.example;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExampleRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandlerTest.class);

    private ExampleRecordHandler handler;
    private boolean enableTests = System.getenv("publishing") != null &&
            System.getenv("publishing").equalsIgnoreCase("true");
    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;
    private AmazonS3 amazonS3;
    private AWSSecretsManager awsSecretsManager;
    private AmazonAthena athena;
    private S3BlockSpillReader spillReader;

    @Rule
    public TestName testName = new TestName();

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        schemaForRead = SchemaBuilder.newBuilder().addIntField("year")
                .addIntField("month")
                .addIntField("day")
                .addStringField("account_id")
                .addStringField("encrypted_payload")
                .addStructField("transaction")
                .addChildField("transaction", "id", Types.MinorType.INT.getType())
                .addChildField("transaction", "completed", Types.MinorType.BIT.getType())
                .addMetadata("partitionCols", "year,month,day")
                .build();

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);
        awsSecretsManager = mock(AWSSecretsManager.class);
        athena = mock(AmazonAthena.class);

        when(amazonS3.doesObjectExist(nullable(String.class), nullable(String.class))).thenReturn(true);
        when(amazonS3.getObject(nullable(String.class), nullable(String.class)))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        S3Object mockObject = mock(S3Object.class);
                        when(mockObject.getObjectContent()).thenReturn(
                                new S3ObjectInputStream(
                                        new ByteArrayInputStream(getFakeObject()), null));
                        return mockObject;
                    }
                });

        handler = new ExampleRecordHandler(amazonS3, awsSecretsManager, athena, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail.
            //This is how we avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("doReadRecordsNoSpill: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }

        for (int i = 0; i < 2; i++) {
            Map<String, ValueSet> constraintsMap = new HashMap<>();

            ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                    "catalog",
                    "queryId-" + System.currentTimeMillis(),
                    new TableName("schema", "table"),
                    schemaForRead,
                    Split.newBuilder(makeSpillLocation(), null)
                            .add("year", "2017")
                            .add("month", "11")
                            .add("day", "1")
                            .build(),
                    new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                    100_000_000_000L, //100GB don't expect this to spill
                    100_000_000_000L
            );

            RecordResponse rawResponse = handler.doReadRecords(allocator, request);
            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
            logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

            assertTrue(response.getRecords().getRowCount() > 0);
            logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        }
    }

    private byte[] getFakeObject()
            throws UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("2017,11,1,2122792308,1755604178,false,0UTIXoWnKqtQe8y+BSHNmdEXmWfQalRQH60pobsgwws=\n");
        sb.append("2017,11,1,2030248245,747575690,false,i9AoMmLI6JidPjw/SFXduBB6HUmE8aXQLMhekhIfE1U=\n");
        sb.append("2017,11,1,23301515,1720603622,false,HWsLCXAnGFXnnjD8Nc1RbO0+5JzrhnCB/feJ/EzSxto=\n");
        sb.append("2017,11,1,1342018392,1167647466,false,lqL0mxeOeEesRY7EU95Fi6QEW92nj2mh8xyex69j+8A=\n");
        sb.append("2017,11,1,945994127,1854103174,true,C57VAyZ6Y0C+xKA2Lv6fOcIP0x6Px8BlEVBGSc74C4I=\n");
        sb.append("2017,11,1,1102797454,2117019257,true,oO0S69X+N2RSyEhlzHguZSLugO8F2cDVDpcAslg0hhQ=\n");
        sb.append("2017,11,1,862601609,392155621,true,L/Wpz4gHiRR7Sab1RCBrp4i1k+0IjUuJAV/Yn/7kZnc=\n");
        sb.append("2017,11,1,1858905353,1131234096,false,w4R3N+vN/EcwrWP7q/h2DwyhyraM1AwLbCbe26a+mQ0=\n");
        sb.append("2017,11,1,1300070253,247762646,false,cjbs6isGO0K7ib1D65VbN4lZEwQv2Y6Q/PoFZhyyacA=\n");
        sb.append("2017,11,1,843851309,1886346292,true,sb/xc+uoe/ZXRXTYIv9OTY33Rj+zSS96Mj/3LVPXvRM=\n");
        sb.append("2017,11,1,2013370128,1783091056,false,9MW9X3OUr40r4B/qeLz55yJIrvw7Gdk8RWUulNadIyw=\n");
        return sb.toString().getBytes("UTF-8");
    }

    private static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    }

    private SpillLocation makeSpillLocation()
    {
        return S3SpillLocation.newBuilder()
                .withBucket("athena-virtuoso-test")
                .withPrefix("lambda-spill")
                .withQueryId(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
    }
}
