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
package com.amazonaws.connectors.athena.elasticsearch;

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
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the ElasticsearchRecordHandler class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRecordHandlerTest.class);

    private ElasticsearchRecordHandler handler;
    private boolean enableTests = System.getenv("publishing") != null &&
            System.getenv("publishing").equalsIgnoreCase("true");
    private BlockAllocatorImpl allocator;

    private Schema mapping;

    @Mock
    private AwsRestHighLevelClientFactory clientFactory;

    @Mock
    private AwsRestHighLevelClient mockClient;

    @Mock
    private SearchResponse mockResponse;

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private AWSSecretsManager awsSecretsManager;

    @Mock
    private AmazonAthena athena;

    private S3BlockSpillReader spillReader;

    @After
    public void after()
    {
        allocator.close();
    }

    @Before
    public void setUp()
            throws IOException
    {
        logger.info("setUpBefore - enter");

        Map <String, Object> document1 = new ObjectMapper().readValue(
                "{\n" +
                "  \"mytext\" : \"My favorite Sci-Fi movie is Interstellar.\",\n" +
                "  \"mykeyword\" : \"I love keywords.\",\n" +
                "  \"mylong\" : [\n" +
                "    11,\n" +
                "    12,\n" +
                "    13\n" +
                "  ],\n" +
                "  \"myinteger\" : 666115,\n" +
                "  \"myshort\" : 1972,\n" +
                "  \"mybyte\" : [\n" +
                "    5,\n" +
                "    6\n" +
                "  ],\n" +
                "  \"mydouble\" : 47.5,\n" +
                "  \"myscaled\" : 0.666,\n" +
                "  \"myfloat\" : 5.6,\n" +
                "  \"myhalf\" : 6.2,\n" +
                "  \"mydatemilli\" : \"2020-05-15T06:49:30\",\n" +
                "  \"mydatenano\" : \"2020-05-15T06:50:01.45678\",\n" +
                "  \"myboolean\" : true,\n" +
                "  \"mybinary\" : \"U29tZSBiaW5hcnkgYmxvYg==\",\n" +
                "  \"mynested\" : {\n" +
                "    \"l1long\" : 357345987,\n" +
                "    \"l1date\" : \"2020-05-15T06:57:44.123\",\n" +
                "    \"l1nested\" : {\n" +
                "      \"l2short\" : [\n" +
                "        1,\n" +
                "        2,\n" +
                "        3,\n" +
                "        4,\n" +
                "        5,\n" +
                "        6,\n" +
                "        7,\n" +
                "        8,\n" +
                "        9,\n" +
                "        10\n" +
                "      ],\n" +
                "      \"l2binary\" : \"U29tZSBiaW5hcnkgYmxvYg==\"\n" +
                "    }\n" +
                "  }\n" +
                "}\n", HashMap.class);

        Map <String, Object> document2 = new ObjectMapper().readValue(
                "{\n" +
                "  \"mytext\" : \"My favorite Sci-Fi movie is Interstellar.\",\n" +
                "  \"mykeyword\" : \"I love keywords.\",\n" +
                "  \"mylong\" : [\n" +
                "    \"11\",\n" +
                "    12,\n" +
                "    13\n" +
                "  ],\n" +
                "  \"myinteger\" : \"666115\",\n" +
                "  \"myshort\" : \"1972\",\n" +
                "  \"mybyte\" : \"5\",\n" +
                "  \"mydouble\" : \"47.5\",\n" +
                "  \"myscaled\" : \"0.666\",\n" +
                "  \"myfloat\" : \"5.6\",\n" +
                "  \"myhalf\" : \"6.2\",\n" +
                "  \"mydatemilli\" : null,\n" +
                "  \"mydatenano\" : \"2020-05-15T06:50:01.45678\",\n" +
                "  \"myboolean\" : \"true\",\n" +
                "  \"mybinary\" : \"U29tZSBiaW5hcnkgYmxvYg==\",\n" +
                "  \"mynested\" : {\n" +
                "    \"l1long\" : \"357345987\",\n" +
                "    \"l1date\" : \"2020-05-15T06:57:44.123\",\n" +
                "    \"l1nested\" : {\n" +
                "      \"l2short\" : [\n" +
                "        1,\n" +
                "        2,\n" +
                "        3,\n" +
                "        \"4\",\n" +
                "        5,\n" +
                "        6,\n" +
                "        7,\n" +
                "        8,\n" +
                "        \"9\",\n" +
                "        10\n" +
                "      ],\n" +
                "      \"l2binary\" : \"U29tZSBiaW5hcnkgYmxvYg==\"\n" +
                "    }\n" +
                "  }\n" +
                "}\n", HashMap.class);

        mapping = SchemaBuilder.newBuilder()
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
                                ImmutableMap.of("scaling_factor", "10.51")), null))
                .addField("myfloat", Types.MinorType.FLOAT4.getType())
                .addField("myhalf", Types.MinorType.FLOAT4.getType())
                .addField("mydatemilli", Types.MinorType.DATEMILLI.getType())
                .addField(new Field("mydatenano", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mydatenano",
                                FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null))))
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

        allocator = new BlockAllocatorImpl();

        when(amazonS3.doesObjectExist(anyString(), anyString())).thenReturn(true);
        when(amazonS3.getObject(anyString(), anyString()))
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

        handler = new ElasticsearchRecordHandler(amazonS3, awsSecretsManager, athena, clientFactory);
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        when(clientFactory.getClient(anyString())).thenReturn(mockClient);
        when(mockClient.getDocuments(anyObject())).thenReturn(mockResponse);
        when(mockClient.getDocument(anyObject())).thenReturn(document1, document2);

        logger.info("setUpBefore - exit");
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        logger.info("doReadRecordsNoSpill: enter");

        SearchHit searchHit[] = new SearchHit[2];
        searchHit[0] = new SearchHit(1);
        searchHit[1] = new SearchHit(2);
        SearchHits searchHits =
                new SearchHits(searchHit, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 4);
        when(mockResponse.getHits())
                .thenReturn(searchHits);

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("myshort", SortedRangeSet.copyOf(Types.MinorType.SMALLINT.getType(),
                ImmutableList.of(Range.range(allocator, Types.MinorType.SMALLINT.getType(),
                        (short) 1955, false, (short) 1972, true)), false));

        ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                "elasticsearch",
                "queryId-" + System.currentTimeMillis(),
                new TableName("movies", "mishmash"),
                mapping,
                Split.newBuilder(makeSpillLocation(), null).build(),
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        Map<String, String> domainMap = ImmutableMap.of("movies",
                "search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com");
        ElasticsearchHelper.setDomainMapping(domainMap);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertEquals(2, response.getRecords().getRowCount());
        for (int i = 0; i < response.getRecords().getRowCount(); ++i) {
            logger.info("doReadRecordsNoSpill - Row: {}\n{}", i, BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");

        SearchHit searchHit1[] = new SearchHit[handler.getQueryBatchSize()];
        for (int i = 0; i < handler.getQueryBatchSize(); ++i) {
            searchHit1[i] = new SearchHit(i + 1);
        }

        SearchHit searchHit2[] = new SearchHit[2];
        searchHit2[0] = new SearchHit(101);
        searchHit2[1] = new SearchHit(102);
        SearchHits searchHits1 =
                new SearchHits(searchHit1, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 4);
        SearchHits searchHits2 =
                new SearchHits(searchHit2, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 4);
        when(mockResponse.getHits())
                .thenReturn(searchHits1, searchHits1, searchHits2, searchHits2);

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("myshort", SortedRangeSet.copyOf(Types.MinorType.SMALLINT.getType(),
                ImmutableList.of(Range.range(allocator, Types.MinorType.SMALLINT.getType(),
                        (short) 1955, false, (short) 1972, true)), false));

        ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                "elasticsearch",
                "queryId-" + System.currentTimeMillis(),
                new TableName("movies", "mishmash"),
                mapping,
                Split.newBuilder(makeSpillLocation(), null).build(),
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        Map<String, String> domainMap = ImmutableMap.of("movies",
                "search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com");
        ElasticsearchHelper.setDomainMapping(domainMap);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsSpill: rows[{}]", response.getRecordCount());

        assertEquals(102, response.getRecords().getRowCount());
        for (int i = 0; i < response.getRecords().getRowCount(); ++i) {
            logger.info("doReadRecordsSpill - Row: {}\n{}", i, BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("doReadRecordsSpill: exit");
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
        return new FederatedIdentity("access_key_id", "principle", "account");
    }

    private SpillLocation makeSpillLocation()
    {
        return S3SpillLocation.newBuilder()
                .withBucket("shurvitz-federation-spill-1")
                .withPrefix("lambda-spill")
                .withQueryId(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
    }
}
