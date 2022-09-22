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
package com.amazonaws.athena.connectors.elasticsearch;

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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

/**
 * This class is used to test the ElasticsearchRecordHandler class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRecordHandlerTest.class);

    private ElasticsearchRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private Schema mapping;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private S3BlockSpillReader spillReader;
    private Split split;

    @Mock
    private AwsRestHighLevelClientFactory clientFactory;

    @Mock
    private AwsRestHighLevelClient mockClient;

    @Mock
    private SearchResponse mockResponse;

    @Mock
    private SearchResponse mockScrollResponse;

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private AWSSecretsManager awsSecretsManager;

    @Mock
    private AmazonAthena athena;

    @Mock
    PutObjectResult putObjectResult;

    @Mock
    S3Object s3Object;

    String[] expectedDocuments = {"[mytext : My favorite Sci-Fi movie is Interstellar.], [mykeyword : I love keywords.], [mylong : {11,12,13}], [myinteger : 666115], [myshort : 1972], [mybyte : 5], [mydouble : 47.5], [myscaled : 7], [myfloat : 5.6], [myhalf : 6.2], [mydatemilli : 2020-05-15T06:49:30], [mydatenano : {2020-05-15T06:50:01.457}], [myboolean : true], [mybinary : U29tZSBiaW5hcnkgYmxvYg==], [mynested : {[l1long : 357345987],[l1date : 2020-05-15T06:57:44.123],[l1nested : {[l2short : {1,2,3,4,5,6,7,8,9,10}],[l2binary : U29tZSBiaW5hcnkgYmxvYg==]}]}], [objlistouter : {}]"
            ,"[mytext : My favorite TV comedy is Seinfeld.], [mykeyword : I hate key-values.], [mylong : {14,null,16}], [myinteger : 732765666], [myshort : 1971], [mybyte : 7], [mydouble : 27.6], [myscaled : 10], [myfloat : 7.8], [myhalf : 7.3], [mydatemilli : null], [mydatenano : {2020-05-15T06:49:30.001}], [myboolean : false], [mybinary : U29tZSBiaW5hcnkgYmxvYg==], [mynested : {[l1long : 7322775555],[l1date : 2020-05-15T01:57:44.777],[l1nested : {[l2short : {11,12,13,14,15,16,null,18,19,20}],[l2binary : U29tZSBiaW5hcnkgYmxvYg==]}]}], [objlistouter : {{[objlistinner : {{[title : somebook],[hi : hi]}}],[test2 : title]}}]"};

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
                "  },\n" +
                "  \"objlistouter\": []" +
                "}\n", HashMap.class);

        Map <String, Object> document2 = new ObjectMapper().readValue(
                "{\n" +
                "  \"mytext\" : \"My favorite TV comedy is Seinfeld.\",\n" +
                "  \"mykeyword\" : \"I hate key-values.\",\n" +
                "  \"mylong\" : [\n" +
                "    \"14.5\",\n" +
                "    null,\n" +
                "    16\n" +
                "  ],\n" +
                "  \"myinteger\" : \"732765666.5\",\n" +
                "  \"myshort\" : \"1971.1\",\n" +
                "  \"mybyte\" : \"7.2\",\n" +
                "  \"mydouble\" : \"27.6\",\n" +
                "  \"myscaled\" : \"0.999\",\n" +
                "  \"myfloat\" : \"7.8\",\n" +
                "  \"myhalf\" : \"7.3\",\n" +
                "  \"mydatemilli\" : null,\n" +
                "  \"mydatenano\" : 1589525370001,\n" +
                "  \"myboolean\" : \"false\",\n" +
                "  \"mybinary\" : \"U29tZSBiaW5hcnkgYmxvYg==\",\n" +
                "  \"mynested\" : {\n" +
                "    \"l1long\" : \"7322775555\",\n" +
                "    \"l1date\" : \"2020-05-15T06:57:44.7765+05:00\",\n" +
                "    \"l1nested\" : {\n" +
                "      \"l2short\" : [\n" +
                "        11,\n" +
                "        12,\n" +
                "        13,\n" +
                "        \"14.3\",\n" +
                "        15,\n" +
                "        16.5,\n" +
                "        null,\n" +
                "        18,\n" +
                "        \"19.4\",\n" +
                "        20\n" +
                "      ],\n" +
                "      \"l2binary\" : \"U29tZSBiaW5hcnkgYmxvYg==\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"objlistouter\": [{\n" +
                "        \"objlistinner\": [{\n" +
                "            \"title\": \"somebook\",\n" +
                "            \"hi\": \"hi\"\n" +
                "        }],\n" +
                "        \"test2\": \"title\"\n" +
                "    }]"        +
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
                                        null)))))
                .addField("objlistouter", Types.MinorType.LIST.getType(),
                        ImmutableList.of(
                                new Field("objlistouter", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                                        ImmutableList.of(
                                                new Field("objlistinner", FieldType.nullable(Types.MinorType.LIST.getType()),
                                                        ImmutableList.of(new Field("objlistinner", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                                                                ImmutableList.of(
                                                                        new Field("title", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
                                                                        new Field("hi", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))))),
                                                new Field("test2", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)))))
                .build();

        allocator = new BlockAllocatorImpl();

        when(amazonS3.putObject(anyObject()))
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

        when(amazonS3.getObject(Matchers.anyString(), Matchers.anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolder.getBytes()), null));
                    return mockObject;
                });

        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        split = Split.newBuilder(makeSpillLocation(), null)
                .add("movies", "https://search-movies-ne3fcqzfipy6jcrew2wca6kyqu.us-east-1.es.amazonaws.com")
                .build();

        when(clientFactory.getOrCreateClient(anyString())).thenReturn(mockClient);
        when(mockClient.getDocument(any())).thenReturn(document1, document2);
        when(mockClient.search(any(), any())).thenReturn(mockResponse);
        when(mockScrollResponse.getHits()).thenReturn(null);
        when(mockClient.scroll(any(), any())).thenReturn(mockScrollResponse);

        handler = new ElasticsearchRecordHandler(amazonS3, awsSecretsManager, athena, clientFactory, 720, 60);

        logger.info("setUpBefore - exit");
    }

    @After
    public void after()
    {
        allocator.close();
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
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(mockResponse.getScrollId()).thenReturn("123");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("myshort", SortedRangeSet.copyOf(Types.MinorType.SMALLINT.getType(),
                ImmutableList.of(Range.range(allocator, Types.MinorType.SMALLINT.getType(),
                        (short) 1955, false, (short) 1972, true)), false));

        List<String> expectedProjection = new ArrayList<>();
        mapping.getFields().forEach(field -> expectedProjection.add(field.getName()));
        String expectedPredicate = "(_exists_:myshort) AND myshort:({1955 TO 1972])";

        ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                "elasticsearch",
                "queryId-" + System.currentTimeMillis(),
                new TableName("movies", "mishmash"),
                mapping,
                split,
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        // Capture the SearchRequest object from the call to client.getDocuments().
        // The former contains information such as the projection and predicate.
        ArgumentCaptor<SearchRequest> argumentCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(mockClient).search(argumentCaptor.capture(), any());
        SearchRequest searchRequest = argumentCaptor.getValue();
        // Get the actual projection and compare to the expected one.
        List<String> actualProjection = ImmutableList.copyOf(searchRequest.source().fetchSource().includes());
        assertEquals("Projections do not match", expectedProjection, actualProjection);
        // Get the actual predicate and compare to the expected one.
        String actualPredicate = searchRequest.source().query().queryName();
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertEquals(2, response.getRecords().getRowCount());
        for (int i = 0; i < response.getRecords().getRowCount(); ++i) {
            logger.info("doReadRecordsNoSpill - Row: {}, {}", i, BlockUtils.rowToString(response.getRecords(), i));
            assertEquals(expectedDocuments[i], BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");

        int batchSize = handler.getQueryBatchSize();
        SearchHit searchHit1[] = new SearchHit[batchSize];
        for (int i = 0; i < batchSize; ++i) {
            searchHit1[i] = new SearchHit(i + 1);
        }

        SearchHit searchHit2[] = new SearchHit[2];
        searchHit2[0] = new SearchHit(batchSize + 1);
        searchHit2[1] = new SearchHit(batchSize + 2);
        SearchHits searchHits1 =
                new SearchHits(searchHit1, new TotalHits(batchSize, TotalHits.Relation.EQUAL_TO), 4);
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
                split,
                new Constraints(constraintsMap),
                10_000L, //10KB Expect this to spill
                0L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertEquals(1, response.getNumberBlocks());

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {
                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    assertEquals(expectedDocuments.length, block.getRowCount());
                    for (int rowCount = 0; rowCount < block.getRowCount(); rowCount++) {
                        logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, rowCount));
                        assertEquals(expectedDocuments[rowCount], BlockUtils.rowToString(block, rowCount));
                    }
                }
            }
        }

        logger.info("doReadRecordsSpill: exit");
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }

    private static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("access_key_id",
            "principle",
            Collections.emptyMap(),
            Collections.emptyList());
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
