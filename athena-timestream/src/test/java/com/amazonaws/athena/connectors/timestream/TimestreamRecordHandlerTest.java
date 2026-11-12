/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
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
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.Row;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static com.amazonaws.athena.connectors.timestream.TestUtils.makeMockQueryResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestreamRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamRecordHandlerTest.class);

    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());

    private TimestreamRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private S3Client amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private static final String DEFAULT_CATALOG = "my_catalog";
    private static final String DEFAULT_SCHEMA = "my_schema";
    private static final String TEST_TABLE = "my_table";
    private static final String TEST_VIEW = "my_view";

    @Rule
    public TestName testName = new TestName();

    @Mock
    private TimestreamQueryClient mockClient;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

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

    @Before
    public void setUp() {
        logger.info("{}: enter", testName.getMethodName());

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(S3Client.class);

        when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        when(amazonS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("measure_value::double", Types.MinorType.FLOAT8.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("time", Types.MinorType.DATEMILLI.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField("region", Types.MinorType.VARCHAR.getType())
                .build();

        handler = new TimestreamRecordHandler(amazonS3, mockSecretsManager, mockAthena, mockClient, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        int numRowsGenerated = 1_000;
        String expectedQuery = "SELECT \"measure_name\", \"measure_value::double\", \"az\", \"time\", \"hostname\", \"region\" FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, numRowsGenerated);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() > 0);

        //ensure we actually filtered something out
        assertTrue(response.getRecords().getRowCount() < numRowsGenerated);

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        String expectedQuery = "SELECT \"measure_name\", \"measure_value::double\", \"az\", \"time\", \"hostname\", \"region\" FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, 100_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                1_500_000L, //~1.5MB so we should see some spill
                0L
        );
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                    logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }
    }

    @Test
    public void readRecordsView()
            throws Exception
    {
        logger.info("readRecordsView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("value", Types.MinorType.FLOAT8.getType())
                .addField("num_samples", Types.MinorType.BIGINT.getType())
                .addMetadata(VIEW_METADATA_FIELD, "select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"" +
                        DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" group by measure_name, az")
                .build();

        String expectedQuery = "WITH t1 AS ( select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"my_schema\".\"my_table\" group by measure_name, az )  SELECT \"measure_name\", \"az\", \"value\", \"num_samples\" FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, null).build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                "default",
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_VIEW),
                schemaForReadView,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsView: rows[{}]", response.getRecordCount());

        for (int i = 0; i < response.getRecordCount() && i < 10; i++) {
            logger.info("readRecordsView: {}", BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("readRecordsView - exit");
    }

    @Test
    public void readRecordsTimeSeriesView()
            throws Exception
    {
        logger.info("readRecordsTimeSeriesView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("region", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addFloat8Field("measure_value::double")
                                .build())
                        .build())
                .addMetadata(VIEW_METADATA_FIELD, "select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"" + DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region")
                .build();

        String expectedQuery = "WITH t1 AS ( select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"my_schema\".\"my_table\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region )  SELECT \"region\", \"az\", \"hostname\", \"cpu_utilization\" FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals("actual: " + request.queryString(), expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, null)
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                "default",
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForReadView,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsTimeSeriesView: rows[{}]", response.getRecordCount());

        for (int i = 0; i < response.getRecordCount() && i < 10; i++) {
            logger.info("readRecordsTimeSeriesView: {}", BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("readRecordsTimeSeriesView - exit");
    }

    @Test
    public void doReadRecordsNoSpillValidateTimeStamp()
            throws Exception
    {

        int numRows = 10;
        String expectedQuery = "SELECT \"measure_name\", \"measure_value::double\", \"az\", \"time\", \"hostname\", \"region\" FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, numRows, numRows, false);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a").build());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() > 0);

        Block block = response.getRecords();
        FieldReader time = block.getFieldReader("time");
        for (int i = 0; i < response.getRecordCount() && i < numRows; i++) {
            time.setPosition(i);
            assertTrue(time.readObject() instanceof LocalDateTime);
            assertEquals(TestUtils.startDate.plusDays(i).truncatedTo(ChronoUnit.MILLIS), time.readObject());
        }

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withIntAndDateDayScalars_returnsParsedValues()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("cnt_int")
                .addField("reading_date", Types.MinorType.DATEDAY.getType())
                .build();

        List<Datum> rowData = List.of(
                Datum.builder().scalarValue("42").build(),
                Datum.builder().scalarValue("2024-06-15").build());
        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(rowData).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        Block block = response.getRecords();
        FieldReader cnt = block.getFieldReader("cnt_int");
        cnt.setPosition(0);
        assertEquals(42, cnt.readInteger().intValue());

        FieldReader readingDate = block.getFieldReader("reading_date");
        readingDate.setPosition(0);
        assertTrue(readingDate.isSet());
    }

    @Test
    public void doReadRecords_withBitTrueFalseAndBigIntScalars_returnsParsedValues()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addBitField("active")
                .addBitField("inactive")
                .addBigIntField("total")
                .build();

        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(
                        Datum.builder().scalarValue("true").build(),
                        Datum.builder().scalarValue("false").build(),
                        Datum.builder().scalarValue("1000").build())).build()))
                .build();
        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        Block block = ((ReadRecordsResponse) handler.doReadRecords(allocator, newReadRecordsRequest(schema))).getRecords();
        block.getFieldReader("active").setPosition(0);
        assertTrue(block.getFieldReader("active").readBoolean());
        block.getFieldReader("inactive").setPosition(0);
        assertTrue(!block.getFieldReader("inactive").readBoolean());
        block.getFieldReader("total").setPosition(0);
        assertEquals(1000L, block.getFieldReader("total").readLong().longValue());
    }

    @Test
    public void doReadRecords_withListStructWrongTimeFieldName_returnsListValue()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("readings", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("item", Types.MinorType.STRUCT.getType())
                                .addStringField("timestamp")
                                .addFloat8Field("value")
                                .build())
                        .build())
                .build();

        Datum listDatum = Datum.builder()
                .arrayValue(List.of(Datum.builder().rowValue(Row.builder().data(List.of(
                        Datum.builder().scalarValue("2024-06-15").build(),
                        Datum.builder().scalarValue("1.5").build())).build()).build()))
                .build();
        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(listDatum)).build()))
                .build());

        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, newReadRecordsRequest(schema));
        assertEquals(1, response.getRecordCount());

        List<Map<String, Object>> readings = (List<Map<String, Object>>) response.getRecords()
                .getFieldReader("readings")
                .readObject();
        assertEquals(1, readings.size());
        Map<String, Object> element = readings.get(0);
        assertEquals("2024-06-15", element.get("timestamp").toString());
        assertEquals(1.5, ((Number) element.get("value")).doubleValue(), 0.001);
    }

    @Test
    public void doReadRecords_withNullScalars_returnsNullValues()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("name")
                .addFloat8Field("reading")
                .addBitField("active")
                .addBigIntField("total")
                .addIntField("count")
                .addField("event_time", Types.MinorType.DATEMILLI.getType())
                .addField("event_date", Types.MinorType.DATEDAY.getType())
                .build();

        List<Datum> rowData = List.of(
                Datum.builder().build(),
                Datum.builder().build(),
                Datum.builder().build(),
                Datum.builder().build(),
                Datum.builder().build(),
                Datum.builder().build(),
                Datum.builder().build());
        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(rowData).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
        assertNullValue(response.getRecords(), "name");
        assertNullValue(response.getRecords(), "reading");
        assertNullValue(response.getRecords(), "active");
        assertNullValue(response.getRecords(), "total");
        assertNullValue(response.getRecords(), "count");
        assertNullValue(response.getRecords(), "event_time");
        assertNullValue(response.getRecords(), "event_date");
    }
    
    @Test
    public void doReadRecords_withArrayColumn_returnsListValue()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("arr", Types.MinorType.LIST.getType())
                        .addIntField("item")
                        .build())
                .build();

        Datum arrDatum = Datum.builder()
                .arrayValue(List.of(
                        Datum.builder().scalarValue("100").build(),
                        Datum.builder().scalarValue("200").build()))
                .build();
        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(arrDatum)).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
        assertNotNull(response.getRecords().getFieldReader("arr").readObject());
    }

    @Test
    public void doReadRecords_withStructColumn_returnsRowValue()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("readings", Types.MinorType.STRUCT.getType())
                        .addFloat8Field("temp")
                        .addBitField("flag")
                        .build())
                .build();

        Datum readings = Datum.builder()
                .rowValue(Row.builder().data(List.of(
                        Datum.builder().scalarValue("21.5").build(),
                        Datum.builder().scalarValue("true").build())).build())
                .build();
        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(readings)).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
        assertNotNull(response.getRecords().getFieldReader("readings").readObject());
    }

    @Test
    public void doReadRecords_withNestedDateFields_returnsComplexValue()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("readings", Types.MinorType.STRUCT.getType())
                        .addField("event_time", Types.MinorType.DATEMILLI.getType(), null)
                        .addField("event_date", Types.MinorType.DATEDAY.getType(), null)
                        .build())
                .build();

        Datum readings = Datum.builder()
                .rowValue(Row.builder().data(List.of(
                        Datum.builder().scalarValue("2024-06-15 10:11:12.123").build(),
                        Datum.builder().scalarValue("2024-06-15").build())).build())
                .build();
        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(readings)).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
        Map<String, Object> rowValue = (Map<String, Object>) response.getRecords()
                .getFieldReader("readings")
                .readObject();
        assertEquals(LocalDateTime.of(2024, 6, 15, 10, 11, 12, 123_000_000), rowValue.get("event_time"));
        assertEquals((int) LocalDate.of(2024, 6, 15).toEpochDay(), ((Integer) rowValue.get("event_date")).intValue());
    }

    @Test
    public void doReadRecords_withTimeSeriesVarcharMeasure_returnsComplexValue()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("note_series", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("item", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addStringField("measure_value")
                                .build())
                        .build())
                .build();

        QueryResponse mockResult = makeMockQueryResult(schema, 1, 1, false);
        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
        assertNotNull(response.getRecords().getFieldReader("note_series").readObject());
    }

    @Test
    public void doReadRecords_withNullComplexDatums_skipsRowWrite()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("arr", Types.MinorType.LIST.getType())
                        .addIntField("item").build())
                .addField(FieldBuilder.newBuilder("st", Types.MinorType.STRUCT.getType())
                        .addFloat8Field("value").build())
                .addField(FieldBuilder.newBuilder("ts", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("item", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addFloat8Field("measure_value").build()).build())
                .build();

        Datum nullDatum = Datum.builder().nullValue(true).build();
        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(nullDatum, nullDatum, nullDatum)).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
    }

    @Test
    public void doReadRecords_withTimeSeriesIntBigintBitMeasures_writesAllThreeTimeSeriesColumns()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("ts_int", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("item", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addIntField("measure_value").build()).build())
                .addField(FieldBuilder.newBuilder("ts_bigint", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("item", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addBigIntField("measure_value").build()).build())
                .addField(FieldBuilder.newBuilder("ts_bit", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("item", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addBitField("measure_value").build()).build())
                .build();

        QueryResponse mockResult = makeMockQueryResult(schema, 1, 1, false);
        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
        assertNotNull(response.getRecords().getFieldReader("ts_int").readObject());
        assertNotNull(response.getRecords().getFieldReader("ts_bigint").readObject());
        assertNotNull(response.getRecords().getFieldReader("ts_bit").readObject());
    }

    @Test
    public void doReadRecords_withPagination_returnsTwoRecordsFromPaginatedQuery()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("col").build();

        QueryResponse page1 = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(Datum.builder().scalarValue("row1").build())).build()))
                .nextToken("page2")
                .build();
        QueryResponse page2 = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(Datum.builder().scalarValue("row2").build())).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class)))
                .thenReturn(page1)
                .thenReturn(page2);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(2, response.getRecordCount());
    }

    @Test
    public void doReadRecords_withStructContainingVarcharBigintInt_writesInfoStructField()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("info", Types.MinorType.STRUCT.getType())
                        .addStringField("name")
                        .addBigIntField("count")
                        .addIntField("cnt_int")
                        .build())
                .build();

        Datum structDatum = Datum.builder()
                .rowValue(Row.builder().data(List.of(
                        Datum.builder().scalarValue("alice").build(),
                        Datum.builder().scalarValue("9876543210").build(),
                        Datum.builder().scalarValue("42").build())).build())
                .build();
        QueryResponse mockResult = QueryResponse.builder()
                .rows(List.of(Row.builder().data(List.of(structDatum)).build()))
                .build();

        when(mockClient.query(nullable(QueryRequest.class))).thenReturn(mockResult);

        ReadRecordsRequest request = newReadRecordsRequest(schema);
        ReadRecordsResponse response = (ReadRecordsResponse) handler.doReadRecords(allocator, request);

        assertEquals(1, response.getRecordCount());
        assertNotNull(response.getRecords().getFieldReader("info").readObject());
    }

    private ReadRecordsRequest newReadRecordsRequest(Schema schema)
    {
        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        return new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schema,
                Split.newBuilder(splitLoc, keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                        DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                10000000000L,
                10000000000L);
    }

    private void assertNullValue(Block block, String fieldName)
    {
        FieldReader reader = block.getFieldReader(fieldName);
        reader.setPosition(0);
        assertTrue("Expected " + fieldName + " to be null", !reader.isSet());
    }
}
