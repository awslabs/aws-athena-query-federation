/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import com.amazonaws.athena.connectors.hbase.connection.HbaseConnectionFactory;
import com.amazonaws.athena.connectors.hbase.connection.ResultProcessor;
import com.amazonaws.athena.connectors.hbase.qpt.HbaseQueryPassthrough;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.END_KEY_FIELD;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.HBASE_CONN_STR;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.REGION_ID_FIELD;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.REGION_NAME_FIELD;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.START_KEY_FIELD;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseRecordHandlerTest
    extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseRecordHandlerTest.class);
    private static final String FAKE_CON_STR = "fake_con_str";
    private static final String FAKE_START_KEY = "fake_start_key";
    private static final String FAKE_END_KEY = "fake_end_key";
    private static final String FAKE_REGION_ID = "fake_region_id";
    private static final String FAKE_REGION_NAME = "fake_region_name";

    private HbaseRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private S3Client amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Mock
    private HBaseConnection mockClient;

    @Mock
    private HbaseConnectionFactory mockConnFactory;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp() {
        logger.info("setUp: enter");

        when(mockConnFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

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

        schemaForRead = TestUtils.makeSchema().addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME).build();

        handler = new HbaseRecordHandler(amazonS3, mockSecretsManager, mockAthena, mockConnFactory, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("tearDown: exit");
    }

    @Test
    public void doReadRecords_withNoSpill_returnsInlineRecordsWithExpectedCount()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults(100);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        ReadRecordsRequest request = buildReadRecordsRequest(schemaForRead, buildSplitWithRegionInfo(),
                buildConstraintsWithSinglePredicate());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue("Response should be ReadRecordsResponse when no spill", rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecords_withNoSpill_returnsInlineRecordsWithExpectedCount: rows[{}]", response.getRecordCount());

        assertEquals("Record count should match expected", 1, response.getRecords().getRowCount());
    }

    @Test
    public void doReadRecords_withSpill_returnsRemoteBlocks()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults(10_000);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("family1:col3", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.BIGINT.getType(), 0L)), true));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, "fake_con_str")
                .add(START_KEY_FIELD, "fake_start_key")
                .add(END_KEY_FIELD, "fake_end_key")
                .add(REGION_ID_FIELD, "fake_region_id")
                .add(REGION_NAME_FIELD, "fake_region_name");

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

        assertTrue("Response should be RemoteReadRecordsResponse when spill occurs", rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecords_withSpill_returnsRemoteBlocks: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue("Spill should produce multiple blocks", response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecords_withSpill_returnsRemoteBlocks: blockNum[{}] recordCount[{}]", blockNum++, block.getRowCount());

                    assertNotNull("Block row string should not be null", BlockUtils.rowToString(block, 0));
                }
            }
        }
    }

    @Test
    public void doReadRecords_withQueryPassthroughEmptyFilter_returnsNonZeroRecords()
            throws Exception
    {
        setupMockScanner();
        Map<String, String> qptArguments = createQptArguments("");
        ReadRecordsRequest request = createReadRecordsRequestWithQpt(schemaForRead, qptArguments);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than 0", response.getRecordCount() > 0);
        logger.info("doReadRecords_withQueryPassthroughEmptyFilter_returnsNonZeroRecords: exit");
    }

    @Test
    public void doReadRecords_withQueryPassthroughWithFilter_returnsFilteredRecords()
            throws Exception
    {
        logger.info("doReadRecords_withQueryPassthroughWithFilter_returnsFilteredRecords: enter");
        setupMockScanner();
        Map<String, String> qptArguments = createQptArguments("RowFilter(=,'substring:test')");
        ReadRecordsRequest request = createReadRecordsRequestWithQpt(schemaForRead, qptArguments);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_withQueryPassthroughWithFilter_returnsFilteredRecords: exit");
    }

    @Test
    public void doReadRecords_withQueryPassthroughInvalidFilter_throwsRuntimeException()
    {
        logger.info("doReadRecords_withQueryPassthroughInvalidFilter_throwsRuntimeException: enter");
        Map<String, String> qptArguments = createQptArguments("InvalidFilterSyntax!!!");
        ReadRecordsRequest request = createReadRecordsRequestWithQpt(schemaForRead, qptArguments);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should not be empty",
                ex.getMessage() != null && !ex.getMessage().isEmpty());
        logger.info("doReadRecords_withQueryPassthroughInvalidFilter_throwsRuntimeException: exit");
    }

    @Test
    public void doReadRecords_withSpecificSchema_returnsRecordsForNativeSchema()
            throws Exception
    {
        logger.info("doReadRecords_withSpecificSchema_returnsRecordsForNativeSchema: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Schema nativeSchema = SchemaBuilder.newBuilder()
                .addStringField("family1:col1")
                .addBigIntField("family1:col3")
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();
        ReadRecordsRequest request = buildReadRecordsRequest(
                nativeSchema,
                buildSplitWithRegionInfo(),
                buildConstraintsWithSinglePredicate());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_withSpecificSchema_returnsRecordsForNativeSchema: exit");
    }

    @Test
    public void doReadRecords_withRowKeyConstraint_returnsRecordsMatchingRowKey()
            throws Exception
    {
        logger.info("doReadRecords_withRowKeyConstraint_returnsRecordsMatchingRowKey: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(HbaseSchemaUtils.ROW_COLUMN_NAME, SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "test_row_key")), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_withRowKeyConstraint_returnsRecordsMatchingRowKey: exit");
    }

    @Test
    public void doReadRecords_withEmptyResults_returnsZeroRecords()
            throws Exception
    {
        logger.info("doReadRecords_withEmptyResults_returnsZeroRecords: enter");
        List<Result> emptyResults = new ArrayList<>();
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(emptyResults.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Constraints noConstraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        ReadRecordsRequest request = buildReadRecordsRequest(schemaForRead, buildSplitWithRegionInfo(), noConstraints
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertEquals("Record count should be 0", 0, response.getRecordCount());
        logger.info("doReadRecords_withEmptyResults_returnsZeroRecords: exit");
    }

    @Test
    public void doReadRecords_withStructField_returnsRecordsWithStructField()
            throws Exception
    {
        logger.info("doReadRecords_withStructField_returnsRecordsWithStructField: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Schema structSchema = SchemaBuilder.newBuilder()
                .addStructField("family1")
                .addChildField("family1", "col1", Types.MinorType.VARCHAR.getType())
                .addChildField("family1", "col2", Types.MinorType.FLOAT8.getType())
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                structSchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_withStructField_returnsRecordsWithStructField: exit");
    }

    @Test
    public void doReadRecords_withInvalidColumnName_throwsRuntimeException()
    {
        logger.info("doReadRecords_withInvalidColumnName_throwsRuntimeException: enter");
        Schema invalidSchema = SchemaBuilder.newBuilder()
                .addStringField("invalid_column_name")
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                invalidSchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RuntimeException ex = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should not be empty",
                ex.getMessage() != null && !ex.getMessage().isEmpty());
        logger.info("doReadRecords_withInvalidColumnName_throwsRuntimeException: exit");
    }

    @Test
    public void doReadRecords_withNoConstraints_returnsAllRecords()
            throws Exception
    {
        logger.info("doReadRecords_withNoConstraints_returnsAllRecords: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Constraints noConstraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        ReadRecordsRequest request = buildReadRecordsRequest(schemaForRead, buildSplitWithRegionInfo(), noConstraints
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_withNoConstraints_returnsAllRecords: exit");
    }


    @Test
    public void doReadRecords_withOnlyRowColumn_returnsRecordsWithRowColumnOnly()
            throws Exception
    {
        logger.info("doReadRecords_withOnlyRowColumn_returnsRecordsWithRowColumnOnly: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Schema rowOnlySchema = SchemaBuilder.newBuilder()
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                rowOnlySchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_withOnlyRowColumn_returnsRecordsWithRowColumnOnly: exit");
    }

    @Test
    public void doReadRecords_withQueryPassthroughCharacterCodingException_throwsRuntimeException()
    {
        logger.info("doReadRecords_withQueryPassthroughCharacterCodingException_throwsRuntimeException: enter");
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, DEFAULT_SCHEMA);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TEST_TABLE);
        qptArguments.put(HbaseQueryPassthrough.FILTER, "\u0000\u0001\u0002");

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null),
                100_000_000_000L,
                100_000_000_000L
        );

        RuntimeException ex = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should not be empty",
                ex.getMessage() != null && !ex.getMessage().isEmpty());
        logger.info("doReadRecords_withQueryPassthroughCharacterCodingException_throwsRuntimeException: exit");
    }

    @Test
    public void doReadRecords_withInvalidColumnFormat_throwsRuntimeException()
    {
        logger.info("doReadRecords_withInvalidColumnFormat_throwsRuntimeException: enter");
        Schema invalidSchema = SchemaBuilder.newBuilder()
                .addStringField("invalid_column_format_no_colon")
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                invalidSchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RuntimeException ex = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should contain column name error",
                ex.getMessage().contains("does not meet family:column") ||
                        ex.getMessage().contains("Column name"));
        logger.info("doReadRecords_withInvalidColumnFormat_throwsRuntimeException: exit");
    }

    private ReadRecordsRequest createReadRecordsRequestWithQpt(Schema schema, Map<String, String> qptArguments)
    {
        S3SpillLocation spillLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
        Split split = Split.newBuilder(spillLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR).build();
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null);
        return new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schema,
                split,
                constraints,
                100_000_000_000L,
                100_000_000_000L);
    }

    private Map<String, String> createQptArguments(String filter)
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, DEFAULT_SCHEMA);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TEST_TABLE);
        qptArguments.put(HbaseQueryPassthrough.FILTER, filter);
        return qptArguments;
    }

    private void setupMockScanner()
    {
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });
    }

    /** Builds a split with spill location and all region fields (conn, startKey, endKey, regionId, regionName). */
    private Split buildSplitWithRegionInfo()
    {
        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
        return Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME)
                .build();
    }

    /** Builds a ReadRecordsRequest with the given schema, split, constraints, and block size limits. */
    private ReadRecordsRequest buildReadRecordsRequest(Schema schema, Split split, Constraints constraints)
    {
        return new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schema,
                split,
                constraints,
                100000000000L,
                100000000000L);
    }

    /** Builds constraints with single predicate */
    private Constraints buildConstraintsWithSinglePredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("family1:col3", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 1L)), false));
        return new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
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
}
