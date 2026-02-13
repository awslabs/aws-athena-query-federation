/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.deltashare.client.DeltaShareClient;
import com.amazonaws.athena.connectors.deltashare.util.ParquetReaderUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeltaShareRecordHandlerTest extends TestBase
{
    @Rule
    public TestName testName = new TestName();

    @Mock
    private BlockSpiller mockSpiller;

    @Mock
    private QueryStatusChecker mockQueryStatusChecker;

    @Mock
    private DeltaShareClient mockDeltaShareClient;

    private DeltaShareRecordHandler handler;

    @Override
    public void setUp()
    {
        super.setUp();
        handler = new DeltaShareRecordHandler(configOptions);
        
        try {
            java.lang.reflect.Field clientField = DeltaShareRecordHandler.class.getDeclaredField("deltaShareClient");
            clientField.setAccessible(true);
            clientField.set(handler, mockDeltaShareClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mock client", e);
        }
    }

    @Test
    public void testGetSpillConfig()
    {
        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create()).build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        SpillConfig spillConfig = handler.getSpillConfig(request);

        assertNotNull(spillConfig);
        assertEquals(spillLocation, spillConfig.getSpillLocation());
        assertEquals(1000000L, spillConfig.getMaxBlockBytes());
        assertEquals(500000L, spillConfig.getMaxInlineBlockSize());
        assertEquals(TEST_QUERY_ID, spillConfig.getRequestId());
        assertEquals(8, spillConfig.getNumSpillThreads());
    }

    @Test
    public void testGetSpillConfigWithCustomMaxBlockSize()
    {
        Map<String, String> customConfig = new HashMap<>(configOptions);
        customConfig.put("MAX_BLOCK_SIZE_BYTES", "2000000");
        
        DeltaShareRecordHandler customHandler = new DeltaShareRecordHandler(customConfig);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create()).build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        SpillConfig spillConfig = customHandler.getSpillConfig(request);

        assertEquals(2000000L, spillConfig.getMaxBlockBytes());
    }

    @Test
    public void testReadWithConstraintStandardMode() throws Exception
    {
        JsonNode mockQueryResponse = createMockQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(mockQueryResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "test_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            verify(mockDeltaShareClient).queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        }
    }

    @Test
    public void testReadWithConstraintRowGroupMode() throws Exception
    {
        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("processing_mode", "ROW_GROUP")
            .add("presigned_url", "https://test-url.com/file.parquet")
            .add("row_group_index", "2")
            .add("total_row_groups", "5")
            .add("file_size", "134217728")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrlWithRowGroup(any(String.class), eq(mockSpiller), any(Schema.class), eq(2)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrlWithRowGroup("https://test-url.com/file.parquet", mockSpiller, request.getSchema(), 2)
            );
        }
    }

    @Test
    public void testReadWithConstraintSingleFileMode() throws Exception
    {
        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("processing_mode", "SINGLE_FILE")
            .add("presigned_url", "https://test-url.com/file.parquet")
            .add("row_group_index", "-1")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://test-url.com/file.parquet", mockSpiller, request.getSchema())
            );
        }
    }

    @Test
    public void testReadWithConstraintWithPartitionValues() throws Exception
    {
        JsonNode mockQueryResponse = createMockPartitionedQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(mockQueryResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "partition_col=value1")
            .add("processing_mode", "STANDARD")
            .add("partition_col", "value1")
            .build();

        Schema partitionedSchema = createPartitionedTestSchema();
        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            partitionedSchema,
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            Block mockBlock = mock(Block.class);
            when(mockBlock.getRowCount()).thenReturn(10);
            doNothing().when(mockSpiller).writeRows(any());

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            verify(mockDeltaShareClient).queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testReadWithConstraintWithError() throws Exception
    {
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME))
            .thenThrow(new RuntimeException("Connection failed"));

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "test_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);
    }

    @Test(expected = RuntimeException.class)
    public void testReadWithConstraintRowGroupModeWithoutUrl() throws Exception
    {
        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("processing_mode", "ROW_GROUP")
            .add("row_group_index", "2")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);
    }

    @Test
    public void testExtractPartitionValues()
    {
        Schema partitionedSchema = createPartitionedTestSchema();
        Map<String, String> splitMetadata = ImmutableMap.of(
            "partition_col", "test_value",
            "other_field", "other_value"
        );

        try {
            java.lang.reflect.Method extractMethod = DeltaShareRecordHandler.class.getDeclaredMethod(
                "extractPartitionValues", Map.class, Schema.class);
            extractMethod.setAccessible(true);
            
            Map<String, String> result = (Map<String, String>) extractMethod.invoke(handler, splitMetadata, partitionedSchema);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("test_value", result.get("partition_col"));
            assertTrue(!result.containsKey("other_field"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to test extractPartitionValues", e);
        }
    }

    @Test
    public void testExtractPartitionValuesWithNoPartitionColumns()
    {
        Schema nonPartitionedSchema = createTestSchema();
        Map<String, String> splitMetadata = ImmutableMap.of(
            "partition_col", "test_value",
            "other_field", "other_value"
        );

        try {
            java.lang.reflect.Method extractMethod = DeltaShareRecordHandler.class.getDeclaredMethod(
                "extractPartitionValues", Map.class, Schema.class);
            extractMethod.setAccessible(true);
            
            Map<String, String> result = (Map<String, String>) extractMethod.invoke(handler, splitMetadata, nonPartitionedSchema);
            
            assertNotNull(result);
            assertTrue(result.isEmpty());
        } catch (Exception e) {
            throw new RuntimeException("Failed to test extractPartitionValues", e);
        }
    }

    @Test
    public void testReadWithConstraintWithInvalidProcessingMode() throws Exception
    {
        JsonNode mockQueryResponse = createMockQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(mockQueryResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("processing_mode", "INVALID_MODE")
            .add("partition_id", "test_partition")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            verify(mockDeltaShareClient).queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        }
    }

    @Test(expected = com.fasterxml.jackson.core.JsonParseException.class)
    public void testReadWithConstraintWithMalformedQueryResponse() throws Exception
    {
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME))
            .thenReturn(objectMapper.readTree("invalid json"));

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "test_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);
    }

    @Test(expected = RuntimeException.class)
    public void testHandlerWithMissingConfiguration()
    {
        Map<String, String> invalidConfig = ImmutableMap.of(
            "endpoint", TEST_ENDPOINT,
            "token", TEST_TOKEN
        );

        new DeltaShareRecordHandler(invalidConfig);
    }

    @Test(expected = RuntimeException.class)
    public void testHandlerWithEmptyShareName()
    {
        Map<String, String> invalidConfig = ImmutableMap.of(
            "endpoint", TEST_ENDPOINT,
            "token", TEST_TOKEN,
            "share_name", ""
        );

        new DeltaShareRecordHandler(invalidConfig);
    }

    @Test
    public void testReadWithConstraintWithNullQueryResponse() throws Exception
    {
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(null);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "test_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);
    }

    @Test
    public void testReadWithConstraintWithEmptyQueryResponse() throws Exception
    {
        JsonNode emptyQueryResponse = createMockEmptyQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(emptyQueryResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "test_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);
    }

    @Test
    public void testReadWithConstraintMultipleFiles() throws Exception
    {
        JsonNode multiFileResponse = createMockMultiFileQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(multiFileResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "no_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://test-url1.com/file1.parquet", mockSpiller, request.getSchema())
            );
            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://test-url2.com/file2.parquet", mockSpiller, request.getSchema())
            );
            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://test-url3.com/file3.parquet", mockSpiller, request.getSchema())
            );
        }
    }

    @Test
    public void testReadWithConstraintMultipleFilesWithOneFailure() throws Exception
    {
        JsonNode multiFileResponse = createMockMultiFileQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(multiFileResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "no_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(eq("https://test-url1.com/file1.parquet"), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(eq("https://test-url2.com/file2.parquet"), eq(mockSpiller), any(Schema.class)))
                .thenThrow(new RuntimeException("File processing failed"));
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(eq("https://test-url3.com/file3.parquet"), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://test-url1.com/file1.parquet", mockSpiller, request.getSchema())
            );
            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://test-url2.com/file2.parquet", mockSpiller, request.getSchema())
            );
            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://test-url3.com/file3.parquet", mockSpiller, request.getSchema())
            );
        }
    }

    @Test
    public void testReadWithConstraintLargeNumberOfFiles() throws Exception
    {
        JsonNode largeFileResponse = createMockLargeFileQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(largeFileResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "no_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)),
                org.mockito.Mockito.times(15)
            );
        }
    }

    @Test
    public void testReadWithConstraintFilesWithMissingUrls() throws Exception
    {
        JsonNode malformedResponse = createMockMalformedFileQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(malformedResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "no_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://valid-url.com/file.parquet", mockSpiller, request.getSchema())
            );
        }
    }

    @Test
    public void testReadWithConstraintRealDeltaShareResponse() throws Exception
    {
        JsonNode realResponse = createMockRealDeltaShareResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(realResponse);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Split split = Split.newBuilder(spillLocation, new LocalKeyFactory().create())
            .add("partition_id", "no_partition")
            .add("processing_mode", "STANDARD")
            .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
            TEST_IDENTITY,
            TEST_CATALOG_NAME,
            TEST_QUERY_ID,
            TEST_TABLE,
            createTestSchema(),
            split,
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
            1000000L,
            500000L
        );

        try (MockedStatic<ParquetReaderUtil> mockedParquetUtil = mockStatic(ParquetReaderUtil.class)) {
            mockedParquetUtil.when(() -> ParquetReaderUtil.streamParquetFromUrl(any(String.class), eq(mockSpiller), any(Schema.class)))
                .then(invocation -> null);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://testdeltasharetablebucket-942204942515.s3-fips.us-east-1.amazonaws.com/call_center_delta_new/part-00000-e896cd28-7514-4f10-b1fc-14ba88b29d23.c000.snappy.parquet", mockSpiller, request.getSchema())
            );
            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://testdeltasharetablebucket-942204942515.s3-fips.us-east-1.amazonaws.com/call_center_delta_new/part-00001-b2f224b2-d622-4611-87c3-a42c0763a057.c000.snappy.parquet", mockSpiller, request.getSchema())
            );
            mockedParquetUtil.verify(() -> 
                ParquetReaderUtil.streamParquetFromUrl("https://testdeltasharetablebucket-942204942515.s3-fips.us-east-1.amazonaws.com/call_center_delta_new/part-00002-4a0f5a34-6b63-4f30-87e3-506050bed9f9.c000.snappy.parquet", mockSpiller, request.getSchema())
            );
        }
    }
}
