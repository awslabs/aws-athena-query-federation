/*-
 * #%L
 * athena-lark-base
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
package com.amazonaws.athena.connectors.lark.base;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connectors.lark.base.model.response.SearchRecordsResponse;
import com.amazonaws.athena.connectors.lark.base.service.EnvVarService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import com.amazonaws.athena.connectors.lark.base.translator.RegistererExtractor;
import com.google.common.cache.LoadingCache;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BaseRecordHandlerTest {

    @Mock
    private S3Client mockS3Client;

    @Mock
    private SecretsManagerClient mockSecretsManagerClient;

    @Mock
    private AthenaClient mockAthenaClient;

    @Mock
    private EnvVarService mockEnvVarService;

    @Mock
    private LarkBaseService mockLarkBaseService;

    @Mock
    private LoadingCache<String, ThrottlingInvoker> mockInvokerCache;

    @Mock
    private ThrottlingInvoker mockInvoker;

    private TestRecordHandler handler;
    private BlockAllocator allocator;

    @Before
    public void setUp() throws Exception {
        allocator = new BlockAllocatorImpl();
        when(mockInvokerCache.get(anyString())).thenReturn(mockInvoker);

        handler = new TestRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );
    }

    @After
    public void tearDown() {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(handler);
    }

    @Test
    public void testReadWithConstraintQueryPassThrough() {
        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.isQueryPassThrough()).thenReturn(true);
        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        AthenaConnectorException exception = assertThrows(AthenaConnectorException.class,
            () -> handler.readWithConstraint(spiller, request, queryStatusChecker));
        assertTrue(exception.getMessage().contains("QueryPassthrough not supported"));
    }

    @Test
    public void testReadWithConstraintNullSpiller() {
        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        assertThrows(NullPointerException.class,
            () -> handler.readWithConstraint(null, request, queryStatusChecker));
    }

    @Test
    public void testReadWithConstraintNullRequest() {
        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        assertThrows(NullPointerException.class,
            () -> handler.readWithConstraint(spiller, null, queryStatusChecker));
    }

    @Test
    public void testReadWithConstraintNullQueryStatusChecker() {
        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        BlockSpiller spiller = mock(BlockSpiller.class);

        assertThrows(NullPointerException.class,
            () -> handler.readWithConstraint(spiller, request, null));
    }

    @Test
    public void testReadWithConstraintSuccess() throws ExecutionException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Split split = Split.newBuilder(
                mock(S3SpillLocation.class),
                mock(EncryptionKey.class))
                .add(BASE_ID_PROPERTY, "testBase")
                .add(TABLE_ID_PROPERTY, "testTable")
                .add(FILTER_EXPRESSION_PROPERTY, "")
                .add(SORT_EXPRESSION_PROPERTY, "")
                .add(PAGE_SIZE_PROPERTY, "100")
                .add(EXPECTED_ROW_COUNT_PROPERTY, "10")
                .add(IS_PARALLEL_SPLIT_PROPERTY, "false")
                .add(SPLIT_START_INDEX_PROPERTY, "0")
                .add(SPLIT_END_INDEX_PROPERTY, "0")
                .add(LARK_FIELD_TYPE_MAPPING_PROPERTY, "{}")
                .build();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.isQueryPassThrough()).thenReturn(false);
        when(request.getSplit()).thenReturn(split);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        handler.readWithConstraint(spiller, request, queryStatusChecker);

        verify(mockInvokerCache, atLeastOnce()).get("testTable");
    }

    @Test
    public void testReadWithConstraintWithInvalidLarkFieldTypeMapping() throws ExecutionException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .build();

        Split split = Split.newBuilder(
                mock(S3SpillLocation.class),
                mock(EncryptionKey.class))
                .add(BASE_ID_PROPERTY, "testBase")
                .add(TABLE_ID_PROPERTY, "testTable")
                .add(FILTER_EXPRESSION_PROPERTY, "")
                .add(SORT_EXPRESSION_PROPERTY, "")
                .add(PAGE_SIZE_PROPERTY, "100")
                .add(EXPECTED_ROW_COUNT_PROPERTY, "10")
                .add(IS_PARALLEL_SPLIT_PROPERTY, "false")
                .add(SPLIT_START_INDEX_PROPERTY, "0")
                .add(SPLIT_END_INDEX_PROPERTY, "0")
                .add(LARK_FIELD_TYPE_MAPPING_PROPERTY, "invalid-json")
                .build();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.isQueryPassThrough()).thenReturn(false);
        when(request.getSplit()).thenReturn(split);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        // Should not throw, just log warning
        handler.readWithConstraint(spiller, request, queryStatusChecker);

        verify(mockInvokerCache, atLeastOnce()).get("testTable");
    }

    @Test
    public void testGetIteratorBasicScenario() throws Exception {
        // Mock response with one page
        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(false)
                        .pageToken(null)
                        .total(1)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        Map<String, Object> record = iterator.next();
        assertEquals("rec1", record.get(RESERVED_RECORD_ID));
        assertEquals("tableId", record.get(RESERVED_TABLE_ID));
        assertEquals("baseId", record.get(RESERVED_BASE_ID));
        assertEquals("value1", record.get("field1"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorMultiplePages() throws Exception {
        // First page
        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        SearchRecordsResponse response1 = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(true)
                        .pageToken("token1")
                        .total(2)
                        .build())
                .build();

        // Second page
        SearchRecordsResponse.RecordItem item2 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec2")
                .fields(Map.of("field1", "value2"))
                .build();

        SearchRecordsResponse response2 = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item2))
                        .hasMore(false)
                        .pageToken(null)
                        .total(2)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response1, response2);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        Map<String, Object> record1 = iterator.next();
        assertEquals("rec1", record1.get(RESERVED_RECORD_ID));

        assertTrue(iterator.hasNext());
        Map<String, Object> record2 = iterator.next();
        assertEquals("rec2", record2.get(RESERVED_RECORD_ID));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithExpectedRowCount() throws Exception {
        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(true)
                        .pageToken("token1")
                        .total(10)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                1, // Expected only 1 row
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        iterator.next();

        // Should stop after expected count
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorEmptyResponse() throws Exception {
        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(Collections.emptyList())
                        .hasMore(false)
                        .pageToken(null)
                        .total(0)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithException() throws Exception {
        when(mockInvoker.invoke(any())).thenThrow(new RuntimeException("API Error"));
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        // After the fix, exceptions should be handled gracefully and return false instead of throwing
        assertFalse("Iterator should return false when API throws exception", iterator.hasNext());
    }

    @Test
    public void testGetIteratorNoSuchElement() throws Exception {
        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(Collections.emptyList())
                        .hasMore(false)
                        .pageToken(null)
                        .total(0)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testGetIteratorWithParallelSplit() throws Exception {
        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(false)
                        .pageToken(null)
                        .total(1)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);
        when(mockEnvVarService.isActivateParallelSplit()).thenReturn(true);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                true, // parallel split
                1,
                100,
                "",
                "",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        Map<String, Object> record = iterator.next();
        assertEquals("rec1", record.get(RESERVED_RECORD_ID));
    }

    @Test
    public void testGetIteratorWithDebugLogging() throws Exception {
        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(false)
                        .pageToken(null)
                        .total(1)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "filter",
                "sort",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        iterator.next();
    }

    @Test
    public void testGetIteratorWithNullResponse() throws Exception {
        when(mockInvoker.invoke(any())).thenReturn(null);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithNullItems() throws Exception {
        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(null)
                        .hasMore(false)
                        .pageToken(null)
                        .total(0)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithEmptyPageToken() throws Exception {
        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(true)
                        .pageToken("")
                        .total(1)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithHashMapFields() throws Exception {
        HashMap<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");

        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(fields)
                .build();

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(false)
                        .pageToken(null)
                        .total(1)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        Map<String, Object> record = iterator.next();
        assertEquals("rec1", record.get(RESERVED_RECORD_ID));
    }

    @Test
    public void testGetIteratorExceedsExpectedRowCount() throws Exception {
        // A single page can carry more records than expectedRowCountForSplit asks for (e.g. the split
        // planner estimated 5 rows for this split, but one page fetch returns 10 because the API
        // doesn't slice mid-page). The iterator must stop emitting exactly at expectedRowCountForSplit
        // rather than yielding every record it happened to fetch - otherwise a split can emit more
        // rows than Athena's split-planning phase accounted for.
        List<SearchRecordsResponse.RecordItem> items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            items.add(SearchRecordsResponse.RecordItem.builder()
                    .recordId("rec" + i)
                    .fields(Map.of("field1", "value" + i))
                    .build());
        }

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(items)
                        .hasMore(true)
                        .pageToken("token1")
                        .total(20)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                5,
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(5, count);
    }

    @Test
    public void testGetIteratorWithNullFilterExpression() throws Exception {
        SearchRecordsResponse.RecordItem item1 = SearchRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(false)
                        .pageToken(null)
                        .total(1)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                0,
                false,
                0,
                0,
                null,
                "",
                Collections.emptyMap()
        );

        assertTrue(iterator.hasNext());
        iterator.next();
    }

    @Test
    public void testWriteItemsToBlockIntegration() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("name")
                .addIntField("age")
                .build();

        Map<String, Object> record1 = new HashMap<>();
        record1.put("name", "John");
        record1.put("age", 30);

        List<Map<String, Object>> records = List.of(record1);

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker, records.iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testWriteItemsToBlockWithDebugLogging() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("field1")
                .build();

        Map<String, Object> record = new HashMap<>();
        record.put("field1", "value1");

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker, List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testWriteItemsToBlockWithException() {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("field1")
                .build();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        Map<String, Object> record = new HashMap<>();
        record.put("field1", "value1");

        // Test with exception during row writer build (by using invalid schema setup)
        RegistererExtractor registererExtractor = mock(RegistererExtractor.class);
        doThrow(new RuntimeException("Test exception")).when(registererExtractor).registerExtractorsForSchema(any(), any());

        assertThrows(RuntimeException.class, () ->
                realHandler.writeItemsToBlock(spiller, request, queryStatusChecker, List.of(record).iterator(), registererExtractor));
    }

    @Test
    public void testReadWithConstraintWithException() throws ExecutionException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .build();

        Split split = Split.newBuilder(
                mock(S3SpillLocation.class),
                mock(EncryptionKey.class))
                .add(BASE_ID_PROPERTY, "testBase")
                .add(TABLE_ID_PROPERTY, "testTable")
                .add(FILTER_EXPRESSION_PROPERTY, "")
                .add(SORT_EXPRESSION_PROPERTY, "")
                .add(PAGE_SIZE_PROPERTY, "100")
                .add(EXPECTED_ROW_COUNT_PROPERTY, "10")
                .add(IS_PARALLEL_SPLIT_PROPERTY, "false")
                .add(SPLIT_START_INDEX_PROPERTY, "0")
                .add(SPLIT_END_INDEX_PROPERTY, "0")
                .add(LARK_FIELD_TYPE_MAPPING_PROPERTY, "{}")
                .build();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.isQueryPassThrough()).thenReturn(false);
        when(request.getSplit()).thenReturn(split);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        when(mockInvokerCache.get(anyString())).thenThrow(new ExecutionException(new RuntimeException("Cache error")));

        assertThrows(AthenaConnectorException.class,
                () -> handler.readWithConstraint(spiller, request, queryStatusChecker));
    }

    @Test
    public void testReadWithConstraintWithDebugLogging() throws ExecutionException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .build();

        Split split = Split.newBuilder(
                mock(S3SpillLocation.class),
                mock(EncryptionKey.class))
                .add(BASE_ID_PROPERTY, "testBase")
                .add(TABLE_ID_PROPERTY, "testTable")
                .add(FILTER_EXPRESSION_PROPERTY, "")
                .add(SORT_EXPRESSION_PROPERTY, "")
                .add(PAGE_SIZE_PROPERTY, "100")
                .add(EXPECTED_ROW_COUNT_PROPERTY, "10")
                .add(IS_PARALLEL_SPLIT_PROPERTY, "false")
                .add(SPLIT_START_INDEX_PROPERTY, "0")
                .add(SPLIT_END_INDEX_PROPERTY, "0")
                .add(LARK_FIELD_TYPE_MAPPING_PROPERTY, "{}")
                .build();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.isQueryPassThrough()).thenReturn(false);
        when(request.getSplit()).thenReturn(split);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        handler.readWithConstraint(spiller, request, queryStatusChecker);

        verify(mockInvokerCache, atLeastOnce()).get("testTable");
    }

    @Test
    public void testProcessRecordsWithActualRowWriter() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("name")
                .addIntField("age")
                .addBigIntField("timestamp")
                .build();

        Map<String, Object> record1 = new HashMap<>();
        record1.put("name", "John");
        record1.put("age", 30);
        record1.put("timestamp", 1234567890L);

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record1).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsWithMissingFieldsAndConstraints() throws Exception {
        // A missing field is always written as null regardless of whether the active constraint on it
        // allows null - the SDK's own ConstraintProjector correctly evaluates null against the constraint
        // (see BaseRecordHandler.processRecords), so this must not crash even when the constraint (here,
        // nullAllowed=false) would reject the row.
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("name")
                .addIntField("age")
                .build();

        Map<String, Object> record = new HashMap<>();
        record.put("name", "John");
        // age field is missing

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);

        Map<String, ValueSet> summary = new HashMap<>();
        ValueSet ageValueSet = mock(ValueSet.class);
        summary.put("age", ageValueSet);
        when(constraints.getSummary()).thenReturn(summary);
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsWithNullableFieldsAndNullAllowedConstraint() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("name")
                .addBitField("flag")
                .build();

        Map<String, Object> record = new HashMap<>();
        // Both fields missing

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);

        Map<String, ValueSet> summary = new HashMap<>();
        ValueSet nameValueSet = mock(ValueSet.class);
        summary.put("name", nameValueSet);

        ValueSet flagValueSet = mock(ValueSet.class);
        summary.put("flag", flagValueSet);

        when(constraints.getSummary()).thenReturn(summary);
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsWithDefaultValuesForAllTypes() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("varchar")
                .addIntField("int")
                .addBigIntField("bigint")
                .addFloat4Field("float4")
                .addFloat8Field("float8")
                .addDecimalField("decimal", 10, 2)
                .addBitField("bit")
                .build();

        Map<String, Object> record = new HashMap<>();
        // All fields missing - will use default values

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsWithTimestampTypes() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addDateMilliField("ts_milli")
                .addDateDayField("date_day")
                .build();

        Map<String, Object> record = new HashMap<>();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsWithListAndStructTypes() throws Exception {
        // Create schema with list and struct types using SchemaBuilder methods
        Field listField = new Field("list_field",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(new ArrowType.List()),
                List.of(Field.nullable("item", ArrowType.Utf8.INSTANCE)));

        Field structField = new Field("struct_field",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(new ArrowType.Struct()),
                List.of(Field.nullable("subfield", ArrowType.Utf8.INSTANCE)));

        Schema schema = new Schema(List.of(listField, structField));

        Map<String, Object> record = new HashMap<>();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsStopsWhenQueryNotRunning() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("name")
                .build();

        List<Map<String, Object>> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> record = new HashMap<>();
            record.put("name", "Name" + i);
            records.add(record);
        }

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        // Query stops after 3 records
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, true, true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                records.iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testGetIteratorStopsAtExpectedRowCountWithDebugLogging() throws Exception {
        List<SearchRecordsResponse.RecordItem> items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            items.add(SearchRecordsResponse.RecordItem.builder()
                    .recordId("rec" + i)
                    .fields(Map.of("field1", "value" + i))
                    .build());
        }

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(items)
                        .hasMore(true)
                        .pageToken("token1")
                        .total(20)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(response);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        Iterator<Map<String, Object>> iterator = handler.getIterator(
                "baseId",
                "tableId",
                100,
                5, // Expected only 5 rows
                false,
                0,
                0,
                "",
                "",
                Collections.emptyMap()
        );

        // First page carries more items (10) than expectedRowCountForSplit asks for (5).
        int count = 0;
        while (iterator.hasNext() && count < 15) {
            iterator.next();
            count++;
        }

        // Must stop at the expected count, not at however many the first page happened to carry.
        assertEquals(5, count);
    }

    @Test
    public void testGetIteratorWithNullConstraints() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("name")
                .build();

        Map<String, Object> record1 = new HashMap<>();
        record1.put("name", "John");

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        when(request.getConstraints()).thenReturn(null);
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record1).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsWithAdditionalArrowTypes() throws Exception {
        // Test TINYINT, SMALLINT types
        Field tinyIntField = new Field("tinyint_field",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(new ArrowType.Int(8, true)),
                null);
        Field smallIntField = new Field("smallint_field",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(new ArrowType.Int(16, true)),
                null);
        Field varbinaryField = new Field("varbinary_field",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(new ArrowType.Binary()),
                null);

        Schema schema = new Schema(List.of(tinyIntField, smallIntField, varbinaryField));

        Map<String, Object> record = new HashMap<>();
        // All fields missing - will trigger default value creation

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    @Test
    public void testProcessRecordsWithTimestampAndDurationTypes() throws Exception {
        Field timestampField = new Field("timestamp_field",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)),
                null);
        Field durationField = new Field("duration_field",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.MILLISECOND)),
                null);

        Schema schema = new Schema(List.of(timestampField, durationField));

        Map<String, Object> record = new HashMap<>();
        // All fields missing - will trigger default value creation

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true, false);

        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.writeItemsToBlock(spiller, request, queryStatusChecker,
                List.of(record).iterator(), new RegistererExtractor(Collections.emptyMap()));

        verify(spiller, atLeastOnce()).writeRows(any());
    }

    // ========== Tests for NullsFirstIterator (ORDER BY ... NULLS FIRST two-phase fetch) ==========

    @Test
    public void testNullsFirstIterator_emitsNullsBeforeNonNulls() {
        Map<String, Object> nullRow = Map.of("id", "null1");
        Map<String, Object> nonNullRow = Map.of("id", "nonnull1");

        BaseRecordHandler.NullsFirstIterator iterator = new BaseRecordHandler.NullsFirstIterator(
                List.of(nullRow).iterator(), List.of(nonNullRow).iterator(), 0);

        assertTrue(iterator.hasNext());
        assertEquals(nullRow, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(nonNullRow, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNullsFirstIterator_enforcesCombinedCapAcrossBothPhases() {
        // Each inner iterator carries its own copy of expectedRowCountForSplit (see getIterator), so
        // without its own counter this wrapper would let both phases emit up to the full cap each -
        // overshooting by up to 2x. The wrapper's own `emitted` count is what must actually bound the
        // total.
        List<Map<String, Object>> nullRows = List.of(Map.of("id", "n1"), Map.of("id", "n2"));
        List<Map<String, Object>> nonNullRows = List.of(Map.of("id", "v1"), Map.of("id", "v2"));

        BaseRecordHandler.NullsFirstIterator iterator = new BaseRecordHandler.NullsFirstIterator(
                nullRows.iterator(), nonNullRows.iterator(), 3);

        List<Object> emitted = new ArrayList<>();
        while (iterator.hasNext()) {
            emitted.add(iterator.next().get("id"));
        }

        assertEquals(List.of("n1", "n2", "v1"), emitted);
    }

    @Test
    public void testNullsFirstIterator_nullsAloneReachCap_neverConsultsNonNullIterator() {
        List<Map<String, Object>> nullRows = List.of(Map.of("id", "n1"), Map.of("id", "n2"), Map.of("id", "n3"));
        @SuppressWarnings("unchecked")
        Iterator<Map<String, Object>> nonNulls = mock(Iterator.class);

        BaseRecordHandler.NullsFirstIterator iterator = new BaseRecordHandler.NullsFirstIterator(
                nullRows.iterator(), nonNulls, 2);

        assertEquals("n1", iterator.next().get("id"));
        assertEquals("n2", iterator.next().get("id"));
        assertFalse(iterator.hasNext());
        verifyNoInteractions(nonNulls);
    }

    @Test
    public void testNullsFirstIterator_zeroExpectedRowCount_meansUnbounded() {
        // expectedRowCountForSplit <= 0 means "no cap" everywhere else in this class (see getIterator's
        // own hasNext()), so NullsFirstIterator must honor the same convention rather than treating 0 as
        // "emit nothing".
        List<Map<String, Object>> nullRows = List.of(Map.of("id", "n1"));
        List<Map<String, Object>> nonNullRows = List.of(Map.of("id", "v1"), Map.of("id", "v2"));

        BaseRecordHandler.NullsFirstIterator iterator = new BaseRecordHandler.NullsFirstIterator(
                nullRows.iterator(), nonNullRows.iterator(), 0);

        List<Object> emitted = new ArrayList<>();
        while (iterator.hasNext()) {
            emitted.add(iterator.next().get("id"));
        }
        assertEquals(List.of("n1", "v1", "v2"), emitted);
    }

    @Test
    public void testNullsFirstIterator_nextThrowsWhenExhausted() {
        BaseRecordHandler.NullsFirstIterator iterator = new BaseRecordHandler.NullsFirstIterator(
                Collections.emptyIterator(), Collections.emptyIterator(), 0);

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testReadWithConstraint_nullsFirstField_runsTwoPhaseFetchAndMergesResults() throws Exception {
        // End-to-end proof that NULLS_FIRST_FIELD_PROPERTY on the split actually routes readWithConstraint
        // into the two-phase fetch instead of a single plain getIterator call: one row from the
        // nulls-only phase and one from the Lark-sorted non-null phase must both reach the spiller.
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .build();

        Split split = Split.newBuilder(
                mock(S3SpillLocation.class),
                mock(EncryptionKey.class))
                .add(BASE_ID_PROPERTY, "testBase")
                .add(TABLE_ID_PROPERTY, "testTable")
                .add(FILTER_EXPRESSION_PROPERTY, "")
                .add(SORT_EXPRESSION_PROPERTY, "[{\"field_name\":\"Currency Field\",\"desc\":false}]")
                .add(NULLS_FIRST_FIELD_PROPERTY, "Currency Field")
                .add(PAGE_SIZE_PROPERTY, "100")
                .add(EXPECTED_ROW_COUNT_PROPERTY, "10")
                .add(IS_PARALLEL_SPLIT_PROPERTY, "false")
                .add(SPLIT_START_INDEX_PROPERTY, "0")
                .add(SPLIT_END_INDEX_PROPERTY, "0")
                .add(LARK_FIELD_TYPE_MAPPING_PROPERTY, "{}")
                .build();

        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.isQueryPassThrough()).thenReturn(false);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        when(request.getSplit()).thenReturn(split);
        when(request.getSchema()).thenReturn(schema);

        SearchRecordsResponse.RecordItem nullRowItem = SearchRecordsResponse.RecordItem.builder()
                .recordId("null-rec")
                .fields(new HashMap<>())
                .build();
        SearchRecordsResponse nullsPhaseResponse = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(nullRowItem))
                        .hasMore(false)
                        .total(1)
                        .build())
                .build();

        SearchRecordsResponse.RecordItem nonNullRowItem = SearchRecordsResponse.RecordItem.builder()
                .recordId("non-null-rec")
                .fields(Map.of("col1", "value"))
                .build();
        SearchRecordsResponse nonNullsPhaseResponse = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(List.of(nonNullRowItem))
                        .hasMore(false)
                        .total(1)
                        .build())
                .build();

        when(mockInvoker.invoke(any())).thenReturn(nullsPhaseResponse, nonNullsPhaseResponse);
        when(mockEnvVarService.isEnableDebugLogging()).thenReturn(false);

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        BaseRecordHandler realHandler = new BaseRecordHandler(
                mockS3Client,
                mockSecretsManagerClient,
                mockAthenaClient,
                Collections.emptyMap(),
                mockEnvVarService,
                mockLarkBaseService,
                mockInvokerCache
        );

        realHandler.readWithConstraint(spiller, request, queryStatusChecker);

        verify(spiller, times(2)).writeRows(any());
    }

    private static class TestRecordHandler extends BaseRecordHandler {
        private Iterator<Map<String, Object>> customIterator;

        public TestRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager,
                                AthenaClient amazonAthena, Map<String, String> configOptions,
                                EnvVarService envVarService, LarkBaseService larkBaseService,
                                LoadingCache<String, ThrottlingInvoker> invokerCache) {
            super(amazonS3, secretsManager, amazonAthena, configOptions, envVarService, larkBaseService, invokerCache);
        }

        @Override
        protected Iterator<Map<String, Object>> getIterator(String baseId, String tableId,
                                                            int pageSizeForApi, int expectedRowCountForSplit,
                                                            boolean isParallelSplit, long splitStartIndex,
                                                            long splitEndIndex, String originalFilterExpression,
                                                            String originalSortExpression,
                                                            Map<String, String> fieldNameToAthenaNameMap) {
            if (customIterator != null) {
                return customIterator;
            }
            return super.getIterator(baseId, tableId, pageSizeForApi, expectedRowCountForSplit,
                    isParallelSplit, splitStartIndex, splitEndIndex, originalFilterExpression, originalSortExpression,
                    fieldNameToAthenaNameMap);
        }

        public void setCustomIterator(Iterator<Map<String, Object>> iterator) {
            this.customIterator = iterator;
        }

        @Override
        protected void writeItemsToBlock(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                                        QueryStatusChecker queryStatusChecker,
                                        Iterator<Map<String, Object>> itemIterator,
                                        RegistererExtractor registererExtractor) {
            // Simple implementation for testing
            if (itemIterator != null) {
                while (itemIterator.hasNext()) {
                    itemIterator.next();
                }
            }
        }
    }
}
