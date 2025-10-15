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
import com.amazonaws.athena.connectors.lark.base.model.response.ListRecordsResponse;
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
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
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
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse response1 = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
                        .items(List.of(item1))
                        .hasMore(true)
                        .pageToken("token1")
                        .total(2)
                        .build())
                .build();

        // Second page
        ListRecordsResponse.RecordItem item2 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec2")
                .fields(Map.of("field1", "value2"))
                .build();

        ListRecordsResponse response2 = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
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
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        assertTrue(iterator.hasNext());
        iterator.next();

        // Should stop after expected count
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorEmptyResponse() throws Exception {
        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
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
                ""
        );

        assertThrows(RuntimeException.class, iterator::hasNext);
    }

    @Test
    public void testGetIteratorNoSuchElement() throws Exception {
        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testGetIteratorWithParallelSplit() throws Exception {
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        assertTrue(iterator.hasNext());
        Map<String, Object> record = iterator.next();
        assertEquals("rec1", record.get(RESERVED_RECORD_ID));
    }

    @Test
    public void testGetIteratorWithDebugLogging() throws Exception {
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                "sort"
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
                ""
        );

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithNullItems() throws Exception {
        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithEmptyPageToken() throws Exception {
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetIteratorWithHashMapFields() throws Exception {
        HashMap<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");

        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(fields)
                .build();

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        assertTrue(iterator.hasNext());
        Map<String, Object> record = iterator.next();
        assertEquals("rec1", record.get(RESERVED_RECORD_ID));
    }

    @Test
    public void testGetIteratorExceedsExpectedRowCount() throws Exception {
        List<ListRecordsResponse.RecordItem> items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            items.add(ListRecordsResponse.RecordItem.builder()
                    .recordId("rec" + i)
                    .fields(Map.of("field1", "value" + i))
                    .build());
        }

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    public void testGetIteratorWithNullFilterExpression() throws Exception {
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        assertTrue(iterator.hasNext());
        iterator.next();
    }

    @Test
    public void testWriteItemsToBlockIntegration() {
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
    public void testWriteItemsToBlockWithDebugLogging() {
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
    public void testProcessRecordsWithActualRowWriter() {
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
    public void testProcessRecordsWithMissingFieldsAndConstraints() {
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
        when(ageValueSet.isNullAllowed()).thenReturn(false);
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
    public void testProcessRecordsWithNullableFieldsAndNullAllowedConstraint() {
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
        when(nameValueSet.isNullAllowed()).thenReturn(true);
        summary.put("name", nameValueSet);

        ValueSet flagValueSet = mock(ValueSet.class);
        when(flagValueSet.isNullAllowed()).thenReturn(true);
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
    public void testProcessRecordsWithDefaultValuesForAllTypes() {
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
    public void testProcessRecordsWithTimestampTypes() {
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
    public void testProcessRecordsWithListAndStructTypes() {
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
    public void testProcessRecordsStopsWhenQueryNotRunning() {
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
        List<ListRecordsResponse.RecordItem> items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            items.add(ListRecordsResponse.RecordItem.builder()
                    .recordId("rec" + i)
                    .fields(Map.of("field1", "value" + i))
                    .build());
        }

        ListRecordsResponse response = (ListRecordsResponse) ListRecordsResponse.builder()
                .data(ListRecordsResponse.ListData.builder()
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
                ""
        );

        // Fetch all available in first page (10 items)
        int count = 0;
        while (iterator.hasNext() && count < 15) {
            iterator.next();
            count++;
        }

        // Should have gotten all 10 items even though expected was 5
        assertEquals(10, count);
    }

    @Test
    public void testGetIteratorWithNullConstraints() {
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
    public void testProcessRecordsWithAdditionalArrowTypes() {
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
    public void testProcessRecordsWithTimestampAndDurationTypes() {
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
                                                            String originalSortExpression) {
            if (customIterator != null) {
                return customIterator;
            }
            return super.getIterator(baseId, tableId, pageSizeForApi, expectedRowCountForSplit,
                    isParallelSplit, splitStartIndex, splitEndIndex, originalFilterExpression, originalSortExpression);
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
