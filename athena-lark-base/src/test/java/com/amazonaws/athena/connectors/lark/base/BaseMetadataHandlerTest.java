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

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.lark.base.metadataProvider.ExperimentalMetadataProvider;
import com.amazonaws.athena.connectors.lark.base.metadataProvider.LarkSourceMetadataProvider;
import com.amazonaws.athena.connectors.lark.base.model.TableDirectInitialized;
import com.amazonaws.athena.connectors.lark.base.model.response.SearchRecordsResponse;
import com.amazonaws.athena.connectors.lark.base.service.EnvVarService;
import com.amazonaws.athena.connectors.lark.base.service.GlueCatalogService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.PAGE_SIZE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BaseMetadataHandlerTest {

    @Mock
    private GlueClient mockGlueClient;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Mock
    private EnvVarService mockEnvVarService;

    @Mock
    private LarkBaseService mockLarkBaseService;

    @Mock
    private GlueCatalogService mockGlueCatalogService;

    @Mock
    private LarkSourceMetadataProvider mockLarkSourceMetadataProvider;

    @Mock
    private ExperimentalMetadataProvider mockExperimentalMetadataProvider;

    @Mock
    private ThrottlingInvoker mockInvoker;

    private BlockAllocator allocator;
    private BaseMetadataHandler handler;

    @Before
    public void setUp() {
        allocator = new BlockAllocatorImpl();
        EncryptionKeyFactory keyFactory = new LocalKeyFactory();
        Map<String, String> configOptions = new HashMap<>();
        List<TableDirectInitialized> mockMappingTable = Collections.emptyList();

        handler = new BaseMetadataHandler(
                mockGlueClient,
                keyFactory,
                mockSecretsManager,
                mockAthena,
                "test-bucket",
                "test-prefix",
                configOptions,
                mockEnvVarService,
                mockLarkBaseService,
                mockGlueCatalogService,
                mockMappingTable,
                mockLarkSourceMetadataProvider,
                mockExperimentalMetadataProvider,
                mockInvoker
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
    public void testDoGetTable_BlacklistedTable_ThrowsAthenaConnectorException() {
        when(mockEnvVarService.getWhitelistTables()).thenReturn("");
        when(mockEnvVarService.getBlacklistTables()).thenReturn("schemaa:table1");

        com.amazonaws.athena.connector.lambda.security.FederatedIdentity identity =
                new com.amazonaws.athena.connector.lambda.security.FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        GetTableRequest request = new GetTableRequest(identity, "queryId", "catalog",
                new com.amazonaws.athena.connector.lambda.domain.TableName("schemaA", "table1"), Collections.emptyMap());

        assertThrows(com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException.class,
                () -> handler.doGetTable(allocator, request));
    }

    @Test
    public void testDoGetTable_TableNotInWhitelistedSchema_ThrowsAthenaConnectorException() {
        when(mockEnvVarService.getWhitelistTables()).thenReturn("schemaa:table1,schemaa:table2");
        when(mockEnvVarService.getBlacklistTables()).thenReturn("");

        com.amazonaws.athena.connector.lambda.security.FederatedIdentity identity =
                new com.amazonaws.athena.connector.lambda.security.FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        GetTableRequest request = new GetTableRequest(identity, "queryId", "catalog",
                new com.amazonaws.athena.connector.lambda.domain.TableName("schemaA", "table3"), Collections.emptyMap());

        assertThrows(com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException.class,
                () -> handler.doGetTable(allocator, request));
    }

    @Test
    public void testDoGetTable_TableInWhitelistedSchema_DoesNotThrowFromAccessControlGuard() {
        when(mockEnvVarService.getWhitelistTables()).thenReturn("schemaa:table1");
        when(mockEnvVarService.getBlacklistTables()).thenReturn("");
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(false);
        when(mockEnvVarService.isActivateLarkDriveSource()).thenReturn(false);
        when(mockEnvVarService.isActivateExperimentalFeatures()).thenReturn(false);

        com.amazonaws.athena.connector.lambda.security.FederatedIdentity identity =
                new com.amazonaws.athena.connector.lambda.security.FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        GetTableRequest request = new GetTableRequest(identity, "queryId", "catalog",
                new com.amazonaws.athena.connector.lambda.domain.TableName("schemaA", "table1"), Collections.emptyMap());

        // The access-control guard must not be what blocks this table (it's allowed); whatever the handler
        // does next (Glue fallback failing since nothing is mocked further) is out of scope for this test.
        try {
            handler.doGetTable(allocator, request);
        }
        catch (com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException e) {
            assertFalse("Access-control guard should not be the cause of failure for an allowed table",
                    e.getMessage() != null && e.getMessage().contains("Table not found"));
        }
        catch (Exception e) {
            // Any other failure downstream (e.g. Glue fallback) is unrelated to the access-control guard.
        }
    }

    @Test
    public void testDoGetTable_GlueHasTable_SkipsLarkSourceAndExperimentalProviders() {
        // Regression test: doGetTable previously tried the Lark Base source and experimental providers
        // BEFORE falling back to Glue, so every query against a crawler-populated table paid for two
        // guaranteed-to-fail (or, for the experimental path, expensive false-positive) metadata
        // resolution attempts - the exact class of problem already fixed for resolvePartitionInfo (see
        // tryResolveFromCrawledSchemaMetadata) but left unaddressed here, where it actually first occurs.
        // Deliberately NOT stubbing isActivateLarkBaseSource/isActivateLarkDriveSource/
        // isActivateExperimentalFeatures: the whole point of this test is that the Glue-first shortcut
        // returns before those flags are ever even checked. Mockito's strict stubbing would flag them
        // as unnecessary if stubbed here, which is itself a nice confirmation the fix works.
        when(mockEnvVarService.getWhitelistTables()).thenReturn("");
        when(mockEnvVarService.getBlacklistTables()).thenReturn("");

        software.amazon.awssdk.services.glue.model.StorageDescriptor storageDescriptor =
                software.amazon.awssdk.services.glue.model.StorageDescriptor.builder()
                        .columns(Collections.emptyList())
                        .build();
        software.amazon.awssdk.services.glue.model.Table glueTable =
                software.amazon.awssdk.services.glue.model.Table.builder()
                        .name("table1")
                        .databaseName("schemaa")
                        .storageDescriptor(storageDescriptor)
                        .parameters(Collections.emptyMap())
                        .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse glueApiResponse =
                software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                        .table(glueTable)
                        .build();
        when(mockGlueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenReturn(glueApiResponse);

        com.amazonaws.athena.connector.lambda.security.FederatedIdentity identity =
                new com.amazonaws.athena.connector.lambda.security.FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        GetTableRequest request = new GetTableRequest(identity, "queryId", "catalog",
                new com.amazonaws.athena.connector.lambda.domain.TableName("schemaa", "table1"), Collections.emptyMap());

        com.amazonaws.athena.connector.lambda.metadata.GetTableResponse response = handler.doGetTable(allocator, request);

        assertNotNull(response);
        assertNotNull(response.getSchema());
        verifyNoInteractions(mockLarkSourceMetadataProvider);
        verifyNoInteractions(mockExperimentalMetadataProvider);
    }

    @Test
    public void testDoGetTable_GlueTableNotFound_FallsThroughToLarkSourceProvider() {
        // The other half of the same fix: a table genuinely not in Glue (the "live Lark source, never
        // crawled" deployment mode) must still fall through to the Lark Base source provider as before -
        // the Glue-first shortcut should be a fast, cheap no-op for this case, not a dead end.
        when(mockEnvVarService.getWhitelistTables()).thenReturn("");
        when(mockEnvVarService.getBlacklistTables()).thenReturn("");
        // isActivateLarkBaseSource() alone short-circuits the "isActivateLarkBaseSource() ||
        // isActivateLarkDriveSource()" check below, so isActivateLarkDriveSource() is deliberately not
        // stubbed here.
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.isActivateExperimentalFeatures()).thenReturn(false);
        when(mockGlueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenThrow(software.amazon.awssdk.services.glue.model.EntityNotFoundException.builder()
                        .message("Table not found").build());
        when(mockLarkSourceMetadataProvider.getTableSchema(any(GetTableRequest.class)))
                .thenReturn(java.util.Optional.empty());

        com.amazonaws.athena.connector.lambda.security.FederatedIdentity identity =
                new com.amazonaws.athena.connector.lambda.security.FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        GetTableRequest request = new GetTableRequest(identity, "queryId", "catalog",
                new com.amazonaws.athena.connector.lambda.domain.TableName("schemaa", "table1"), Collections.emptyMap());

        com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException thrown =
                assertThrows(com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException.class,
                        () -> handler.doGetTable(allocator, request));
        // Same classification as the whitelist/blacklist "table not found" case - a raw, unclassified
        // RuntimeException here would propagate without Athena's ENTITY_NOT_FOUND_EXCEPTION handling.
        assertEquals(software.amazon.awssdk.services.glue.model.FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString(),
                thrown.getErrorDetails().errorCode());

        verify(mockLarkSourceMetadataProvider).getTableSchema(any(GetTableRequest.class));
    }

    @Test
    public void testDoGetDataSourceCapabilities() {
        GetDataSourceCapabilitiesRequest request = mock(GetDataSourceCapabilitiesRequest.class);
        when(request.getCatalogName()).thenReturn("test-catalog");

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        assertNotNull(response);
        assertEquals("test-catalog", response.getCatalogName());
        assertNotNull(response.getCapabilities());
        assertFalse(response.getCapabilities().isEmpty());
        // Verify it has at least 3 capabilities
        assertTrue(response.getCapabilities().size() >= 3);
    }

    @Test
    public void testEnhancePartitionSchema() {
        GetTableLayoutRequest request = mock(GetTableLayoutRequest.class);
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        when(request.getTableName()).thenReturn(tableName);

        com.amazonaws.athena.connector.lambda.data.SchemaBuilder schemaBuilder =
            com.amazonaws.athena.connector.lambda.data.SchemaBuilder.newBuilder();

        handler.enhancePartitionSchema(schemaBuilder, request);

        org.apache.arrow.vector.types.pojo.Schema schema = schemaBuilder.build();
        assertNotNull(schema);
        // Verify required partition fields are added
        assertNotNull(schema.findField("base_id"));
        assertNotNull(schema.findField("table_id"));
        assertNotNull(schema.findField("filter_expression"));
        assertNotNull(schema.findField("page_size"));
        assertNotNull(schema.findField("expected_row_count"));
    }

    @Test
    public void testDoGetSplitsWithNoPartitions() {
        GetSplitsRequest request = mock(GetSplitsRequest.class);
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        when(request.getTableName()).thenReturn(tableName);
        when(request.getCatalogName()).thenReturn("test-catalog");

        // Create empty block
        com.amazonaws.athena.connector.lambda.data.Block partitions = allocator.createBlock(
            com.amazonaws.athena.connector.lambda.data.SchemaBuilder.newBuilder().build());
        when(request.getPartitions()).thenReturn(partitions);

        GetSplitsResponse response = handler.doGetSplits(allocator, request);

        assertNotNull(response);
        assertEquals("test-catalog", response.getCatalogName());
        assertTrue(response.getSplits().isEmpty());
    }

    @Test
    public void testShouldUseParallelSplits_falseWhenTableHasNoParallelSplitKey() {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");

        // Absence of the split key must short-circuit before even checking the activation flag.
        boolean result = handler.shouldUseParallelSplits(false, "base1", "tbl1", "", false, tableName);

        assertFalse(result);
    }

    @Test
    public void testShouldUseParallelSplits_falseWhenFeatureNotActivated() {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        when(mockEnvVarService.isActivateParallelSplit()).thenReturn(false);

        boolean result = handler.shouldUseParallelSplits(true, "base1", "tbl1", "", false, tableName);

        assertFalse(result);
    }

    @Test
    public void testShouldUseParallelSplits_trueWhenNoFilter() throws Exception {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        when(mockEnvVarService.isActivateParallelSplit()).thenReturn(true);

        boolean result = handler.shouldUseParallelSplits(true, "base1", "tbl1", "", false, tableName);

        assertTrue(result);
        // No filter means no selectivity check is needed, so the row-count lookup must never fire.
        verify(mockInvoker, never()).invoke(any());
    }

    @Test
    public void testShouldUseParallelSplits_falseWhenFilterIsHighlySelective() throws Exception {
        // A selective filter (e.g. WHERE id = 'x') matching only a handful of rows must not trigger parallel
        // splitting, because splits are sized off the table's full row count and would mostly return zero rows.
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        when(mockEnvVarService.isActivateParallelSplit()).thenReturn(true);

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(Collections.emptyList())
                        .hasMore(false)
                        .total(1)
                        .build())
                .build();
        when(mockInvoker.invoke(any())).thenReturn(response);

        boolean result = handler.shouldUseParallelSplits(true, "base1", "tbl1", "{\"conditions\":[]}", false, tableName);

        assertFalse(result);
    }

    @Test
    public void testShouldUseParallelSplits_trueWhenFilterMatchesManyRows() throws Exception {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        when(mockEnvVarService.isActivateParallelSplit()).thenReturn(true);

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(Collections.emptyList())
                        .hasMore(false)
                        .total(50_000)
                        .build())
                .build();
        when(mockInvoker.invoke(any())).thenReturn(response);

        boolean result = handler.shouldUseParallelSplits(true, "base1", "tbl1", "{\"conditions\":[]}", false, tableName);

        assertTrue(result);
    }

    @Test
    public void testShouldUseParallelSplits_falseWhenHasOrderBy() throws Exception {
        // writeParallelPartitions splits by positional index and pushes no sort expression to Lark at all -
        // each split's rows come back in arbitrary order. This connector advertises SUPPORTS_TOP_N_PUSHDOWN
        // unconditionally, so Athena's engine trusts that claim and skips its own re-sort; a parallel-split
        // ORDER BY would silently return rows in the wrong order (confirmed live: `ORDER BY field_currency
        // ASC LIMIT 3` returned the 4th-smallest value first and dropped the true minimum entirely). Only
        // writeSinglePartition ever pushes a real sort expression, so ORDER BY must force that path even
        // when the table supports parallel splits and there's no filter (which would otherwise return true).
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        when(mockEnvVarService.isActivateParallelSplit()).thenReturn(true);

        boolean result = handler.shouldUseParallelSplits(true, "base1", "tbl1", "", true, tableName);

        assertFalse(result);
        verify(mockInvoker, never()).invoke(any());
    }

    @Test
    public void testWriteSinglePartition_alwaysLooksUpRowCount_regardlessOfHasOrderByFlag() throws Exception {
        // Athena's engine doesn't populate GetTableLayoutRequest's ORDER BY constraint - hasOrderBy is
        // unreliable here (always false in practice) - so writeSinglePartition can't skip this lookup
        // based on it. Verifies both flag values still take the real, unconditional lookup path.
        java.lang.reflect.Method method = BaseMetadataHandler.class.getDeclaredMethod("writeSinglePartition",
                BlockWriter.class, String.class, String.class, String.class, String.class, String.class,
                String.class, long.class, boolean.class);
        method.setAccessible(true);

        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(Collections.emptyList())
                        .hasMore(false)
                        .total(10)
                        .build())
                .build();
        when(mockInvoker.invoke(any())).thenReturn(response);

        BlockWriter mockBlockWriter = mock(BlockWriter.class);
        method.invoke(handler, mockBlockWriter, "base1", "tbl1", "", "", "{}", "{}", -1L, true);
        method.invoke(handler, mockBlockWriter, "base1", "tbl1", "", "", "{}", "{}", -1L, false);

        verify(mockInvoker, times(2)).invoke(any());
        verify(mockBlockWriter, times(2)).writeRows(any());
    }

    @Test
    public void testResolveOrderBySplitTotalRowCount_singlePartitionWithRawCount_reusesStoredValue() throws Exception {
        // getPartitions already fetched this exact (filtered) count once via writeSinglePartition - reuse
        // it instead of a second, identical Lark API round-trip.
        int result = handler.resolveOrderBySplitTotalRowCount(true, 550, "base1", "tbl1", "");

        assertEquals(550, result);
        verify(mockInvoker, never()).invoke(any());
    }

    @Test
    public void testResolveOrderBySplitTotalRowCount_singlePartitionWithZeroRawCount_reusesGenuineZero() throws Exception {
        // 0 is a legitimate fetched count (a filter matching no rows), not the "unavailable" sentinel -
        // must still be reused, not treated as missing.
        int result = handler.resolveOrderBySplitTotalRowCount(true, 0, "base1", "tbl1", "");

        assertEquals(0, result);
        verify(mockInvoker, never()).invoke(any());
    }

    @Test
    public void testResolveOrderBySplitTotalRowCount_parallelPlanned_fallsBackToFreshFetch() throws Exception {
        // writeParallelPartitions stores an UNFILTERED count under the same property (wrong semantics to
        // reuse whenever a filter is present), so a parallel-planned row 0 must never be trusted here -
        // regardless of what value it carries (-1 sentinel in production, but the flag alone must gate this).
        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(Collections.emptyList())
                        .hasMore(false)
                        .total(999)
                        .build())
                .build();
        when(mockInvoker.invoke(any())).thenReturn(response);

        int result = handler.resolveOrderBySplitTotalRowCount(false, -1, "base1", "tbl1", "");

        assertEquals(999, result);
        verify(mockInvoker, times(1)).invoke(any());
    }

    @Test
    public void testResolveOrderBySplitTotalRowCount_rawCountUnavailable_fallsBackToFreshFetch() throws Exception {
        // null means the property wasn't present on the partition schema at all (e.g. a version-mismatch
        // edge case) - must not be confused with a genuinely-fetched zero.
        SearchRecordsResponse response = (SearchRecordsResponse) SearchRecordsResponse.builder()
                .data(SearchRecordsResponse.ListData.builder()
                        .items(Collections.emptyList())
                        .hasMore(false)
                        .total(42)
                        .build())
                .build();
        when(mockInvoker.invoke(any())).thenReturn(response);

        int result = handler.resolveOrderBySplitTotalRowCount(true, null, "base1", "tbl1", "");

        assertEquals(42, result);
        verify(mockInvoker, times(1)).invoke(any());
    }

    @Test
    public void testExceedsParallelSplitMappingBudget_smallMappingManySplits_staysUnderBudget() {
        // A realistic mapping JSON for a modest table is a few dozen bytes, so even thousands of splits
        // must not trip the safety budget.
        boolean result = handler.exceedsParallelSplitMappingBudget(10_000, "{\"col1\":\"TEXT\"}", "{\"Col 1\":\"col1\"}");

        assertFalse(result);
    }

    @Test
    public void testExceedsParallelSplitMappingBudget_largeMappingManySplits_exceedsBudget() {
        // A wide table (many columns, e.g. long Chinese field names) producing a several-KB mapping JSON,
        // duplicated across a few thousand splits, must trip the budget rather than risk Lambda's ~6MB
        // synchronous response payload limit.
        String largeTypeMapping = "a".repeat(3000);
        String largeNameMapping = "b".repeat(3000);

        boolean result = handler.exceedsParallelSplitMappingBudget(1000, largeTypeMapping, largeNameMapping);

        assertTrue(result);
    }

    @Test
    public void testExceedsParallelSplitMappingBudget_exactlyAtBudget_doesNotExceed() {
        String typeMapping = "a".repeat(2000);
        String nameMapping = "b".repeat(2000);

        // bytesPerSplit (4000) * numSplits (1000) == MAX_PARALLEL_SPLIT_MAPPING_BYTES (4_000_000) exactly.
        boolean result = handler.exceedsParallelSplitMappingBudget(1000, typeMapping, nameMapping);

        assertFalse(result);
    }

    @Test
    public void testExceedsParallelSplitMappingBudget_oneSplitOverBudget_exceeds() {
        String typeMapping = "a".repeat(2000);
        String nameMapping = "b".repeat(2000);

        // One split beyond the exact-budget case pushes the projected total just past the limit.
        boolean result = handler.exceedsParallelSplitMappingBudget(1001, typeMapping, nameMapping);

        assertTrue(result);
    }

    @Test
    public void testExceedsParallelSplitMappingBudget_nullMappingJson_treatedAsZeroBytesNotNpe() {
        boolean result = handler.exceedsParallelSplitMappingBudget(Integer.MAX_VALUE, null, null);

        assertFalse(result);
    }

    @Test
    public void testComputeParallelSplitEndIndex_lastSplit_isOpenEnded() {
        // $reserved_split_key is a user-populated auto-number field whose values can have gaps or exceed
        // the row-count estimate once any row has ever been deleted. The last split must stay open-ended
        // (Long.MAX_VALUE) so it still covers every row above its start index regardless of gaps, rather
        // than silently excluding rows with a higher key value than the stale row-count-based estimate.
        long endIndex = handler.computeParallelSplitEndIndex(4, 5, 2500);

        assertEquals(Long.MAX_VALUE, endIndex);
    }

    @Test
    public void testComputeParallelSplitEndIndex_lastSplit_singleSplitTotal_isOpenEnded() {
        // A single-split "parallel" plan (numSplits == 1) is still the last split - it must cover the
        // whole table's key range, not just [1, PAGE_SIZE].
        long endIndex = handler.computeParallelSplitEndIndex(0, 1, 50);

        assertEquals(Long.MAX_VALUE, endIndex);
    }

    @Test
    public void testComputeParallelSplitEndIndex_nonLastSplit_boundedByPageSize() {
        // Every split except the last is still sized normally off PAGE_SIZE, preserving parallelism.
        long endIndex = handler.computeParallelSplitEndIndex(0, 5, 2500);

        assertEquals(PAGE_SIZE, endIndex);
    }

    @Test
    public void testComputeParallelSplitEndIndex_nonLastSplit_boundedByEffectiveRowCount() {
        // A non-last split's bound is still clamped to effectiveRowCount when that's smaller than a full
        // page (e.g. a LIMIT reduced the effective row count below what raw split-index math would give).
        long endIndex = handler.computeParallelSplitEndIndex(0, 2, 300);

        assertEquals(300, endIndex);
    }

    @Test
    public void testCalculateOrderBySplitSizing_limitZero_requestsOneRowNotWholeTable() {
        // Regression test: a bare `limit > 0` check used to treat LIMIT 0 (SELECT ... ORDER BY x LIMIT 0
        // - a valid, if unusual, query) the same as "no LIMIT at all", fetching and sorting the entire
        // table via Lark's Search API for zero requested rows. Requesting 1 row (not 0 - BaseRecordHandler's
        // own row-count cap checks treat 0 as "unbounded" too) caps the real fetch to a single small page.
        software.amazon.awssdk.utils.Pair<Integer, Integer> sizing = handler.calculateOrderBySplitSizing(0, 550);

        assertEquals(1, sizing.left().intValue());
        assertEquals(1, sizing.right().intValue());
    }

    @Test
    public void testCalculateOrderBySplitSizing_noLimit_usesPageSizeAndTotalRowCount() {
        software.amazon.awssdk.utils.Pair<Integer, Integer> sizing = handler.calculateOrderBySplitSizing(-1, 550);

        assertEquals(PAGE_SIZE, sizing.left().intValue());
        assertEquals(550, sizing.right().intValue());
    }

    @Test
    public void testCalculateOrderBySplitSizing_limitSmallerThanTotal_usesLimit() {
        software.amazon.awssdk.utils.Pair<Integer, Integer> sizing = handler.calculateOrderBySplitSizing(10, 550);

        assertEquals(10, sizing.left().intValue());
        assertEquals(10, sizing.right().intValue());
    }

    @Test
    public void testCalculateOrderBySplitSizing_limitLargerThanTotal_usesTotalRowCount() {
        software.amazon.awssdk.utils.Pair<Integer, Integer> sizing = handler.calculateOrderBySplitSizing(1000, 550);

        assertEquals(PAGE_SIZE, sizing.left().intValue());
        assertEquals(550, sizing.right().intValue());
    }

    @Test
    public void testBuildSortExpressionForSplits_invertsLarkFieldNameMapping() throws Exception {
        // The partition only carries larkFieldNameMappingJson as Map<larkFieldName, athenaColumnName> (see
        // its use in BaseRecordHandler) - this must invert it before handing it to
        // SearchApiFilterTranslator.toSortJson, which looks fields up by Athena column name.
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");

        String larkFieldNameMappingJson = "{\"Currency Field\":\"field_currency\"}";
        List<com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField> orderByClause =
            Collections.singletonList(new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_currency", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.ASC_NULLS_LAST));

        String sortExpression = handler.buildSortExpressionForSplits(orderByClause, larkFieldNameMappingJson, tableName);

        assertNotNull(sortExpression);
        assertTrue(sortExpression.contains("\"field_name\":\"Currency Field\""));
    }

    @Test
    public void testBuildSortExpressionForSplits_emptyMappingJson_returnsEmptyString() {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");

        String sortExpression = handler.buildSortExpressionForSplits(Collections.emptyList(), "", tableName);

        assertEquals("", sortExpression);
    }

    @Test
    public void testFindNullsFirstOriginalFieldName_ascNullsFirst_returnsLarkFieldName() {
        // ORDER BY x ASC NULLS FIRST conflicts with Lark's fixed "nulls last" sort behavior (confirmed
        // live: the connector still returned non-null rows first), so this must be detected and resolved
        // back to the original Lark field name for the record handler's two-phase fetch.
        String larkFieldNameMappingJson = "{\"Currency Field\":\"field_currency\"}";
        List<com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField> orderByClause =
            Collections.singletonList(new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_currency", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.ASC_NULLS_FIRST));

        String result = handler.findNullsFirstOriginalFieldName(orderByClause, larkFieldNameMappingJson);

        assertEquals("Currency Field", result);
    }

    @Test
    public void testFindNullsFirstOriginalFieldName_descNullsFirst_returnsLarkFieldName() {
        String larkFieldNameMappingJson = "{\"Currency Field\":\"field_currency\"}";
        List<com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField> orderByClause =
            Collections.singletonList(new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_currency", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.DESC_NULLS_FIRST));

        String result = handler.findNullsFirstOriginalFieldName(orderByClause, larkFieldNameMappingJson);

        assertEquals("Currency Field", result);
    }

    @Test
    public void testFindNullsFirstOriginalFieldName_nullsLast_returnsNull() {
        // NULLS_LAST already matches Lark's fixed behavior (confirmed live via CloudWatch logs: an
        // implicit `ORDER BY x DESC` resolves to DESC_NULLS_LAST, not DESC_NULLS_FIRST as SQL's
        // PostgreSQL-style convention would suggest), so no two-phase fetch is needed.
        String larkFieldNameMappingJson = "{\"Currency Field\":\"field_currency\"}";
        List<com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField> orderByClause =
            Collections.singletonList(new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_currency", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.DESC_NULLS_LAST));

        assertNull(handler.findNullsFirstOriginalFieldName(orderByClause, larkFieldNameMappingJson));
    }

    @Test
    public void testFindNullsFirstOriginalFieldName_onlyChecksPrimarySortColumn() {
        // Lark's Search API sorts on a flat priority list; a NULLS FIRST conflict on a secondary sort
        // column (already tied on the primary key) is a narrower case this connector doesn't attempt to
        // correct for, so only orderByClause.get(0) is ever checked.
        String larkFieldNameMappingJson = "{\"Currency Field\":\"field_currency\",\"Rating Field\":\"field_rating\"}";
        List<com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField> orderByClause = List.of(
            new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_currency", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.ASC_NULLS_LAST),
            new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_rating", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.ASC_NULLS_FIRST));

        assertNull(handler.findNullsFirstOriginalFieldName(orderByClause, larkFieldNameMappingJson));
    }

    @Test
    public void testFindNullsFirstOriginalFieldName_columnNotInMapping_returnsNull() {
        String larkFieldNameMappingJson = "{\"Currency Field\":\"field_currency\"}";
        List<com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField> orderByClause =
            Collections.singletonList(new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_unmapped", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.ASC_NULLS_FIRST));

        assertNull(handler.findNullsFirstOriginalFieldName(orderByClause, larkFieldNameMappingJson));
    }

    @Test
    public void testFindNullsFirstOriginalFieldName_emptyOrderByClause_returnsNull() {
        assertNull(handler.findNullsFirstOriginalFieldName(Collections.emptyList(), "{\"Currency Field\":\"field_currency\"}"));
    }

    @Test
    public void testFindNullsFirstOriginalFieldName_emptyMappingJson_returnsNull() {
        List<com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField> orderByClause =
            Collections.singletonList(new com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField(
                "field_currency", com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction.ASC_NULLS_FIRST));

        assertNull(handler.findNullsFirstOriginalFieldName(orderByClause, ""));
        assertNull(handler.findNullsFirstOriginalFieldName(orderByClause, null));
    }

    // ========== Tests for tryResolveFromCrawledSchemaMetadata (skip wasted Lark source/experimental
    // resolution attempts for a table the crawler already populated - see resolvePartitionInfo) ==========

    @Test
    public void testTryResolveFromCrawledSchemaMetadata_metadataPresent_returnsResultWithoutIdLookup() throws Exception {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        GetTableLayoutRequest request = mock(GetTableLayoutRequest.class);

        Map<String, String> schemaMetadata = new HashMap<>();
        schemaMetadata.put("larkBaseId", "base123");
        schemaMetadata.put("larkTableId", "tbl456");
        org.apache.arrow.vector.types.pojo.Schema schema = new org.apache.arrow.vector.types.pojo.Schema(
            Collections.emptyList(), schemaMetadata);
        when(request.getSchema()).thenReturn(schema);

        List<com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping> mappings =
            Collections.singletonList(new com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping(
                "field_a", "Field A", new com.amazonaws.athena.connectors.lark.base.model.NestedUIType(
                    com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum.TEXT, null)));
        when(mockGlueCatalogService.getFieldNameMappings("test_schema", "test_table")).thenReturn(mappings);

        java.util.Optional<com.amazonaws.athena.connectors.lark.base.model.PartitionInfoResult> result =
            handler.tryResolveFromCrawledSchemaMetadata(tableName, request);

        assertTrue(result.isPresent());
        assertEquals("base123", result.get().baseId());
        assertEquals("tbl456", result.get().tableId());
        assertEquals(mappings, result.get().fieldNameMappings());
        // The whole point: the IDs came from the schema itself, never from a fresh Glue ID lookup.
        verify(mockGlueCatalogService, never()).getLarkBaseAndTableIdFromTable(any(), any());
    }

    @Test
    public void testTryResolveFromCrawledSchemaMetadata_nullSchema_returnsEmpty() {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        GetTableLayoutRequest request = mock(GetTableLayoutRequest.class);
        when(request.getSchema()).thenReturn(null);

        assertFalse(handler.tryResolveFromCrawledSchemaMetadata(tableName, request).isPresent());
    }

    @Test
    public void testTryResolveFromCrawledSchemaMetadata_missingIds_returnsEmpty() {
        // A table resolved through the "direct"/Lark-source path (not crawler-populated) has no
        // larkBaseId/larkTableId on its schema - this must fall through to the normal provider chain,
        // not silently produce a bogus empty-string ID pair.
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        GetTableLayoutRequest request = mock(GetTableLayoutRequest.class);
        org.apache.arrow.vector.types.pojo.Schema schema = new org.apache.arrow.vector.types.pojo.Schema(
            Collections.emptyList(), Collections.emptyMap());
        when(request.getSchema()).thenReturn(schema);

        assertFalse(handler.tryResolveFromCrawledSchemaMetadata(tableName, request).isPresent());
    }

    @Test
    public void testTryResolveFromCrawledSchemaMetadata_glueFieldMappingLookupFails_returnsEmpty() throws Exception {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName =
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");
        GetTableLayoutRequest request = mock(GetTableLayoutRequest.class);

        Map<String, String> schemaMetadata = new HashMap<>();
        schemaMetadata.put("larkBaseId", "base123");
        schemaMetadata.put("larkTableId", "tbl456");
        org.apache.arrow.vector.types.pojo.Schema schema = new org.apache.arrow.vector.types.pojo.Schema(
            Collections.emptyList(), schemaMetadata);
        when(request.getSchema()).thenReturn(schema);
        when(mockGlueCatalogService.getFieldNameMappings("test_schema", "test_table"))
            .thenThrow(new RuntimeException("Glue unavailable"));

        // Falls back to the normal provider chain rather than throwing - a transient Glue error here
        // shouldn't take down the whole request when the existing chain might still succeed.
        assertFalse(handler.tryResolveFromCrawledSchemaMetadata(tableName, request).isPresent());
    }
}
