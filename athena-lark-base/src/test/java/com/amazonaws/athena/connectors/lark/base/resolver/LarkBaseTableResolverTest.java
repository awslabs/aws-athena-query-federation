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
package com.amazonaws.athena.connectors.lark.base.resolver;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connectors.lark.base.model.LarkDatabaseRecord;
import com.amazonaws.athena.connectors.lark.base.model.TableDirectInitialized;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.model.response.ListAllTableResponse;
import com.amazonaws.athena.connectors.lark.base.model.response.ListFieldResponse;
import com.amazonaws.athena.connectors.lark.base.service.EnvVarService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import com.amazonaws.athena.connectors.lark.base.service.LarkDriveService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LarkBaseTableResolverTest {

    @Mock
    private EnvVarService mockEnvVarService;

    @Mock
    private LarkBaseService mockLarkBaseService;

    @Mock
    private LarkDriveService mockLarkDriveService;

    @Mock
    private ThrottlingInvoker mockInvoker;

    private LarkBaseTableResolver resolver;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        resolver = new LarkBaseTableResolver(mockEnvVarService, mockLarkBaseService, mockLarkDriveService, mockInvoker);

        // Mock the invoker to simply execute the callable
        doAnswer(invocation -> {
            Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        }).when(mockInvoker).invoke(any(Callable.class));
    }

    @Test
    public void testResolveTables_BothSourcesDisabled() {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(false);
        when(mockEnvVarService.isActivateLarkDriveSource()).thenReturn(false);
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertNotNull(tables);
        assertEquals(0, tables.size());
    }

    @Test
    public void testResolveTables_LarkBaseEnabled() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.singletonList(ListFieldResponse.FieldItem.builder().fieldName("field1").fieldId("fieldId1").uiType("TEXT").build()));

        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
    }

    @Test
    public void testResolveTables_LarkDriveEnabled() throws Exception {
        when(mockEnvVarService.isActivateLarkDriveSource()).thenReturn(true);
        when(mockEnvVarService.getLarkDriveSources()).thenReturn("drive1");
        when(mockLarkDriveService.getLarkBases(anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.singletonList(ListFieldResponse.FieldItem.builder().fieldName("field1").fieldId("fieldId1").uiType("TEXT").build()));

        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
    }

    @Test
    public void testResolveTables_CollidingSanitizedFieldNames_disambiguatesWithFieldId() throws Exception {
        // Reproduces a real production case: two distinct Lark fields named "segment 5" and "Segment 5"
        // both sanitize to the same Athena column name "segment_5". Without disambiguation, one field
        // would silently vanish from the discovered schema (SchemaBuilder.addField overwrites by name).
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName("segment 5").fieldId("fldxdVXbHa").uiType("TEXT").build(),
                ListFieldResponse.FieldItem.builder().fieldName("Segment 5").fieldId("fldZrayo2s").uiType("TEXT").build()));

        List<TableDirectInitialized> tables = resolver.resolveTables();

        assertEquals(1, tables.size());
        List<String> athenaNames = tables.get(0).columns().stream()
                .map(com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping::athenaName)
                .toList();
        assertEquals(2, athenaNames.size());
        assertEquals(2, new java.util.HashSet<>(athenaNames).size());
        assertEquals("segment_5", athenaNames.get(0));
        assertEquals("segment_5_fldzrayo2s", athenaNames.get(1));
    }

    @Test
    public void testResolveFromLarkBaseSource_Exception() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenThrow(new RuntimeException("Lark Error"));

        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testResolveFromLarkDriveSource_Exception() throws Exception {
        when(mockEnvVarService.isActivateLarkDriveSource()).thenReturn(true);
        when(mockEnvVarService.getLarkDriveSources()).thenReturn("drive1");
        when(mockLarkDriveService.getLarkBases(anyString())).thenThrow(new RuntimeException("Lark Error"));

        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testDiscoverTableFields_LookupType() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));

        Map<String, Object> property = new HashMap<>();
        Map<String, Object> filterInfo = new HashMap<>();
        filterInfo.put("target_table", "relatedTableId");
        property.put("target_field", "relatedFieldId");
        property.put("filter_info", filterInfo);

        ListFieldResponse.FieldItem lookupField = ListFieldResponse.FieldItem.builder()
                .fieldName("lookupField")
                .fieldId("fieldId1")
                .uiType("LOOKUP")
                .property(property)
                .build();

        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.singletonList(lookupField));
        // The LOOKUP is resolved using the Lark base ID ("db1", from LarkDatabaseRecord.id()), not the
        // Athena/Presto database name ("base1", from LarkDatabaseRecord.name()) - discoverTableFields is called
        // with larkBaseId, which comes from record.id() in processTargetDatabaseRecords.
        when(mockLarkBaseService.getLookupType("db1", "relatedTableId", "relatedFieldId")).thenReturn(UITypeEnum.TEXT);

        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
        assertEquals(1, tables.get(0).columns().size());
        assertEquals("lookupField", tables.get(0).columns().get(0).larkBaseFieldName());
        // The chained LOOKUP must actually resolve to its target field's type (TEXT), not be left null/UNKNOWN.
        assertEquals(UITypeEnum.TEXT, tables.get(0).columns().get(0).nestedUIType().childType());

        // getLookupType() itself calls getTableFields() internally on a cache miss, hitting the Lark API
        // directly; it must go through the same throttling invoker as every other Lark API call in this
        // resolver so a 429 gets rate-limit backoff instead of failing raw. The lookup call must be routed
        // through invoker.invoke(...) (4 calls total: getDatabaseRecords, listTables, getTableFields for
        // table1, and the LOOKUP resolution), not called directly and unwrapped.
        verify(mockLarkBaseService).getLookupType("db1", "relatedTableId", "relatedFieldId");
        verify(mockInvoker, times(4)).invoke(any(Callable.class));
    }

    @Test
    public void testDiscoverTableFields_LookupResolutionFailure_skipsOnlyThatFieldNotWholeTable() throws Exception {
        // Regression test: getTargetFieldAndTableForLookup() returns Pair.of(null, null) - not a thrown
        // exception - for a LOOKUP field whose target is malformed or permission-restricted (a realistic
        // case, not contrived). That null tableId/fieldId then makes the getLookupType(...) call below
        // fail against Lark's API. Before this fix, that exception wasn't caught per-field - it escaped
        // the whole for-loop into discoverTableFields's outer catch, which returns whatever fieldMappings
        // had been collected BEFORE the failure, silently dropping every field that came after the broken
        // one in Lark's response, not just that one column.
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));

        ListFieldResponse.FieldItem brokenLookupField = ListFieldResponse.FieldItem.builder()
                .fieldName("brokenLookup")
                .fieldId("fieldId1")
                .uiType("LOOKUP")
                .property(Collections.emptyMap()) // no target_field/filter_info -> getTargetFieldAndTableForLookup() returns Pair.of(null, null)
                .build();
        ListFieldResponse.FieldItem laterTextField = ListFieldResponse.FieldItem.builder()
                .fieldName("laterField")
                .fieldId("fieldId2")
                .uiType("Text")
                .build();

        when(mockLarkBaseService.getTableFields(anyString(), anyString()))
                .thenReturn(java.util.Arrays.asList(brokenLookupField, laterTextField));
        when(mockLarkBaseService.getLookupType(eq("db1"), isNull(), isNull()))
                .thenThrow(new RuntimeException("Failed to retrieve fields for table: null, Error: NOTEXIST"));

        List<TableDirectInitialized> tables = resolver.resolveTables();

        assertEquals(1, tables.size());
        // The field AFTER the broken LOOKUP in Lark's response must still make it into the schema.
        assertEquals(1, tables.get(0).columns().size());
        assertEquals("laterField", tables.get(0).columns().get(0).larkBaseFieldName());
    }

    @Test
    public void testResolveFromLarkBaseSource_TimeoutException() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockInvoker.invoke(any(Callable.class))).thenThrow(new TimeoutException());
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testResolveFromLarkDriveSource_TimeoutException() throws Exception {
        when(mockEnvVarService.isActivateLarkDriveSource()).thenReturn(true);
        when(mockEnvVarService.getLarkDriveSources()).thenReturn("drive1");
        when(mockInvoker.invoke(any(Callable.class))).thenThrow(new TimeoutException());
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testProcessTargetDatabaseRecords_InvalidDbIdentifier() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("", "base1")));
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testProcessTargetDatabaseRecords_InvalidTableIdentifier() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("").tableId("tableId1").build()));
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testProcessTargetDatabaseRecords_ListTablesTimeout() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockInvoker.invoke(any(Callable.class))).thenThrow(new TimeoutException());
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testProcessTargetDatabaseRecords_ListTablesException() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenThrow(new RuntimeException());
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(0, tables.size());
    }

    @Test
    public void testResolveTables_DuplicateSanitizedDatabaseNames_disambiguatesWithId() throws Exception {
        // Reproduces a real risk: two distinct Lark Bases (e.g. Drive files "Sales Data" and
        // "sales_data") both sanitize to the same Athena schema name. Without disambiguation, the
        // second base would be permanently shadowed by LarkSourceMetadataProvider.findMapping()'s
        // findFirst() - it would never be reachable via Athena, with no error at all.
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(List.of(
                new LarkDatabaseRecord("dbId1", "sales_data"),
                new LarkDatabaseRecord("dbId2", "sales_data")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(
                ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.emptyList());

        List<TableDirectInitialized> tables = resolver.resolveTables();

        assertEquals(2, tables.size());
        List<String> dbNames = tables.stream().map(t -> t.database().athenaName()).toList();
        assertEquals(2, new java.util.HashSet<>(dbNames).size());
        assertEquals("sales_data", dbNames.get(0));
        assertEquals("sales_data_dbid2", dbNames.get(1));
    }

    @Test
    public void testResolveTables_DuplicateSanitizedTableNamesInSameBase_disambiguatesWithId() throws Exception {
        // Same collision risk as database names, but scoped to two tables within one base.
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(List.of(
                ListAllTableResponse.BaseItem.builder().name("report a").tableId("tableId1").build(),
                ListAllTableResponse.BaseItem.builder().name("Report A").tableId("tableId2").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.emptyList());

        List<TableDirectInitialized> tables = resolver.resolveTables();

        assertEquals(2, tables.size());
        List<String> tableNames = tables.stream().map(t -> t.table().athenaName()).toList();
        assertEquals(2, new java.util.HashSet<>(tableNames).size());
        assertEquals("report_a", tableNames.get(0));
        assertEquals("report_a_tableid2", tableNames.get(1));
    }

    @Test
    public void testDiscoverTableFields_InvalidFieldName() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.singletonList(ListFieldResponse.FieldItem.builder().fieldName("").fieldId("fieldId1").uiType("TEXT").build()));
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
        assertEquals(0, tables.get(0).columns().size());
    }

    @Test
    public void testDiscoverTableFields_Timeout() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockInvoker.invoke(any(Callable.class))).thenAnswer(invocation -> {
            Callable<?> callable = invocation.getArgument(0);
            if (callable.toString().contains("getTableFields")) {
                throw new TimeoutException();
            }
            return callable.call();
        });
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
        assertEquals(0, tables.get(0).columns().size());
    }

    @Test
    public void testDiscoverTableFields_Exception() throws Exception {
        when(mockEnvVarService.isActivateLarkBaseSource()).thenReturn(true);
        when(mockEnvVarService.getLarkBaseSources()).thenReturn("base1:table1");
        when(mockLarkBaseService.getDatabaseRecords(anyString(), anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenThrow(new RuntimeException());
        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
        assertEquals(0, tables.get(0).columns().size());
    }
}