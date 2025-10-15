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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
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
    public void testResolveTables_LarkDriveEnabled() {
        when(mockEnvVarService.isActivateLarkDriveSource()).thenReturn(true);
        when(mockEnvVarService.getLarkDriveSources()).thenReturn("drive1");
        when(mockLarkDriveService.getLarkBases(anyString())).thenReturn(Collections.singletonList(new LarkDatabaseRecord("db1", "base1")));
        when(mockLarkBaseService.listTables(anyString())).thenReturn(Collections.singletonList(ListAllTableResponse.BaseItem.builder().name("table1").tableId("tableId1").build()));
        when(mockLarkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.singletonList(ListFieldResponse.FieldItem.builder().fieldName("field1").fieldId("fieldId1").uiType("TEXT").build()));

        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
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
    public void testResolveFromLarkDriveSource_Exception() {
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
        when(mockLarkBaseService.getLookupType("base1", "relatedTableId", "relatedFieldId")).thenReturn(UITypeEnum.TEXT);

        List<TableDirectInitialized> tables = resolver.resolveTables();
        assertEquals(1, tables.size());
        assertEquals(1, tables.get(0).columns().size());
        assertEquals("lookupField", tables.get(0).columns().get(0).larkBaseFieldName());
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