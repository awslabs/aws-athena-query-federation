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
package com.amazonaws.athena.connectors.lark.base.metadataProvider;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.lark.base.model.PartitionInfoResult;
import com.amazonaws.athena.connectors.lark.base.model.TableSchemaResult;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.model.response.ListFieldResponse;
import com.amazonaws.athena.connectors.lark.base.service.AthenaService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import com.amazonaws.athena.connectors.lark.base.throttling.BaseExceptionFilter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class ExperimentalMetadataProviderTest {

    @Mock
    private AthenaService athenaService;

    @Mock
    private LarkBaseService larkBaseService;

    private ThrottlingInvoker invoker;

    private ExperimentalMetadataProvider metadataProvider;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        invoker = ThrottlingInvoker.newDefaultBuilder(new BaseExceptionFilter(), Collections.emptyMap()).build();
        metadataProvider = new ExperimentalMetadataProvider(athenaService, larkBaseService, invoker);
    }

    @Test
    public void getTableSchema_success() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder()
                        .fieldName("field1")
                        .uiType(UITypeEnum.TEXT.name())
                        .property(Collections.emptyMap())
                        .build()
        ));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertTrue(result.isPresent());
        assertNotNull(result.get().schema());
    }

    @Test
    public void getTableSchema_lookupField() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        ListFieldResponse.FieldItem lookupField = ListFieldResponse.FieldItem.builder()
                .fieldName("lookup_field")
                .uiType(UITypeEnum.LOOKUP.name())
                .property(Map.of("target_field", "fldxxxx", "filter_info", Map.of("target_table", "tblxxxx")))
                .build();
        when(larkBaseService.getTableFields("base1", "table1")).thenReturn(List.of(lookupField));
        when(larkBaseService.getLookupType("base1", "tblxxxx", "fldxxxx")).thenReturn(UITypeEnum.TEXT);

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertTrue(result.isPresent());
        assertNotNull(result.get().schema());
    }

    @Test
    public void getTableSchema_noIds() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_apiError() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenThrow(new RuntimeException("API Error"));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_noFields() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.emptyList());

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_emptyFieldName() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName(" ").build()
        ));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_success() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName("field1").uiType(UITypeEnum.TEXT.name()).build()
        ));

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertTrue(result.isPresent());
        assertEquals("base1", result.get().baseId());
        assertEquals("table1", result.get().tableId());
    }

    @Test
    public void getPartitionInfo_noIds() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_emptyFields() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.emptyList());

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_missingQueryId() {
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), null, "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_athenaError() {
        when(athenaService.getAthenaQueryString(anyString())).thenThrow(InvalidRequestException.builder().build());
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_timeoutError() {
        when(athenaService.getAthenaQueryString(anyString())).thenThrow(new RuntimeException(new TimeoutException()));
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_genericError() {
        when(athenaService.getAthenaQueryString(anyString())).thenThrow(new RuntimeException());
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_apiError() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenThrow(new RuntimeException("API Error"));
        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());
        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);
        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_lookupField() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        ListFieldResponse.FieldItem lookupField = ListFieldResponse.FieldItem.builder()
                .fieldName("lookup_field")
                .uiType(UITypeEnum.LOOKUP.name())
                .property(Map.of("target_field", "fldxxxx", "filter_info", Map.of("target_table", "tblxxxx")))
                .build();
        when(larkBaseService.getTableFields("base1", "table1")).thenReturn(List.of(lookupField));
        when(larkBaseService.getLookupType("base1", "tblxxxx", "fldxxxx")).thenReturn(UITypeEnum.TEXT);

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().fieldNameMappings().size());
    }

    @Test
    public void testConstructorWithNullAthenaService() {
        assertThrows(NullPointerException.class, () -> new ExperimentalMetadataProvider(null, larkBaseService, invoker));
    }

    @Test
    public void testConstructorWithNullLarkBaseService() {
        assertThrows(NullPointerException.class, () -> new ExperimentalMetadataProvider(athenaService, null, invoker));
    }

    @Test
    public void testConstructorWithNullInvoker() {
        assertThrows(NullPointerException.class, () -> new ExperimentalMetadataProvider(athenaService, larkBaseService, null));
    }

    @Test
    public void getTableSchema_extractOriginalIdsFromQuery_returnsEmpty() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_extractOriginalIdsFromQuery_returnsEmpty() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_emptyFieldName() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName(" ").build()
        ));
        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());
        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_nullQuery() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn(null);
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_lookupTypeThrowsException() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        ListFieldResponse.FieldItem lookupField = ListFieldResponse.FieldItem.builder()
                .fieldName("lookup_field")
                .uiType(UITypeEnum.LOOKUP.name())
                .property(Map.of("target_field", "fldxxxx", "filter_info", Map.of("target_table", "tblxxxx")))
                .build();
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(lookupField));
        doThrow(new RuntimeException("API Error")).when(larkBaseService).getLookupType("base1", "tblxxxx", "fldxxxx");

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().schema().getFields().size());
    }

    @Test
    public void getTableSchema_nullFieldName() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName(null).build()
        ));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_lookupTypeThrowsException() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        ListFieldResponse.FieldItem lookupField = ListFieldResponse.FieldItem.builder()
                .fieldName("lookup_field")
                .uiType(UITypeEnum.LOOKUP.name())
                .property(Map.of("target_field", "fldxxxx", "filter_info", Map.of("target_table", "tblxxxx")))
                .build();
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(lookupField));
        doThrow(new RuntimeException("API Error")).when(larkBaseService).getLookupType("base1", "tblxxxx", "fldxxxx");

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().fieldNameMappings().size());
    }

    @Test
    public void discoverTableFields_nullFieldName() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName(null).build()
        ));
        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());
        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_emptyQueryId() {
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_lookupTypeThrowsException_simplified() {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        ListFieldResponse.FieldItem lookupField = ListFieldResponse.FieldItem.builder()
                .fieldName("lookup_field")
                .uiType(UITypeEnum.LOOKUP.name())
                .property(Map.of("target_field", "fldxxxx", "filter_info", Map.of("target_table", "tblxxxx")))
                .build();
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(lookupField));
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().schema().getFields().size());
    }
}