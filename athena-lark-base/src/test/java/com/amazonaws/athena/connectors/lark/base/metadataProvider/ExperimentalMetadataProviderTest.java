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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
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
    public void getTableSchema_success() throws Exception {
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
    public void getTableSchema_collidingSanitizedFieldNames_disambiguatesWithFieldId() throws Exception {
        // Reproduces a real production case: two distinct Lark fields named "segment 5" and "Segment 5"
        // both sanitize to the same Athena column name "segment_5". Without disambiguation, one field
        // would silently vanish from the schema (SchemaBuilder.addField overwrites by name).
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName("segment 5").fieldId("fldxdVXbHa").uiType(UITypeEnum.TEXT.name()).build(),
                ListFieldResponse.FieldItem.builder().fieldName("Segment 5").fieldId("fldZrayo2s").uiType(UITypeEnum.TEXT.name()).build()
        ));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertTrue(result.isPresent());
        Schema schema = result.get().schema();
        List<String> fieldNames = schema.getFields().stream().map(org.apache.arrow.vector.types.pojo.Field::getName).toList();
        assertTrue(fieldNames.contains("segment_5"));
        assertTrue(fieldNames.contains("segment_5_fldzrayo2s"));
    }

    @Test
    public void getTableSchema_complexTypeAsJsonString_collapsesListColumnToVarchar() throws Exception {
        ExperimentalMetadataProvider flagOnProvider = new ExperimentalMetadataProvider(athenaService, larkBaseService, invoker, true);
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder()
                        .fieldName("tags")
                        .uiType(UITypeEnum.MULTI_SELECT.name())
                        .property(Collections.emptyMap())
                        .build()
        ));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = flagOnProvider.getTableSchema(request);

        assertTrue(result.isPresent());
        assertEquals(org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE,
                result.get().schema().findField("tags").getType());
    }

    @Test
    public void getTableSchema_lookupField() throws Exception {
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
    public void getTableSchema_noIds() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_apiError() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenThrow(new RuntimeException("API Error"));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_noFields() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(Collections.emptyList());

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_emptyFieldName() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName(" ").build()
        ));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_success() throws Exception {
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
    public void getPartitionInfo_noIds() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_emptyFields() throws Exception {
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
    public void extractOriginalIds_athenaError() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenThrow(InvalidRequestException.builder().build());
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_timeoutError() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenThrow(new RuntimeException(new TimeoutException()));
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_genericError() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenThrow(new RuntimeException());
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_apiError() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenThrow(new RuntimeException("API Error"));
        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());
        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);
        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_lookupField() throws Exception {
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
        // The chained LOOKUP must actually resolve to its target field's type (TEXT), not be left null/UNKNOWN.
        assertEquals(UITypeEnum.TEXT, result.get().fieldNameMappings().get(0).nestedUIType().childType());
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
    public void getTableSchema_extractOriginalIdsFromQuery_returnsEmpty() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_extractOriginalIdsFromQuery_returnsEmpty() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM somewhere.else");

        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());

        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);

        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_emptyFieldName() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName(" ").build()
        ));
        GetTableLayoutRequest request = new GetTableLayoutRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null), Mockito.mock(Schema.class), Collections.emptySet());
        Optional<PartitionInfoResult> result = metadataProvider.getPartitionInfo(new TableName("base1", "table1"), request);
        assertFalse(result.isPresent());
    }

    @Test
    public void extractOriginalIds_nullQuery() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn(null);
        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());
        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);
        assertFalse(result.isPresent());
    }

    @Test
    public void getTableSchema_lookupTypeThrowsException() throws Exception {
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

        // The only field is a LOOKUP whose type resolution throws, so it is skipped (see the catch+continue in
        // getTableSchema). With no fields left, the schema is empty, not the pre-fix behavior of silently
        // including it with an unresolved type - that only "worked" because getLookupType was never actually
        // reached due to the (now fixed) dead-code bug in the LOOKUP-vs-FORMULA branch condition.
        assertFalse(result.isPresent());
        verify(larkBaseService).getLookupType("base1", "tblxxxx", "fldxxxx");
    }

    @Test
    public void getTableSchema_nullFieldName() throws Exception {
        when(athenaService.getAthenaQueryString(anyString())).thenReturn("SELECT * FROM \"base1\".\"table1\"");
        when(larkBaseService.getTableFields(anyString(), anyString())).thenReturn(List.of(
                ListFieldResponse.FieldItem.builder().fieldName(null).build()
        ));

        GetTableRequest request = new GetTableRequest(new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()), "queryId", "catalog", new TableName("base1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = metadataProvider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void discoverTableFields_lookupTypeThrowsException() throws Exception {
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

        // Same reasoning as getTableSchema_lookupTypeThrowsException: the only field is a LOOKUP whose type
        // resolution throws, so it's skipped (catch+continue), leaving no field mappings and an empty Optional.
        assertFalse(result.isPresent());
        verify(larkBaseService).getLookupType("base1", "tblxxxx", "fldxxxx");
    }

    @Test
    public void discoverTableFields_nullFieldName() throws Exception {
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
    public void getTableSchema_lookupTypeThrowsException_simplified() throws Exception {
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