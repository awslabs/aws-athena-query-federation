/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.google.bigquery.qpt.BigQueryQueryPassthrough.NAME;
import static com.amazonaws.athena.connectors.google.bigquery.qpt.BigQueryQueryPassthrough.QUERY;
import static com.amazonaws.athena.connectors.google.bigquery.qpt.BigQueryQueryPassthrough.SCHEMA_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryMetadataHandlerTest
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final String SCHEMA = "testSchema";
    private static final String TABLE = "testTable";
    private static final TableName TABLE_NAME = new TableName("dataset1", "table1");

    @Mock
    BigQuery bigQuery;

    @Mock
    SecretsManagerClient secretsManagerClient;

    private BigQueryMetadataHandler bigQueryMetadataHandler;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    private Job job;
    private JobStatus jobStatus;

    private Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
            "gcp_project_id", "testProject",
            "concurrencyLimit", "10",
            BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID, "dummySecret"
    );
    private MockedStatic<BigQueryUtils> mockedStatic;

    @Before
    public void setUp() {
        System.setProperty("aws.region", "us-east-1");
        MockitoAnnotations.openMocks(this);
        
        // Mock the SecretsManager response
        GetSecretValueResponse secretResponse = GetSecretValueResponse.builder()
                .secretString("dummy-secret-value")
                .build();
        when(secretsManagerClient.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(secretResponse);
        
        bigQueryMetadataHandler = new BigQueryMetadataHandler(new LocalKeyFactory(), secretsManagerClient, null, "BigQuery", "spill-bucket", "spill-prefix", configOptions);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        job = mock(Job.class);
        jobStatus = mock(JobStatus.class);
        mockedStatic = Mockito.mockStatic(BigQueryUtils.class);
        mockedStatic.when(() -> BigQueryUtils.getBigQueryClient(any(Map.class), any(String.class))).thenReturn(bigQuery);
        mockedStatic.when(() -> BigQueryUtils.getBigQueryClient(any(Map.class))).thenReturn(bigQuery);
        mockedStatic.when(() -> BigQueryUtils.getEnvBigQueryCredsSmId(any(Map.class))).thenReturn("dummySecret");
        mockedStatic.when(() -> BigQueryUtils.fixCaseForDatasetName(any(String.class), any(String.class), any(BigQuery.class))).thenCallRealMethod();
        mockedStatic.when(() -> BigQueryUtils.fixCaseForTableName(any(String.class), any(String.class), any(String.class), any(BigQuery.class))).thenCallRealMethod();
        mockedStatic.when(() -> BigQueryUtils.translateToArrowType(any(LegacySQLTypeName.class))).thenCallRealMethod();
        mockedStatic.when(() -> BigQueryUtils.getChildFieldList(any(com.google.cloud.bigquery.Field.class))).thenCallRealMethod();
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
        mockedStatic.close();
    }

    @Test
    public void testDoListSchemaNames() throws IOException
    {
        final int numDatasets = 5;
        BigQueryPage<Dataset> datasetPage =
                new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, numDatasets));
        when(bigQuery.listDatasets(nullable(String.class), nullable(BigQuery.DatasetListOption.class))).thenReturn(datasetPage);

        //This will test case insenstivity
        ListSchemasRequest request = new ListSchemasRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME.toLowerCase());
        ListSchemasResponse schemaNames = bigQueryMetadataHandler.doListSchemaNames(blockAllocator, request);

        assertNotNull(schemaNames);
        assertEquals("Schema count does not match!", numDatasets, schemaNames.getSchemas().size());
    }

    @Test
    public void testDoListTables() throws IOException
    {
        //Build mocks for Datasets
        final int numDatasets = 5;
        BigQueryPage<Dataset> datasetPage =
                new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, numDatasets));
        when(bigQuery.listDatasets(nullable(String.class))).thenReturn(datasetPage);

        //Get the first dataset name.
        String datasetName = datasetPage.iterateAll().iterator().next().getDatasetId().getDataset();

        final int numTables = 5;
        BigQueryPage<Table> tablesPage =
                new BigQueryPage<>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME,
                        datasetName, numTables));

        //This will test case insenstivity
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME.toLowerCase(),
                datasetName, null, UNLIMITED_PAGE_SIZE_VALUE);

        // This commented out line was used when the wrong UNLIMITED_PAGE_SIZE_VALUE was set, which
        // triggered a method invocation to the same name but different signature.

        when(bigQuery.listTables(nullable(DatasetId.class))).thenReturn(tablesPage);
        ListTablesResponse tableNames = bigQueryMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertNotNull(tableNames);
        assertEquals("Schema count does not match!", numTables, tableNames.getTables().size());
    }

    @Test
    public void testDoGetTable() throws IOException
    {
        //Build mocks for Datasets
        final int numDatasets = 5;
        BigQueryPage<Dataset> datasetPage =
                new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, numDatasets));
        when(bigQuery.listDatasets(nullable(String.class))).thenReturn(datasetPage);

        //Get the first dataset name.
        String datasetName = datasetPage.iterateAll().iterator().next().getDatasetId().getDataset();

        //Build mocks for Tables
        final int numTables = 5;
        BigQueryPage<Table> tablesPage =
                new BigQueryPage<>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME,
                        datasetName, numTables));

        String tableName = tablesPage.iterateAll().iterator().next().getTableId().getTable();

        when(bigQuery.listTables(nullable(DatasetId.class))).thenReturn(tablesPage);

        Schema tableSchema = BigQueryTestUtils.getTestSchema();
        StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(tableSchema).build();

        Table table = mock(Table.class);
        when(table.getDefinition()).thenReturn(tableDefinition);
        when(bigQuery.getTable(nullable(TableId.class))).thenReturn(table);
        //Make the call
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME,
                new TableName(datasetName, tableName), Collections.emptyMap());

        GetTableResponse response = bigQueryMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        assertNotNull(response);

        //Number of Fields
        assertEquals(tableSchema.getFields().size(), response.getSchema().getFields().size());

        Schema tableSchemaComplex = BigQueryTestUtils.getTestSchemaComplexSchema();
        StandardTableDefinition tableDefinitionComplex = StandardTableDefinition.newBuilder()
                .setSchema(tableSchemaComplex).build();

        when(table.getDefinition()).thenReturn(tableDefinitionComplex);
        when(bigQuery.getTable(nullable(TableId.class))).thenReturn(table);
        //Make the call
        GetTableRequest getTableRequest1 = new GetTableRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME,
                new TableName(datasetName, tableName), Collections.emptyMap());

        GetTableResponse responseComplex = bigQueryMetadataHandler.doGetTable(blockAllocator, getTableRequest1);

        assertNotNull(responseComplex);
        //Number of Fields
        assertEquals(tableSchemaComplex.getFields().size(), responseComplex.getSchema().getFields().size());
    }

    @Test
    public void testDoGetSplits() {

//        mockedStatic.when(() -> BigQueryUtils.fixCaseForDatasetName(any(String.class), any(String.class), any(BigQuery.class))).thenReturn("testDataset");
//        mockedStatic.when(() -> BigQueryUtils.fixCaseForTableName(any(String.class), any(String.class), any(String.class), any(BigQuery.class))).thenReturn("testTable");
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME,
                mock(Block.class), Collections.emptyList(), new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), null);
        // added schema with integer column countCol
        List<Field> testSchemaFields = List.of(Field.of("countCol", LegacySQLTypeName.INTEGER));
        Schema tableSchema = Schema.of(testSchemaFields);

        // mocked table row count as 15
        List<FieldValue> bigQueryRowValue = List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "15"));
        FieldValueList fieldValueList = FieldValueList.of(bigQueryRowValue,
                FieldList.of(testSchemaFields));
        List<FieldValueList> tableRows = List.of(fieldValueList);

        GetSplitsResponse response = bigQueryMetadataHandler.doGetSplits(blockAllocator, request);

        assertEquals(1, response.getSplits().size());
    }

    @Test(expected = Exception.class)
    public void testDoListSchemaNamesForException() throws IOException
    {
        ListSchemasRequest request = new ListSchemasRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME.toLowerCase());
        when(bigQueryMetadataHandler.doListSchemaNames(blockAllocator, request)).thenThrow(new BigQueryExceptions.TooManyTablesException());
        bigQueryMetadataHandler.doListSchemaNames(blockAllocator, request);
    }

    @Test
    public void testDoGetDataSourceCapabilities()
    {
        com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest request = 
            new com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest(federatedIdentity, QUERY_ID, CATALOG);
        com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse response = 
            bigQueryMetadataHandler.doGetDataSourceCapabilities(blockAllocator, request);
        assertNotNull(response);
        assertNotNull(response.getCapabilities());
    }

    @Test
    public void testDoGetDataSourceCapabilities_VerifyOptimizations()
    {
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(federatedIdentity, QUERY_ID, CATALOG);

        GetDataSourceCapabilitiesResponse response =
                bigQueryMetadataHandler.doGetDataSourceCapabilities(blockAllocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        assertEquals(CATALOG, response.getCatalogName());

        // Filter pushdown
        List<OptimizationSubType> filterPushdown = capabilities.get("supports_filter_pushdown");
        assertNotNull("Expected supports_filter_pushdown capability to be present", filterPushdown);
        assertEquals(2, filterPushdown.size());
        assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals("sorted_range_set")));
        assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals("nullable_comparison")));

        // Complex expression pushdown
        List<OptimizationSubType> complexPushdown = capabilities.get("supports_complex_expression_pushdown");
        assertNotNull("Expected supports_complex_expression_pushdown capability to be present", complexPushdown);
        assertEquals(1, complexPushdown.size());
        OptimizationSubType complexSubType = complexPushdown.get(0);
        assertEquals("supported_function_expression_types", complexSubType.getSubType());
        assertNotNull("Expected function expression types to be present", complexSubType.getProperties());
        assertFalse("Expected function expression types to be non-empty", complexSubType.getProperties().isEmpty());

        // Top-N pushdown
        List<OptimizationSubType> topNPushdown = capabilities.get("supports_top_n_pushdown");
        assertNotNull("Expected supports_top_n_pushdown capability to be present", topNPushdown);
        assertEquals(1, topNPushdown.size());
        assertEquals("SUPPORTS_ORDER_BY", topNPushdown.get(0).getSubType());
    }

    @Test
    public void testDoGetQueryPassthroughSchema_WhenEnabled_ShouldGetSucceeded() throws Exception {
        Map<String, String> queryPassthroughParameters = Map.of(
                SCHEMA_FUNCTION_NAME, "system.query",
                ENABLE_QUERY_PASSTHROUGH, "true",
                NAME, "query",
                SCHEMA_NAME, "system",
                QUERY, "select col1 from testTable");

        GetTableRequest getTableRequest = getTableRequest(queryPassthroughParameters);

        try (MockedStatic<BigQueryOptions> mockBigQueryOptions = mockStatic(BigQueryOptions.class)) {
            BigQueryOptions options = mock(BigQueryOptions.class);
            mockBigQueryOptions.when(BigQueryOptions::getDefaultInstance).thenReturn(options);
            when(options.getService()).thenReturn(bigQuery);

            Field field = Field.of("column1", LegacySQLTypeName.STRING);
            com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(field);

            JobStatistics.QueryStatistics stats = mock(JobStatistics.QueryStatistics.class);
            when(stats.getStatementType()).thenReturn(JobStatistics.QueryStatistics.StatementType.SELECT);
            when(stats.getSchema()).thenReturn(schema);
            when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
            when(job.getStatistics()).thenReturn(stats);

            GetTableResponse response = bigQueryMetadataHandler.doGetQueryPassthroughSchema(blockAllocator, getTableRequest);
            assertNotNull(response);
            assertEquals(CATALOG, response.getCatalogName());
            assertEquals(1, response.getSchema().getFields().size());
            verify(bigQuery).create(any(JobInfo.class));
        }
    }

    @Test
    public void testDoGetQueryPassthroughSchema_WithMissingQueryArg() {
        // Required QUERY parameter is missing
        Map<String, String> queryPassthroughParameters = Map.of(
                SCHEMA_FUNCTION_NAME, "system.query",
                ENABLE_QUERY_PASSTHROUGH, "true",
                NAME, "query",
                SCHEMA_NAME, "system");

        executeAndAssertTest(queryPassthroughParameters, "Missing Query Passthrough Argument: QUERY");
    }

    @Test
    public void testDoGetQueryPassthroughSchema_WithMissingQueryValue() {
        // Required QUERY parameter value is missing
        Map<String, String> queryPassthroughParameters = Map.of(
                SCHEMA_FUNCTION_NAME, "system.query",
                ENABLE_QUERY_PASSTHROUGH, "true",
                NAME, "query",
                SCHEMA_NAME, "system",
                QUERY, "");
        executeAndAssertTest(queryPassthroughParameters, "Missing Query Passthrough Value for Argument: QUERY");
    }

    @Test
    public void testDoGetQueryPassthroughSchema_WithWrongSchemaNameArg() {
        // Schema function name is incorrect
        Map<String, String> queryPassthroughParameters = Map.of(
                SCHEMA_FUNCTION_NAME, "wrong.query",
                ENABLE_QUERY_PASSTHROUGH, "true",
                NAME, "query",
                SCHEMA_NAME, "system",
                QUERY, "select col1 from testTable");
        executeAndAssertTest(queryPassthroughParameters, "Function Signature doesn't match implementation's");
    }

    private void executeAndAssertTest(Map<String, String> queryPassthroughParameters, String errorMessage) {
        GetTableRequest getTableRequest = getTableRequest(queryPassthroughParameters);

        Exception e = assertThrows(AthenaConnectorException.class, () ->
                bigQueryMetadataHandler.doGetQueryPassthroughSchema(blockAllocator, getTableRequest));
        assertTrue(e.getMessage().contains(errorMessage));
    }

    private GetTableRequest getTableRequest(Map<String, String> queryPassthroughParameters) {
        return new GetTableRequest(federatedIdentity,
                QUERY_ID, CATALOG,
                new TableName(SCHEMA, TABLE), queryPassthroughParameters);
    }

    @Test
    public void testDoGetQueryPassthroughSchema_WhenDisabled_ShouldThrowException() {
        GetTableRequest getTableRequest = getTableRequest(Collections.emptyMap());
        Exception e = assertThrows(IllegalArgumentException.class, () ->
                bigQueryMetadataHandler.doGetQueryPassthroughSchema(blockAllocator, getTableRequest));
        assertTrue(e.getMessage().contains("No Query passed through"));
    }
}
