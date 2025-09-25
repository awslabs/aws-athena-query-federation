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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.deltashare.model.DeltaShareTable;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeltaShareMetadataHandlerTest extends TestBase
{
    @Rule
    public TestName testName = new TestName();

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient athena;

    private DeltaShareMetadataHandler handler;

    @Override
    public void setUp()
    {
        super.setUp();
        handler = new DeltaShareMetadataHandler(
            new LocalKeyFactory(),
            secretsManager,
            athena,
            "test-spill-bucket",
            "test-spill-prefix",
            configOptions
        );
        
        try {
            java.lang.reflect.Field clientField = DeltaShareMetadataHandler.class.getDeclaredField("deltaShareClient");
            clientField.setAccessible(true);
            clientField.set(handler, mockDeltaShareClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mock client", e);
        }
    }

    @Test
    public void testDoListSchemaNames() throws Exception
    {
        List<String> mockSchemas = createMockSchemaList();
        when(mockDeltaShareClient.listSchemas(TEST_SHARE_NAME)).thenReturn(mockSchemas);

        ListSchemasRequest request = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse response = handler.doListSchemaNames(allocator, request);

        assertNotNull(response);
        assertEquals(TEST_CATALOG_NAME, response.getCatalogName());
        assertEquals(3, response.getSchemas().size());
        assertTrue(response.getSchemas().contains(TEST_SCHEMA_NAME));
        assertTrue(response.getSchemas().contains("schema1"));
        assertTrue(response.getSchemas().contains("schema2"));
    }

    @Test
    public void testDoListSchemaNamesWithEmptyResult() throws Exception
    {
        when(mockDeltaShareClient.listSchemas(TEST_SHARE_NAME)).thenReturn(Collections.emptyList());

        ListSchemasRequest request = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse response = handler.doListSchemaNames(allocator, request);

        assertNotNull(response);
        assertEquals(1, response.getSchemas().size());
        assertTrue(response.getSchemas().contains("default"));
    }

    @Test(expected = RuntimeException.class)
    public void testDoListSchemaNamesWithError() throws Exception
    {
        when(mockDeltaShareClient.listSchemas(TEST_SHARE_NAME)).thenThrow(new RuntimeException("Connection failed"));

        ListSchemasRequest request = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        handler.doListSchemaNames(allocator, request);
    }

    @Test
    public void testDoListTables() throws Exception
    {
        List<DeltaShareTable> mockTables = createMockTableList();
        when(mockDeltaShareClient.listTables(TEST_SHARE_NAME, TEST_SCHEMA_NAME)).thenReturn(mockTables);

        ListTablesRequest request = new ListTablesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_SCHEMA_NAME, null, 100);
        ListTablesResponse response = handler.doListTables(allocator, request);

        assertNotNull(response);
        assertEquals(TEST_CATALOG_NAME, response.getCatalogName());
        assertEquals(3, response.getTables().size());
        assertTrue(response.getTables().contains(new TableName(TEST_SCHEMA_NAME, "table1")));
        assertTrue(response.getTables().contains(new TableName(TEST_SCHEMA_NAME, "table2")));
        assertTrue(response.getTables().contains(TEST_TABLE));
    }

    @Test(expected = RuntimeException.class)
    public void testDoListTablesWithError() throws Exception
    {
        when(mockDeltaShareClient.listTables(TEST_SHARE_NAME, TEST_SCHEMA_NAME)).thenThrow(new RuntimeException("Schema not found"));

        ListTablesRequest request = new ListTablesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_SCHEMA_NAME, null, 100);
        handler.doListTables(allocator, request);
    }

    @Test
    public void testDoGetTable() throws Exception
    {
        JsonNode mockMetadata = createMockTableMetadata();
        when(mockDeltaShareClient.getTableMetadata(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(mockMetadata);

        GetTableRequest request = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE, Collections.emptyMap());
        GetTableResponse response = handler.doGetTable(allocator, request);

        assertNotNull(response);
        assertEquals(TEST_CATALOG_NAME, response.getCatalogName());
        assertEquals(TEST_TABLE, response.getTableName());
        
        Schema schema = response.getSchema();
        assertNotNull(schema);
        assertEquals(7, schema.getFields().size());
        assertEquals("deltashare", schema.getCustomMetadata().get("source"));
        assertEquals(TEST_SHARE_NAME, schema.getCustomMetadata().get("share"));
        assertEquals(TEST_SCHEMA_NAME, schema.getCustomMetadata().get("schema"));
        assertEquals(TEST_TABLE_NAME, schema.getCustomMetadata().get("table"));
        
        assertTrue(response.getPartitionColumns().isEmpty());
    }

    @Test
    public void testDoGetTableWithPartitions() throws Exception
    {
        JsonNode mockMetadata = createMockPartitionedTableMetadata();
        when(mockDeltaShareClient.getTableMetadata(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(mockMetadata);

        GetTableRequest request = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE, Collections.emptyMap());
        GetTableResponse response = handler.doGetTable(allocator, request);

        assertNotNull(response);
        Schema schema = response.getSchema();
        assertEquals("partition_col", schema.getCustomMetadata().get("partitionCols"));
        assertEquals(1, response.getPartitionColumns().size());
        assertTrue(response.getPartitionColumns().contains("partition_col"));
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetTableWithError() throws Exception
    {
        when(mockDeltaShareClient.getTableMetadata(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenThrow(new RuntimeException("Table not found"));

        GetTableRequest request = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE, Collections.emptyMap());
        handler.doGetTable(allocator, request);
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetTableWithNullMetadata() throws Exception
    {
        when(mockDeltaShareClient.getTableMetadata(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(null);

        GetTableRequest request = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE, Collections.emptyMap());
        handler.doGetTable(allocator, request);
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetTableWithMalformedMetadata() throws Exception
    {
        JsonNode malformedMetadata = objectMapper.readTree("{}");
        when(mockDeltaShareClient.getTableMetadata(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(malformedMetadata);

        GetTableRequest request = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE, Collections.emptyMap());
        handler.doGetTable(allocator, request);
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetTableLayoutWithQueryError() throws Exception
    {
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME))
            .thenThrow(new RuntimeException("Query execution failed"));

        Schema testSchema = createTestSchema();
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        GetTableLayoutRequest request = new GetTableLayoutRequest(
            TEST_IDENTITY,
            TEST_QUERY_ID,
            TEST_CATALOG_NAME,
            TEST_TABLE,
            constraints,
            testSchema,
            Collections.emptySet()
        );

        handler.doGetTableLayout(allocator, request);
    }

    @Test
    public void testDoGetTableLayoutWithEmptyQueryResult() throws Exception
    {
        JsonNode emptyQueryResponse = createMockEmptyQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(emptyQueryResponse);

        Schema testSchema = createTestSchema();
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        GetTableLayoutRequest request = new GetTableLayoutRequest(
            TEST_IDENTITY,
            TEST_QUERY_ID,
            TEST_CATALOG_NAME,
            TEST_TABLE,
            constraints,
            testSchema,
            Collections.emptySet()
        );

        GetTableLayoutResponse response = handler.doGetTableLayout(allocator, request);

        assertNotNull(response);
        assertNotNull(response.getPartitions());
        assertEquals(0, response.getPartitions().getRowCount());
    }

    @Test(expected = RuntimeException.class)
    public void testHandlerWithMissingShareNameConfiguration()
    {
        Map<String, String> invalidConfig = ImmutableMap.of(
            "endpoint", TEST_ENDPOINT,
            "token", TEST_TOKEN
        );

        new DeltaShareMetadataHandler(invalidConfig);
    }

    @Test(expected = RuntimeException.class)
    public void testHandlerWithEmptyShareNameConfiguration()
    {
        Map<String, String> invalidConfig = ImmutableMap.of(
            "endpoint", TEST_ENDPOINT,
            "token", TEST_TOKEN,
            "share_name", ""
        );

        new DeltaShareMetadataHandler(invalidConfig);
    }

    @Test
    public void testDoGetDataSourceCapabilities()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        assertNotNull(response);
        assertEquals(TEST_CATALOG_NAME, response.getCatalogName());
        
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        assertNotNull("Response should not be null", capabilities);
        // Verify that we have the expected capabilities
        assertFalse("Capabilities should not be empty", capabilities.isEmpty());
        
        // Check for the actual keys returned by the implementation
        boolean hasFilterPushdown = capabilities.keySet().stream()
                .anyMatch(key -> key.contains("filter_pushdown"));
        boolean hasLimitPushdown = capabilities.keySet().stream()
                .anyMatch(key -> key.contains("limit_pushdown"));
        boolean hasComplexExpressionPushdown = capabilities.keySet().stream()
                .anyMatch(key -> key.contains("complex_expression_pushdown"));
                
        assertTrue("Should support filter pushdown", hasFilterPushdown);
        assertTrue("Should support limit pushdown", hasLimitPushdown);
        assertTrue("Should support complex expression pushdown", hasComplexExpressionPushdown);
    }

    @Test
    public void testDoGetTableLayout() throws Exception
    {
        JsonNode mockQueryResponse = createMockQueryResponse();
        when(mockDeltaShareClient.queryTable(TEST_SHARE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME)).thenReturn(mockQueryResponse);

        Schema testSchema = createTestSchema();
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        GetTableLayoutRequest request = new GetTableLayoutRequest(
            TEST_IDENTITY,
            TEST_QUERY_ID,
            TEST_CATALOG_NAME,
            TEST_TABLE,
            constraints,
            testSchema,
            Collections.emptySet()
        );

        GetTableLayoutResponse response = handler.doGetTableLayout(allocator, request);

        assertNotNull(response);
        assertNotNull(response.getPartitions());
        assertTrue(response.getPartitions().getRowCount() > 0);
        
        Schema partitionSchema = response.getPartitions().getSchema();
        assertEquals(TEST_SHARE_NAME, partitionSchema.getCustomMetadata().get("share"));
        assertEquals(TEST_SCHEMA_NAME, partitionSchema.getCustomMetadata().get("schema"));
        assertEquals(TEST_TABLE_NAME, partitionSchema.getCustomMetadata().get("table"));
    }

    @Test
    public void testDoGetTableLayoutWithConstraints() throws Exception
    {
        Schema testSchema = createTestSchema();
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col1", EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true).add("test_value").build());
        
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        GetTableLayoutRequest request = new GetTableLayoutRequest(
            TEST_IDENTITY,
            TEST_QUERY_ID,
            TEST_CATALOG_NAME,
            TEST_TABLE,
            constraints,
            testSchema,
            Collections.emptySet()
        );

        GetTableLayoutResponse response = handler.doGetTableLayout(allocator, request);

        assertNotNull(response);
        assertNotNull(response.getPartitions());
    }

    @Test
    public void testDoGetSplits() throws Exception
    {
        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Block partitions = allocator.createBlock(SchemaBuilder.newBuilder()
            .addStringField("partition_id")
            .addIntField("file_count")
            .addStringField("processing_mode")
            .build());

        partitions.setValue("partition_id", 0, "test_partition");
        partitions.setValue("file_count", 0, 2);
        partitions.setValue("processing_mode", 0, "STANDARD");
        partitions.setRowCount(1);

        GetSplitsRequest request = new GetSplitsRequest(
            TEST_IDENTITY,
            TEST_QUERY_ID,
            TEST_CATALOG_NAME,
            TEST_TABLE,
            partitions,
            Collections.emptyList(),
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null),
            "continuationToken"
        );

        GetSplitsResponse response = handler.doGetSplits(allocator, request);

        assertNotNull(response);
        assertEquals(TEST_CATALOG_NAME, response.getCatalogName());
        assertFalse(response.getSplits().isEmpty());
        
        Split split = response.getSplits().iterator().next();
        assertNotNull(split);
        assertEquals("test_partition", split.getProperty("partition_id"));
    }

    @Test
    public void testDoGetSplitsWithLargeFiles() throws Exception
    {
        SpillLocation spillLocation = S3SpillLocation.newBuilder()
            .withBucket("test-bucket")
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(TEST_QUERY_ID)
            .withIsDirectory(true)
            .build();

        Block partitions = allocator.createBlock(SchemaBuilder.newBuilder()
            .addStringField("partition_id")
            .addStringField("presigned_url")
            .addIntField("row_group_index")
            .addStringField("processing_mode")
            .addBigIntField("file_size")
            .addIntField("total_row_groups")
            .build());

        partitions.setValue("partition_id", 0, "large_file_partition_rg_000");
        partitions.setValue("presigned_url", 0, "https://presigned-url-large.com/large-file.parquet");
        partitions.setValue("row_group_index", 0, 0);
        partitions.setValue("processing_mode", 0, "ROW_GROUP");
        partitions.setValue("file_size", 0, 134217728L);
        partitions.setValue("total_row_groups", 0, 4);
        partitions.setRowCount(1);

        GetSplitsRequest request = new GetSplitsRequest(
            TEST_IDENTITY,
            TEST_QUERY_ID,
            TEST_CATALOG_NAME,
            TEST_TABLE,
            partitions,
            Collections.emptyList(),
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null),
            "continuationToken"
        );

        GetSplitsResponse response = handler.doGetSplits(allocator, request);

        assertNotNull(response);
        assertFalse(response.getSplits().isEmpty());
        
        Split split = response.getSplits().iterator().next();
        assertEquals("ROW_GROUP", split.getProperty("processing_mode"));
        assertEquals("0", split.getProperty("row_group_index"));
    }
}
