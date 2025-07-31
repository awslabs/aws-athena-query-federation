/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.HintsSubtype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

public class PostGreSqlMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(PostGreSqlMetadataHandlerTest.class);

    private static final String FILTER_PUSHDOWN = DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.getOptimization();
    private static final String COMPLEX_EXPRESSION_PUSHDOWN = DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.getOptimization();
    private static final String DATA_SOURCE_HINTS = DataSourceOptimizations.DATA_SOURCE_HINTS.getOptimization();
    private static final String QUERY_PASSTHROUGH = "supports_query_passthrough";
    private static final String SORTED_RANGE_SET = FilterPushdownSubType.SORTED_RANGE_SET.getSubType();
    private static final String NULLABLE_COMPARISON = FilterPushdownSubType.NULLABLE_COMPARISON.getSubType();
    private static final String SUPPORTED_FUNCTIONS = ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES.getSubType();
    private static final String NON_DEFAULT_COLLATE = HintsSubtype.NON_DEFAULT_COLLATE.getSubType();
    private static final String CATALOG_NAME = "testCatalog";
    private static final int FILTER_PUSHDOWN_SIZE = 2;
    private static final int COMPLEX_EXPRESSION_SIZE = 1;
    private static final int HINTS_SIZE = 1;

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "postgres",
            "postgres://jdbc:postgresql://hostname/user=A&password=B");
    private PostGreSqlMetadataHandler postGreSqlMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator blockAllocator;

    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.postGreSqlMetadataHandler = new PostGreSqlMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
    }

    @Test
    public void doListPaginatedTables()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}};
        TableName[] expected = {new TableName("testSchema", "testTable")};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // insensitive search schema
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
        preparedSchemaStatement.setString(1, "testSchema");

        String[] columnNames = new String[] {"schema_name"};
        String[][] tableNameValues = new String[][]{new String[] {"testSchema"}};
        ResultSet caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult, caseInsensitiveSchemaResult);

        ListTablesResponse listTablesResponse = this.postGreSqlMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, 1));
        Assert.assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        Object[][] nextValues = {{"testSchema", "testTable2"}};
        TableName[] nextExpected = {new TableName("testSchema", "testTable2")};
        ResultSet nextResultSet = mockResultSet(schema, nextValues, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(nextResultSet);


        // insensitive search schema
        String sql2 = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        PreparedStatement preparedSchemaStatement2 = connection.prepareStatement(sql2);
        preparedSchemaStatement2.setString(1, "testSchema");

        String[] columnNames2 = new String[] {"schema_name"};
        String[][] tableNameValues2 = new String[][]{new String[] {"testSchema"}};
        caseInsensitiveSchemaResult = mockResultSet(columnNames2, tableNameValues2, new AtomicInteger(-1));
        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult, caseInsensitiveSchemaResult);

        listTablesResponse = this.postGreSqlMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "1", 10));
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
        Assert.assertNull(listTablesResponse.getNextToken());
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())
                        .addField(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.postGreSqlMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"child_schema", "child"};
        int[] types = {Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{"s0", "p0"}, {"s1", "p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.postGreSqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Arrays.asList("[partition_schema_name : s0], [partition_name : p0]", "[partition_schema_name : s1], [partition_name : p1]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getSchemaName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getTableName());
    }

    @Test
    public void testListTablesWithValidConnection() {
        try {
            String databaseName = "testSchema";
            Connection jdbcConnection = Mockito.mock(Connection.class);
            PreparedStatement tablesStatement = Mockito.mock(PreparedStatement.class);
            PreparedStatement matViewStatement = Mockito.mock(PreparedStatement.class);
            ResultSet tablesResultSet = Mockito.mock(ResultSet.class);
            ResultSet matViewResultSet = Mockito.mock(ResultSet.class);

            when(jdbcConnection.prepareStatement(anyString())).thenReturn(tablesStatement, matViewStatement);
            when(tablesStatement.executeQuery()).thenReturn(tablesResultSet);
            when(matViewStatement.executeQuery()).thenReturn(matViewResultSet);
            when(tablesResultSet.next()).thenReturn(true, false);
            when(matViewResultSet.next()).thenReturn(true, false);
            when(tablesResultSet.getString("TABLE_NAME")).thenReturn("table1");
            when(tablesResultSet.getString("TABLE_SCHEM")).thenReturn(databaseName);
            when(matViewResultSet.getString("TABLE_NAME")).thenReturn("matview1");
            when(matViewResultSet.getString("TABLE_SCHEM")).thenReturn(databaseName);

            List<TableName> result = postGreSqlMetadataHandler.listTables(jdbcConnection, databaseName);

            List<TableName> expected = ImmutableList.of(
                    new TableName(databaseName, "table1"),
                    new TableName(databaseName, "matview1")
            );

            assertEquals("Expected tables and materialized views", expected, result);
            Mockito.verify(tablesStatement).executeQuery();
            Mockito.verify(matViewStatement).setString(1, databaseName);
            Mockito.verify(matViewStatement).executeQuery();
        } catch (Exception e) {
            fail("Unexpected exception occurred: " + e.getMessage());
        }
    }


    @Test
    public void testListTablesWithNoMaterializedViews() {
        try {
            String databaseName = "testSchema";
            Connection jdbcConnection = Mockito.mock(Connection.class);
            PreparedStatement tablesStatement = Mockito.mock(PreparedStatement.class);
            PreparedStatement matViewStatement = Mockito.mock(PreparedStatement.class);
            ResultSet tablesResultSet = Mockito.mock(ResultSet.class);
            ResultSet matViewResultSet = Mockito.mock(ResultSet.class);

            when(jdbcConnection.prepareStatement(anyString())).thenReturn(tablesStatement, matViewStatement);
            when(tablesStatement.executeQuery()).thenReturn(tablesResultSet);
            when(matViewStatement.executeQuery()).thenReturn(matViewResultSet);
            when(tablesResultSet.next()).thenReturn(true, false);
            when(matViewResultSet.next()).thenReturn(false);
            when(tablesResultSet.getString("TABLE_NAME")).thenReturn("table1");
            when(tablesResultSet.getString("TABLE_SCHEM")).thenReturn(databaseName);

            List<TableName> result = postGreSqlMetadataHandler.listTables(jdbcConnection, databaseName);

            List<TableName> expected = ImmutableList.of(new TableName(databaseName, "table1"));
            assertEquals("Expected only tables", expected, result);
            Mockito.verify(tablesStatement).executeQuery();
            Mockito.verify(matViewStatement).setString(1, databaseName);
            Mockito.verify(matViewStatement).executeQuery();
        } catch (Exception e) {
            fail("Unexpected exception occurred: " + e.getMessage());
        }
    }

    @Test
    public void testListTablesWithDatabaseConnectionError() {
        try {
            String databaseName = "testSchema";
            Connection jdbcConnection = Mockito.mock(Connection.class);
            when(jdbcConnection.prepareStatement(anyString())).thenThrow(new SQLException("Database error"));

            postGreSqlMetadataHandler.listTables(jdbcConnection, databaseName);
            fail("Expected SQLException was not thrown");
        } catch (SQLException e) {
            assertEquals("Database error", e.getMessage());
        } catch (Exception e) {
            fail("Unexpected exception occurred: " + e.getMessage());
        }
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"child_schema", "child"};
        int[] types = {Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.postGreSqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(1, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Collections.singletonList("[partition_schema_name : *], [partition_name : *]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getSchemaName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        PostGreSqlMetadataHandler postGreSqlMetadataHandler = new PostGreSqlMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        postGreSqlMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"child_schema", "child"};
        int[] types = {Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{"s0", "p0"}, {"s1", "p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.postGreSqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.postGreSqlMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(ImmutableMap.of("partition_schema_name", "s0", "partition_name", "p0"));
        expectedSplits.add(ImmutableMap.of("partition_schema_name", "s1", "partition_name", "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsContinuation()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"child_schema", "child"};
        int[] types = {Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{"s0", "p0"}, {"s1", "p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.postGreSqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
        GetSplitsResponse getSplitsResponse = this.postGreSqlMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(ImmutableMap.of("partition_schema_name", "s1", "partition_name", "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTableWithArrayColumns()
            throws Exception
    {
        logger.info("doGetTableWithArrayColumns - enter");

        String[] schema = {"DATA_TYPE", "COLUMN_NAME",  "COLUMN_SIZE", "DECIMAL_DIGITS", "TYPE_NAME"};
        Object[][] values = {
                {Types.ARRAY, "bool_array", 0, 0, "_bool"},
                {Types.ARRAY, "smallint_array", 0, 0, "_int2"},
                {Types.ARRAY, "int_array", 0, 0, "_int4"},
                {Types.ARRAY, "bigint_array", 0, 0, "_int8"},
                {Types.ARRAY, "float_array", 0, 0, "_float4"},
                {Types.ARRAY, "double_array", 0, 0, "_float8"},
                {Types.ARRAY, "date_array", 0, 0, "_date"},
                {Types.ARRAY, "timestamp_array", 0, 0, "_timestamp"},
                {Types.ARRAY, "binary_array", 0, 0, "_bytea"},
                {Types.ARRAY, "decimal_array", 38, 2, "_numeric"},
                {Types.ARRAY, "string_array", 0, 0, "_text"},
                {Types.ARRAY, "uuid_array", 0, 0, "_uuid"}
        };
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder
                .addListField("bool_array", new ArrowType.Bool())
                .addListField("smallint_array", new ArrowType.Int(16, true))
                .addListField("int_array", new ArrowType.Int(32, true))
                .addListField("bigint_array", new ArrowType.Int(64, true))
                .addListField("float_array", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))
                .addListField("double_array", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                .addListField("date_array", new ArrowType.Date(DateUnit.DAY))
                .addListField("timestamp_array", new ArrowType.Date(DateUnit.MILLISECOND))
                .addListField("binary_array", new ArrowType.Utf8())
                .addListField("decimal_array", new ArrowType.Decimal(38, 2,128))
                .addListField("string_array", new ArrowType.Utf8())
                .addListField("uuid_array", new ArrowType.Utf8());
        postGreSqlMetadataHandler.getPartitionSchema("testCatalog").getFields()
                .forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        ResultSet caseInsensitiveSchemaResult = Mockito.mock(ResultSet.class);
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
        preparedSchemaStatement.setString(1, "testschema");

        String[] columnNames = new String[] {"schema_name"};
        String[][] tableNameValues = new String[][]{new String[] {"testSchema"}};
        caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));

        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult);

        TableName inputTableName = new TableName("testSchema", "testtable");
        columnNames = new String[] {"table_name"};
        tableNameValues = new String[][]{new String[] {"testTable"}};
        ResultSet resultSetName = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND lower(table_name) = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, "testSchema");
        preparedStatement.setString(2, "testtable");

        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSetName);
        String resolvedTableName = "testTable";

        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), resolvedTableName, null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = this.postGreSqlMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        logger.info("Schema: {}", getTableResponse.getSchema());

        TableName expectedTableName = new TableName("testSchema", "testTable");
        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(expectedTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());

        logger.info("doGetTableWithArrayColumns - exit");
   }

   @Test
   public void doGetTableMaterializedView()
           throws Exception
   {
       logger.info("doGetTableWithArrayColumns - enter");

       String[] schema = {"DATA_TYPE", "COLUMN_NAME",  "COLUMN_SIZE", "DECIMAL_DIGITS", "TYPE_NAME"};
       Object[][] values = {
               {Types.ARRAY, "bool_array", 0, 0, "_bool"},
               {Types.ARRAY, "int_array", 0, 0, "_int4"},
               {Types.ARRAY, "bigint_array", 0, 0, "_int8"},
               {Types.ARRAY, "float_array", 0, 0, "_float4"}
       };
       AtomicInteger rowNumber = new AtomicInteger(-1);
       ResultSet resultSet = mockResultSet(schema, values, rowNumber);

       SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
       expectedSchemaBuilder
               .addListField("bool_array", new ArrowType.Bool())
               .addListField("int_array", new ArrowType.Int(32, true))
               .addListField("bigint_array", new ArrowType.Int(64, true))
               .addListField("float_array", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
       postGreSqlMetadataHandler.getPartitionSchema("testCatalog").getFields()
               .forEach(expectedSchemaBuilder::addField);
       Schema expected = expectedSchemaBuilder.build();

       // Simulates table look up in information_schema.schemata and returns empty result set

       ResultSet caseInsensitiveSchemaResult = Mockito.mock(ResultSet.class);
       String sql = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
       PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
       preparedSchemaStatement.setString(1, "testschema");

       String[] columnNames = new String[] {"schema_name"};
       String[][] tableNameValues = new String[][]{new String[] {"testSchema"}};
       caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));

       Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult);
//
//       // Simulates table look up in information_schema.tables and returns empty result set, because materialized views are stored separately
       ResultSet caseInsensitiveTableResult = Mockito.mock(ResultSet.class);
       sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND lower(table_name) = ?";
       PreparedStatement preparedStatement = connection.prepareStatement(sql);
       preparedStatement.setString(1, "testSchema");
       preparedStatement.setString(2, "testmatview");


       Mockito.when(preparedStatement.executeQuery()).thenReturn(caseInsensitiveTableResult);
       Mockito.when(caseInsensitiveTableResult.next()).thenReturn(false);

       // Simulates Materialized View look up in pg_catalog.pgmatviews system table
       sql = "select matviewname as \"table_name\" from pg_catalog.pg_matviews mv where schemaname = ? and lower(matviewname) = ?";
       preparedStatement = connection.prepareStatement(sql);
       preparedStatement.setString(1, "testSchema");
       preparedStatement.setString(2, "testmatview");

       TableName inputTableName = new TableName("testSchema", "testmatview");
       columnNames = new String[] {"table_name"};
       tableNameValues = new String[][]{new String[] {"testMatView"}};
       caseInsensitiveTableResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));

       Mockito.when(preparedStatement.executeQuery()).thenReturn(caseInsensitiveTableResult);

       String resolvedTableName = "testMatView";

       Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), resolvedTableName, null)).thenReturn(resultSet);
       Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

       GetTableResponse getTableResponse = this.postGreSqlMetadataHandler.doGetTable(new BlockAllocatorImpl(),
               new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

       logger.info("Schema: {}", getTableResponse.getSchema());

       TableName expectedTableName = new TableName("testSchema", "testMatView");
       Assert.assertEquals(expected, getTableResponse.getSchema());
       Assert.assertEquals(expectedTableName, getTableResponse.getTableName());
       Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());

        logger.info("doGetTableWithArrayColumns - exit");
    }
    @Test
    public void testDoGetDataSourceCapabilitiesWithoutQueryPassthrough()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, "testQueryId", CATALOG_NAME);

        PostGreSqlMetadataHandler handler = new PostGreSqlMetadataHandler(
                databaseConnectionConfig,
                secretsManager,
                athena,
                jdbcConnectionFactory,
                ImmutableMap.of()
        );

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(blockAllocator, request);

        verifyCommonCapabilities(response);
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        Assert.assertNull("Query passthrough should not be present when disabled.", capabilities.get(QUERY_PASSTHROUGH));
    }

    @Test
    public void testDoGetDataSourceCapabilitiesWithQueryPassthrough()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, "testQueryId", CATALOG_NAME);

        PostGreSqlMetadataHandler handler = new PostGreSqlMetadataHandler(
                databaseConnectionConfig,
                secretsManager,
                athena,
                jdbcConnectionFactory,
                ImmutableMap.of("enable_query_passthrough", "true")
        );

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(blockAllocator, request);
        verifyCommonCapabilities(response);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        List<OptimizationSubType> passthrough = capabilities.get("SYSTEM.QUERY");
        assertNotNull("Query passthrough should be present when enabled.", passthrough);
        Assert.assertFalse("Query passthrough list should not be empty.", passthrough.isEmpty());
    }

    @Test
    public void testDoGetDataSourceCapabilitiesWithNonDefaultCollate()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, "testQueryId", CATALOG_NAME);

        PostGreSqlMetadataHandler handler = new PostGreSqlMetadataHandler(
                databaseConnectionConfig,
                secretsManager,
                athena,
                jdbcConnectionFactory,
                ImmutableMap.of("non_default_collate", "true")
        );

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(blockAllocator, request);
        verifyCommonCapabilities(response);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        List<OptimizationSubType> hints = capabilities.get(DATA_SOURCE_HINTS);
        assertNotNull("Expected " + DATA_SOURCE_HINTS + " capability to be present", hints);
        assertEquals(HINTS_SIZE, hints.size());
        Assert.assertTrue(hints.stream().anyMatch(subType -> subType.getSubType().equals(NON_DEFAULT_COLLATE)));
    }

    @Test
    public void testGetCharColumnsWithCharacterColumns() {
        try {
            String schema = "testSchema";
            String table = "testTable";
            String query = "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND data_type = 'character'";
            PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
            ResultSet resultSet = Mockito.mock(ResultSet.class);

            when(connection.prepareStatement(query)).thenReturn(preparedStatement);
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true, true, false);
            when(resultSet.getString("column_name")).thenReturn("col1", "col2");

            List<String> charColumns = PostGreSqlMetadataHandler.getCharColumns(connection, schema, table);

            assertEquals("Expected two character columns", Arrays.asList("col1", "col2"), charColumns);
            Mockito.verify(preparedStatement).setString(1, schema);
            Mockito.verify(preparedStatement).setString(2, table);
        } catch (Exception e) {
            fail("Unexpected exception occurred: " + e.getMessage());
        }
    }

    @Test
    public void testGetArrayArrowTypeFromTypeName() {
        // Test boolean array
        ArrowType boolType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_bool", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Bool", boolType.getClass().getName());

        // Test integer types
        ArrowType smallintType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_int2", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Int", smallintType.getClass().getName());

        ArrowType intType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_int4", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Int", intType.getClass().getName());

        ArrowType bigintType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_int8", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Int", bigintType.getClass().getName());

        // Test float types
        ArrowType float4Type = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_float4", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$FloatingPoint", float4Type.getClass().getName());

        ArrowType float8Type = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_float8", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$FloatingPoint", float8Type.getClass().getName());

        // Test date/timestamp types
        ArrowType dateType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_date", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Date", dateType.getClass().getName());

        ArrowType timestampType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_timestamp", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Date", timestampType.getClass().getName());

        // Test numeric type
        ArrowType numericType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_numeric", 10, 2);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Decimal", numericType.getClass().getName());

        // Test fallback for unknown type
        ArrowType unknownType = postGreSqlMetadataHandler.getArrayArrowTypeFromTypeName("_unknown", 0, 0);
        assertEquals("org.apache.arrow.vector.types.pojo.ArrowType$Utf8", unknownType.getClass().getName());
    }

    @Test
    public void testGetPartitionSchema() {
        Schema partitionSchema = postGreSqlMetadataHandler.getPartitionSchema("testCatalog");
        assertEquals(2, partitionSchema.getFields().size());
        assertEquals(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, partitionSchema.getFields().get(0).getName());
        assertEquals(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, partitionSchema.getFields().get(1).getName());
    }

    private void verifyCommonCapabilities(GetDataSourceCapabilitiesResponse response)
    {
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        logger.info("Capabilities: {}", capabilities);

        assertEquals(CATALOG_NAME, response.getCatalogName());

        // Verify filter pushdown capabilities
        List<OptimizationSubType> filterPushdown = capabilities.get(FILTER_PUSHDOWN);
        assertNotNull("Expected " + FILTER_PUSHDOWN + " capability to be present", filterPushdown);
        assertEquals(FILTER_PUSHDOWN_SIZE, filterPushdown.size());
        Assert.assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals(SORTED_RANGE_SET)));
        Assert.assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals(NULLABLE_COMPARISON)));

        // Verify complex expression pushdown capabilities
        List<OptimizationSubType> complexExpressionPushdown = capabilities.get(COMPLEX_EXPRESSION_PUSHDOWN);
        assertNotNull("Expected " + COMPLEX_EXPRESSION_PUSHDOWN + " capability to be present", complexExpressionPushdown);
        assertEquals(COMPLEX_EXPRESSION_SIZE, complexExpressionPushdown.size());
        Assert.assertTrue(complexExpressionPushdown.stream().anyMatch(subType ->
                subType.getSubType().equals(SUPPORTED_FUNCTIONS) &&
                        (!subType.getProperties().isEmpty())));
    }
}
