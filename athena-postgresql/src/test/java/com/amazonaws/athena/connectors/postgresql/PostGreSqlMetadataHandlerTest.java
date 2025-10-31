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
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import java.util.UUID;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
    private static final String BLOCK_PARTITION_SCHEMA_COLUMN_NAME = "partition_schema_name";
    private static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    private static final int MAX_SPLITS_PER_REQUEST = 1000;
    private static final String SORTED_RANGE_SET = FilterPushdownSubType.SORTED_RANGE_SET.getSubType();
    private static final String NULLABLE_COMPARISON = FilterPushdownSubType.NULLABLE_COMPARISON.getSubType();
    private static final String SUPPORTED_FUNCTIONS = ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES.getSubType();
    private static final String NON_DEFAULT_COLLATE = HintsSubtype.NON_DEFAULT_COLLATE.getSubType();
    private static final String CATALOG_NAME = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_PREFIX = "test-prefix";
    private static final String SCHEMA_COLUMN = "schema_name";
    private static final String TABLE_NAME_COLUMN = "TABLE_NAME";
    private static final String TABLE_SCHEMA_COLUMN = "TABLE_SCHEM";
    private static final String COLUMN_NAME = "column_name";
    private static final String CHILD_SCHEMA = "child_schema";
    private static final String CHILD = "child";
    
    private static final int FILTER_PUSHDOWN_SIZE = 2;
    private static final int COMPLEX_EXPRESSION_SIZE = 1;
    private static final int HINTS_SIZE = 1;
    private static final String TABLES_SQL = "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ?";
    private static final String MATERIALIZED_VIEWS_SQL = "select matviewname as \"TABLE_NAME\", schemaname as \"TABLE_SCHEM\" from pg_catalog.pg_matviews mv where has_table_privilege(format('%I.%I', mv.schemaname, mv.matviewname), 'select') and schemaname = ?";

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
        String[] schema = {TABLE_SCHEMA_COLUMN, TABLE_NAME_COLUMN};
        Object[][] values = {{TEST_SCHEMA, TEST_TABLE}};
        TableName[] expected = {new TableName(TEST_SCHEMA, TEST_TABLE)};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // insensitive search schema
        String sql = "SELECT " + SCHEMA_COLUMN + " FROM information_schema.schemata WHERE lower(" + SCHEMA_COLUMN + ") = ?";
        PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
        preparedSchemaStatement.setString(1, TEST_SCHEMA);

        String[] columnNames = new String[] {SCHEMA_COLUMN};
        String[][] tableNameValues = new String[][]{new String[] {TEST_SCHEMA}};
        ResultSet caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult, caseInsensitiveSchemaResult);

        ListTablesResponse listTablesResponse = this.postGreSqlMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        CATALOG_NAME, TEST_SCHEMA, null, 1));
        Assert.assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        Object[][] nextValues = {{TEST_SCHEMA, "testTable2"}};
        TableName[] nextExpected = {new TableName(TEST_SCHEMA, "testTable2")};
        ResultSet nextResultSet = mockResultSet(schema, nextValues, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(nextResultSet);

        // insensitive search schema
        String sql2 = "SELECT " + SCHEMA_COLUMN + " FROM information_schema.schemata WHERE lower(" + SCHEMA_COLUMN + ") = ?";
        PreparedStatement preparedSchemaStatement2 = connection.prepareStatement(sql2);
        preparedSchemaStatement2.setString(1, TEST_SCHEMA);

        String[] columnNames2 = new String[] {SCHEMA_COLUMN};
        String[][] tableNameValues2 = new String[][]{new String[] {TEST_SCHEMA}};
        caseInsensitiveSchemaResult = mockResultSet(columnNames2, tableNameValues2, new AtomicInteger(-1));
        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult, caseInsensitiveSchemaResult);

        listTablesResponse = this.postGreSqlMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        CATALOG_NAME, TEST_SCHEMA, "1", 10));
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
        Assert.assertNull(listTablesResponse.getNextToken());
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())
                        .addField(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.postGreSqlMetadataHandler.getPartitionSchema(CATALOG_NAME));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {CHILD_SCHEMA, CHILD};
        int[] types = {Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{"s0", "p0"}, {"s1", "p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.postGreSqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getSchemaName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getTableName());

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
    }

    @Test
    public void testListTablesWithValidConnection() throws SQLException {
        // Create mock connection
        Connection mockConn = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        mockDatabaseMetadata(mockConn);

        // Mock regular tables result
        PreparedStatement tablesStatement = Mockito.mock(PreparedStatement.class);
        ResultSet tablesResultSet = mockResultSet(
            new String[]{TABLE_SCHEMA_COLUMN, TABLE_NAME_COLUMN},
            new Object[][]{{TEST_SCHEMA, "table1"}},
            new AtomicInteger(-1)
        );
        when(mockConn.prepareStatement(TABLES_SQL)).thenReturn(tablesStatement);
        when(tablesStatement.executeQuery()).thenReturn(tablesResultSet);

        // Mock materialized views result
        PreparedStatement matViewStatement = Mockito.mock(PreparedStatement.class);
        ResultSet matViewResultSet = mockResultSet(
            new String[]{TABLE_SCHEMA_COLUMN, TABLE_NAME_COLUMN},
            new Object[][]{{TEST_SCHEMA, "matview1"}},
            new AtomicInteger(-1)
        );
        when(mockConn.prepareStatement(MATERIALIZED_VIEWS_SQL)).thenReturn(matViewStatement);
        when(matViewStatement.executeQuery()).thenReturn(matViewResultSet);

        List<TableName> result = postGreSqlMetadataHandler.listTables(mockConn, TEST_SCHEMA);

        List<TableName> expected = ImmutableList.of(
                new TableName(TEST_SCHEMA, "table1"),
                new TableName(TEST_SCHEMA, "matview1")
        );

        assertEquals("Expected tables and materialized views", expected, result);
    }

    @Test
    public void testListTablesWithNoMaterializedViews() throws SQLException {
        // Create mock connection
        Connection mockConn = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        mockDatabaseMetadata(mockConn);

        // Mock regular tables result
        PreparedStatement tablesStatement = Mockito.mock(PreparedStatement.class);
        ResultSet tablesResultSet = mockResultSet(
            new String[]{TABLE_SCHEMA_COLUMN, TABLE_NAME_COLUMN},
            new Object[][]{{TEST_SCHEMA, "table1"}},
            new AtomicInteger(-1)
        );
        when(mockConn.prepareStatement(TABLES_SQL)).thenReturn(tablesStatement);
        when(tablesStatement.executeQuery()).thenReturn(tablesResultSet);

        // Mock materialized views with no results
        PreparedStatement matViewStatement = Mockito.mock(PreparedStatement.class);
        ResultSet emptyResultSet = mockResultSet(
            new String[]{TABLE_NAME_COLUMN},
            new Object[][]{},
            new AtomicInteger(-1)
        );
        when(mockConn.prepareStatement(MATERIALIZED_VIEWS_SQL)).thenReturn(matViewStatement);
        when(matViewStatement.executeQuery()).thenReturn(emptyResultSet);

        List<TableName> result = postGreSqlMetadataHandler.listTables(mockConn, TEST_SCHEMA);

        List<TableName> expected = ImmutableList.of(new TableName(TEST_SCHEMA, "table1"));
        assertEquals("Expected only tables", expected, result);
    }

    @Test
    public void testListTablesWithDatabaseConnectionError() throws SQLException {
        Connection jdbcConnection = Mockito.mock(Connection.class);
        when(jdbcConnection.prepareStatement(anyString())).thenThrow(new SQLException("Database error"));

        SQLException thrown = assertThrows(SQLException.class, () ->
                postGreSqlMetadataHandler.listTables(jdbcConnection, TEST_SCHEMA)
        );

        assertEquals("Database error", thrown.getMessage());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {CHILD_SCHEMA, CHILD};
        int[] types = {Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.postGreSqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getSchemaName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getTableName());

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
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

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
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {CHILD_SCHEMA, CHILD};
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
        expectedSplits.add(ImmutableMap.of(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, "s0", BLOCK_PARTITION_COLUMN_NAME, "p0"));
        expectedSplits.add(ImmutableMap.of(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, "s1", BLOCK_PARTITION_COLUMN_NAME, "p1"));
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
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.postGreSqlMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {CHILD_SCHEMA, CHILD};
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
    public void testSchemaCaseSensitivityHandling() throws SQLException {
        String mixedCaseSchema = "TestSchema";
        String lowerCaseSchema = "testschema";

        Connection mockConn = mockJdbcConnection(null, null);
        
        String schemaSql = "SELECT " + SCHEMA_COLUMN + " FROM information_schema.schemata WHERE lower(" + SCHEMA_COLUMN + ") = ?";
        mockPreparedStatement(mockConn, schemaSql, SCHEMA_COLUMN, new Object[]{mixedCaseSchema});
        
        Object[][] tableValues = {{mixedCaseSchema, "table1"}};
        ResultSet tablesRs = mockResultSetWithMetadata(
            new String[]{TABLE_SCHEMA_COLUMN, TABLE_NAME_COLUMN},
                tableValues
        );
        mockPreparedStatementWithResultSet(mockConn, TABLES_SQL, tablesRs);
        
        // Mock materialized views query to return no results
        mockPreparedStatement(mockConn, MATERIALIZED_VIEWS_SQL, TABLE_NAME_COLUMN, new Object[]{});

        // Execute test
        List<TableName> result = postGreSqlMetadataHandler.listTables(mockConn, lowerCaseSchema);

        // Verify original case was preserved in the result
        assertEquals(1, result.size());
        assertEquals(mixedCaseSchema, result.get(0).getSchemaName());
        assertEquals("table1", result.get(0).getTableName());
    }

    @Test
    public void testTableNameCaseSensitivityHandling() throws Exception {
        String schemaName = TEST_SCHEMA;
        String mixedCaseTable = "TestTable";
        String lowerCaseTable = "testtable";

        Connection mockConn = mockJdbcConnection(null, null);
        
        String schemaSql = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        mockPreparedStatement(mockConn, schemaSql, "schema_name", new Object[]{schemaName});
        
        Object[][] tableValues = {{schemaName, mixedCaseTable}};
        ResultSet tablesRs = mockResultSetWithMetadata(
            new String[]{TABLE_SCHEMA_COLUMN, TABLE_NAME_COLUMN},
                tableValues
        );
        mockPreparedStatementWithResultSet(mockConn, TABLES_SQL, tablesRs);
        
        mockPreparedStatement(mockConn, MATERIALIZED_VIEWS_SQL, TABLE_NAME_COLUMN, new Object[]{});

        List<TableName> result = postGreSqlMetadataHandler.listTables(mockConn, lowerCaseTable.toLowerCase());

        assertEquals(1, result.size());
        assertEquals(schemaName, result.get(0).getSchemaName());
        assertEquals(mixedCaseTable, result.get(0).getTableName());
    }

    @Test
    public void testListTablesWithInvalidSchema() throws Exception {
        String invalidSchema = "nonexistentschema";
        
        Connection jdbcConnection = mockJdbcConnection(null, null);
        
        String schemaSql = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        mockPreparedStatement(jdbcConnection, schemaSql, "schema_name", new Object[]{});
        
        mockPreparedStatement(jdbcConnection, TABLES_SQL, TABLE_NAME_COLUMN, new Object[]{});
        mockPreparedStatement(jdbcConnection, MATERIALIZED_VIEWS_SQL, TABLE_NAME_COLUMN, new Object[]{});

        List<TableName> result = postGreSqlMetadataHandler.listTables(jdbcConnection, invalidSchema);

        assertTrue("Expected empty list for invalid schema", result.isEmpty());

        result = postGreSqlMetadataHandler.listTables(jdbcConnection, null);
        assertTrue("Expected empty list for null schema", result.isEmpty());
    }

    @Test
    public void testGetCharColumnsWithEmptyResultSet() throws SQLException {
        String schema = TEST_SCHEMA;
        String table = TEST_TABLE;
        String query = "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND data_type = 'character'";
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Connection jdbcConnection = Mockito.mock(Connection.class);

        when(jdbcConnection.prepareStatement(query)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false); // Empty result set

        List<String> charColumns = PostGreSqlMetadataHandler.getCharColumns(jdbcConnection, schema, table);

        Mockito.verify(preparedStatement).setString(1, schema);
        Mockito.verify(preparedStatement).setString(2, table);
        assertTrue("Expected empty list for table with no character columns", charColumns.isEmpty());
    }

    @Test(expected = SQLException.class)
    public void testGetCharColumnsWithSQLException() throws SQLException {
        String query = "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND data_type = 'character'";

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Connection jdbcConnection = Mockito.mock(Connection.class);

        when(jdbcConnection.prepareStatement(query)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("Database connection error"));

        PostGreSqlMetadataHandler.getCharColumns(jdbcConnection, TEST_SCHEMA, TEST_TABLE);
    }

    @Test
    public void testGetTableWithNullArrayTypes() throws Exception {
        logger.info("testGetTableWithNullArrayTypes - enter");

        String[] schema = {"DATA_TYPE", "COLUMN_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "TYPE_NAME"};
        Object[][] values = {
                {Types.ARRAY, "null_array", null, null, "_int4"},
                {Types.ARRAY, "null_size_array", 0, null, "_numeric"},
                {Types.ARRAY, "null_type_array", 0, 0, null}
        };
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        // Mock schema validation
        ResultSet caseInsensitiveSchemaResult;
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
        preparedSchemaStatement.setString(1, TEST_SCHEMA);

        String[] columnNames = new String[] {"schema_name"};
        String[][] tableNameValues = new String[][]{new String[] {TEST_SCHEMA}};
        caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult);

        // Mock table name validation
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        columnNames = new String[] {"table_name"};
        tableNameValues = new String[][]{new String[] {TEST_TABLE}};
        ResultSet resultSetName = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND lower(table_name) = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, TEST_SCHEMA);
        preparedStatement.setString(2, TEST_TABLE);

        when(preparedStatement.executeQuery()).thenReturn(resultSetName);

        when(connection.getMetaData().getColumns(CATALOG_NAME, inputTableName.getSchemaName(), TEST_TABLE, null))
                .thenReturn(resultSet);
        when(connection.getCatalog()).thenReturn(CATALOG_NAME);

        // Execute test
        GetTableResponse response = this.postGreSqlMetadataHandler.doGetTable(
                new BlockAllocatorImpl(),
                new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME, inputTableName, Collections.emptyMap())
        );

        // Verify the schema contains the expected fields with appropriate defaults
        Schema schema1 = response.getSchema();
        assertEquals(5, schema1.getFields().size()); // 3 array fields + 2 partition fields

        // Check null_array field (should default to Int32 array)
        Field nullArrayField = schema1.findField("null_array");
        assertNotNull(nullArrayField);
        assertTrue(nullArrayField.getType() instanceof ArrowType.List);
        assertTrue(nullArrayField.getType() instanceof ArrowType.List);
        assertEquals("List", nullArrayField.getType().toString());

        // Check null_size_array field (should be decimal array with default precision)
        Field nullSizeArrayField = schema1.findField("null_size_array");
        assertNotNull(nullSizeArrayField);
        assertTrue(nullSizeArrayField.getType() instanceof ArrowType.List);
        assertEquals("List", nullSizeArrayField.getType().toString());

        // Check null_type_array field (should default to Utf8 array)
        Field nullTypeArrayField = schema1.findField("null_type_array");
        assertNotNull(nullTypeArrayField);
        assertTrue(nullTypeArrayField.getType() instanceof ArrowType.List);
        assertEquals("List", nullTypeArrayField.getType().toString());

        logger.info("testGetTableWithNullArrayTypes - exit");
    }

    @Test
    public void testDoGetSplitsWithPartitionSplitter() throws Exception {
        // Create a subclass to override getSplitClauses
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("spill_bucket", TEST_BUCKET);
        configOptions.put("spill_prefix", TEST_PREFIX);
        configOptions.put("disable_spill_encryption", "true");

        PostGreSqlMetadataHandler handler = new PostGreSqlMetadataHandler(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions) {
            @Override
            protected List<String> getSplitClauses(TableName tableName) {
                return Arrays.asList("split1", "split2", "split3");
            }

            @Override
            protected SpillLocation makeSpillLocation(MetadataRequest request) {
                return S3SpillLocation.newBuilder()
                    .withBucket(TEST_BUCKET)
                    .withPrefix(TEST_PREFIX)
                    .withQueryId(request.getQueryId())
                    .withSplitId(UUID.randomUUID().toString())
                    .build();
            }
        };
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Map<String, String> queryPassthroughArgs = new HashMap<>();
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = handler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        // Create mock connection with database metadata
        Connection mockConn = mockJdbcConnection(null, null);

        // Mock the partition query to return a single partition with "*" values
        String[] columns = {CHILD_SCHEMA, CHILD};
        Object[][] values = {{"*", "*"}};
        ResultSet partitionRs = mockResultSetWithMetadata(columns, values);
        mockPreparedStatementWithResultSet(mockConn, PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY, partitionRs);

        // Get the table layout first
        when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(mockConn);
        GetTableLayoutResponse getTableLayoutResponse = handler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, CATALOG_NAME,
            tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);

        GetSplitsResponse getSplitsResponse = handler.doGetSplits(blockAllocator, getSplitsRequest);

        Assert.assertEquals("Expected 3 splits", 3, getSplitsResponse.getSplits().size());

        // Verify that each split has the expected partition properties
        Set<Split> splitsSet = getSplitsResponse.getSplits();
        List<Split> splits = new ArrayList<>(splitsSet);
        for (Split split : splits) {
            // Check that partition properties are set correctly
            Assert.assertEquals("*", split.getProperty(BLOCK_PARTITION_SCHEMA_COLUMN_NAME));
            // Note: split names may not be in exact order as returned as a Set
            Assert.assertTrue("Split partition name should start with 'split'",
                    split.getProperty(BLOCK_PARTITION_COLUMN_NAME).startsWith("split"));
            // Verify that split has some properties (spill location and encryption key may be set by the framework)
            Assert.assertFalse("Split should have properties", split.getProperties().isEmpty());
        }

        // Verify that we don't exceed max splits
        Assert.assertTrue("Number of splits should not exceed max splits",
            getSplitsResponse.getSplits().size() <= MAX_SPLITS_PER_REQUEST);
    }

    @Test
    public void testGetCharColumnsWithCharacterColumns() throws SQLException {
        String query = "SELECT " + COLUMN_NAME + " FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND data_type = 'character'";

        Connection mockConn = mockJdbcConnection(TEST_SCHEMA, TEST_TABLE);
        Object[] columnValues = {"col1", "col2"};
        mockPreparedStatement(mockConn, query, COLUMN_NAME, columnValues);

        List<String> charColumns = PostGreSqlMetadataHandler.getCharColumns(mockConn, TEST_SCHEMA, TEST_TABLE);

        // Verify the prepared statement parameters
        PreparedStatement stmt = mockConn.prepareStatement(query);
        Mockito.verify(stmt).setString(1, TEST_SCHEMA);
        Mockito.verify(stmt).setString(2, TEST_TABLE);
        
        assertEquals("Expected two character columns", Arrays.asList("col1", "col2"), charColumns);
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

    private ResultSet mockResultSetWithMetadata(String[] columnNames, Object[][] data) throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        AtomicInteger rowCounter = new AtomicInteger(-1);

        when(resultSet.next()).thenAnswer(invocation -> {
            int currentRow = rowCounter.incrementAndGet();
            return currentRow < data.length;
        });

        // Mock getObject and type-specific getters for each column
        for (int i = 0; i < columnNames.length; i++) {
            final int colIndex = i;
            when(resultSet.getObject(columnNames[colIndex])).thenAnswer(invocation -> {
                int currentRow = rowCounter.get();
                return currentRow >= 0 && currentRow < data.length ? data[currentRow][colIndex] : null;
            });
            when(resultSet.getString(columnNames[colIndex])).thenAnswer(invocation -> {
                Object value = resultSet.getObject(columnNames[colIndex]);
                return value != null ? value.toString() : null;
            });
        }

        return resultSet;
    }


    private Connection mockJdbcConnection(String schemaName, String tableName) throws SQLException {
        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        
        // Setup database metadata
        mockDatabaseMetadata(connection);
        
        // Setup schema and table validation if needed
        if (schemaName != null) {
            mockSchemaAndTableValidation(connection, schemaName, tableName);
        }
        
        return connection;
    }

    private void mockPreparedStatementWithResultSet(Connection connection, String sql, ResultSet resultSet) throws SQLException {
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        when(connection.prepareStatement(sql)).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
    }

    private void mockPreparedStatement(Connection connection, String sql, String columnName, Object[] values) throws SQLException {
        ResultSet resultSet = mockSimpleResultSet(columnName, values);
        mockPreparedStatementWithResultSet(connection, sql, resultSet);
    }

    private ResultSet mockSimpleResultSet(String columnName, Object[] values) throws SQLException {
        return mockResultSetWithMetadata(
            new String[]{columnName},
                Arrays.stream(values).map(v -> new Object[]{v}).toArray(Object[][]::new)
        );
    }

    private void mockDatabaseMetadata(Connection connection) throws SQLException {
        DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);
        when(metadata.getSearchStringEscape()).thenReturn("\\");
        when(connection.getMetaData()).thenReturn(metadata);
        when(connection.getCatalog()).thenReturn(PostGreSqlMetadataHandlerTest.CATALOG_NAME);
    }

    private void mockSchemaAndTableValidation(Connection connection, String schemaName, String tableName) throws SQLException {
        // Mock schema validation
        String schemaSql = "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        PreparedStatement schemaStatement = Mockito.mock(PreparedStatement.class);
        ResultSet schemaResultSet = Mockito.mock(ResultSet.class);
        
        when(connection.prepareStatement(schemaSql)).thenReturn(schemaStatement);
        when(schemaStatement.executeQuery()).thenReturn(schemaResultSet);
        when(schemaResultSet.next()).thenReturn(true);  // Only one result
        when(schemaResultSet.getString("schema_name")).thenReturn(schemaName);

        // Mock table name validation if a table name is provided
        String resolvedTableName;
        if (tableName != null) {
            String tableSql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND lower(table_name) = ?";
            PreparedStatement tableStatement = Mockito.mock(PreparedStatement.class);
            ResultSet tableResultSet = Mockito.mock(ResultSet.class);

            when(connection.prepareStatement(tableSql)).thenReturn(tableStatement);
            when(tableStatement.executeQuery()).thenReturn(tableResultSet);
            when(tableResultSet.next()).thenReturn(true, false);
            resolvedTableName = tableName;
            when(tableResultSet.getString("table_name")).thenReturn(resolvedTableName);
        }

    }

}
