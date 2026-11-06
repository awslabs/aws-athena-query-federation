/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.mysql.MySqlConstants.MYSQL_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;

public class MySqlMetadataHandlerTest
        extends TestBase
{
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", MYSQL_NAME,
            "mysql://jdbc:mysql://hostname/user=A&password=B");
    private MySqlMetadataHandler mySqlMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator blockAllocator;
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG_NAME = "testCatalogName";

    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.mySqlMetadataHandler = new MySqlMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
    }

    @Test
    public void getPartitionSchema_withCatalogName_returnsSchemaWithPartitionColumn()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.mySqlMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout_withPartitions_returnsLayoutWithPartitionList()
            throws Exception
    {
        try (BlockAllocator blockAllocator = new BlockAllocatorImpl()) {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"partition_name"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Arrays.asList("[partition_name : p0]", "[partition_name : p1]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getSchemaName());
        }
    }

    @Test
    public void doGetTableLayout_withNoPartitions_returnsLayoutWithDefaultPartition()
            throws Exception
    {
        try (BlockAllocator blockAllocator = new BlockAllocatorImpl()) {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"partition_name"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Collections.singletonList("[partition_name : *]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getSchemaName());
        }
    }

    @Test
    public void doListTables_withPagination_returnsListOfTablesWithNextToken()
        throws Exception
    {
        try (BlockAllocator blockAllocator = new BlockAllocatorImpl()) {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}};
        TableName[] expected = {new TableName("testSchema", "testTable")};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ListTablesResponse listTablesResponse = this.mySqlMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, 1));
        Assert.assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        Object[][] nextValues = {{"testSchema", "testTable2"}};
        TableName[] nextExpected = {new TableName("testSchema", "testTable2")};
        ResultSet nextResultSet = mockResultSet(schema, nextValues, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(nextResultSet);

        listTablesResponse = this.mySqlMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "1", 1));
        Assert.assertEquals("2", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
        }
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayout_whenSqlException_throwsRuntimeException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        MySqlMetadataHandler mySqlMetadataHandler = new MySqlMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        mySqlMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits_withTableLayout_returnsListOfSplitsPerPartition()
            throws Exception
    {
        try (BlockAllocator blockAllocator = new BlockAllocatorImpl();
             BlockAllocator splitBlockAllocator = new BlockAllocatorImpl()) {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {MySqlMetadataHandler.PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.mySqlMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "p0"));
        expectedSplits.add(Collections.singletonMap(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
        }
    }

    @Test
    public void doGetSplitsWithCatalogCasingFilter()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {MySqlMetadataHandler.PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        FederatedIdentity identity = Mockito.mock(FederatedIdentity.class);
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(EnvironmentConstants.CATALOG_CASING_FILTER, EnvironmentConstants.UPPERCASE_ONLY);
        Mockito.when(identity.getConfigOptions()).thenReturn(configOptions);

        Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);
        GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(identity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.mySqlMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Assert.assertEquals(1, getSplitsResponse.getSplits().size());
        Split split = getSplitsResponse.getSplits().iterator().next();
        Assert.assertEquals(EnvironmentConstants.UPPERCASE_ONLY, split.getProperty(EnvironmentConstants.CATALOG_CASING_FILTER));
    }

    @Test
    public void doGetSplits_withContinuationToken_returnsListOfRemainingSplits()
            throws Exception
    {
        try (BlockAllocator blockAllocator = new BlockAllocatorImpl();
             BlockAllocator splitBlockAllocator = new BlockAllocatorImpl()) {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"partition_name"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        final String expectedQuery = String.format(MySqlMetadataHandler.GET_PARTITIONS_QUERY, tableName.getTableName(), tableName.getSchemaName());
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
        GetSplitsResponse getSplitsResponse = this.mySqlMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap("partition_name", "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
        }
    }

    @org.testng.annotations.Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "More than one table that matches 'testtable' was returned from Database testSchema")
    public void doGetTable_withCaseInsensitiveDuplicateTableNames_throwsRuntimeException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testtable");
        String[] columnNames = new String[] {"table_name"};
        String[][] tableNameValues = new String[][]{new String[] {"testTable"}, new String[] {"TestTable"}};
        ResultSet resultSetName = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        String sql = "SELECT table_name FROM information_schema.tables WHERE (table_name = 'testtable' or lower(table_name) = 'testtable') AND table_schema = 'testSchema'";
        Mockito.when(this.connection.prepareStatement(sql).executeQuery()).thenReturn(resultSetName);

        GetTableResponse getTableResponse = this.mySqlMetadataHandler.doGetTable(this.blockAllocator,
                new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @org.testng.annotations.Test(expectedExceptions = {RuntimeException.class}, expectedExceptionsMessageRegExp = "During Case Insensitive look up could not find Table testtable in Database testSchema")
    public void doGetTable_withCaseInsensitiveNoTablesFound_throwsRuntimeException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testtable");
        ResultSet resultSetName = Mockito.mock(ResultSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(resultSetName.next()).thenReturn(false);
        String sql = "SELECT table_name FROM information_schema.tables WHERE (table_name = 'testtable' or lower(table_name) = 'testtable') AND table_schema = 'testSchema'";
        Mockito.when(this.connection.prepareStatement(sql).executeQuery()).thenReturn(resultSetName);

        GetTableResponse getTableResponse = this.mySqlMetadataHandler.doGetTable(this.blockAllocator,
                new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doGetDataSourceCapabilities_withDefaultRequest_returnsDataSourceCapabilities()
    {
        try (BlockAllocator allocator = new BlockAllocatorImpl()) {
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(federatedIdentity, QUERY_ID, CATALOG_NAME);

        GetDataSourceCapabilitiesResponse response =
                mySqlMetadataHandler.doGetDataSourceCapabilities(allocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        assertEquals(CATALOG_NAME, response.getCatalogName());

        // Filter pushdown
        List<OptimizationSubType> filterPushdown = capabilities.get("supports_filter_pushdown");
        assertNotNull("Expected supports_filter_pushdown capability to be present", filterPushdown);
        assertEquals(2, filterPushdown.size());
        assertTrue("Expected filter pushdown to contain 'sorted_range_set' subtype",
                filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals("sorted_range_set")));
        assertTrue("Expected filter pushdown to contain 'nullable_comparison' subtype",
                filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals("nullable_comparison")));

        // Complex expression pushdown
        List<OptimizationSubType> complexPushdown = capabilities.get("supports_complex_expression_pushdown");
        assertNotNull("Expected supports_complex_expression_pushdown capability to be present", complexPushdown);
        assertEquals(1, complexPushdown.size());
        assertTrue("Expected complex pushdown to contain 'supported_function_expression_types' subtype with non-empty properties",
                complexPushdown.stream().anyMatch(subType ->
                subType.getSubType().equals("supported_function_expression_types") &&
                        !subType.getProperties().isEmpty()));
        }
    }

    @Test
    public void doGetSplits_withQueryPassthrough_returnsSingleSplitWithPassthroughArguments()
    {
        try (BlockAllocator blockAllocator = new BlockAllocatorImpl()) {
        TableName tableName = new TableName("testSchema", "testTable");

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.isQueryPassThrough()).thenReturn(true);
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put("arg1", "val1");
        passthroughArgs.put("arg2", "val2");
        Mockito.when(constraints.getQueryPassthroughArguments()).thenReturn(passthroughArgs);

        Block partitions = Mockito.mock(Block.class);

        GetSplitsRequest request = new GetSplitsRequest(
                federatedIdentity,
                "queryId",
                CATALOG_NAME,
                tableName,
                partitions,
                Collections.emptyList(),
                constraints,
                null
        );

        GetSplitsResponse response = mySqlMetadataHandler.doGetSplits(blockAllocator, request);

        // Assertions
        assertEquals(CATALOG_NAME, response.getCatalogName());
        assertEquals(1, response.getSplits().size());

        Map<String, String> actualProps = response.getSplits().iterator().next().getProperties();
        assertEquals(passthroughArgs, actualProps);
        }
    }

    @Test
    public void listTables_WithRows_ReturnsTableNameList()
            throws Exception
    {
        String listTablesQuery = "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ?";
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(listTablesQuery)).thenReturn(preparedStatement);
        String[] columns = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testTable2"}};
        ResultSet resultSet = mockResultSet(columns, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        List<TableName> tables = this.mySqlMetadataHandler.listTables(this.connection, "testSchema");

        Assert.assertArrayEquals(new TableName[] {
                new TableName("testSchema", "testTable"),
                new TableName("testSchema", "testTable2")
        }, tables.toArray());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "testSchema");
    }

    @Test
    public void listTables_NoRows_ReturnsEmptyList()
            throws Exception
    {
        String listTablesQuery = "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ?";
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(listTablesQuery)).thenReturn(preparedStatement);
        String[] columns = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {};
        ResultSet resultSet = mockResultSet(columns, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        List<TableName> tables = this.mySqlMetadataHandler.listTables(this.connection, "testSchema");

        assertTrue(tables.isEmpty());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "testSchema");
    }
}