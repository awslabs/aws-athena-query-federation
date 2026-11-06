/*-
 * #%L
 * athena-clickhouse
 * %%
 * Copyright (C) 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.clickhouse;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
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
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

public class ClickHouseMetadataHandlerTest
        extends TestBase
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_CONNECTION_STRING = "clickhouse://jdbc:clickhouse://localhost/user=A&password=B";
    private static final String TEST_SECRET_STRING = "{\"username\": \"testUser\", \"password\": \"testPassword\"}";
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("default",
            ClickHouseConstants.NAME,
            TEST_CONNECTION_STRING);
    private ClickHouseMetadataHandler metadataHandler;
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
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(TEST_SECRET).build()))).thenReturn(GetSecretValueResponse.builder().secretString(TEST_SECRET_STRING).build());
        this.metadataHandler = new ClickHouseMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
    }

    @After
    public void tearDown()
    {
        if (this.blockAllocator != null) {
            this.blockAllocator.close();
        }
    }

    @Test
    public void getPartitionSchema_defaultRequest_returnsPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(ClickHouseMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.metadataHandler.getPartitionSchema(TEST_CATALOG));
    }

    @Test
    public void doListTables_withPagination_returnsTablesAndNextToken()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ClickHouseMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{TEST_SCHEMA, TEST_TABLE}};
        TableName[] expected = {new TableName(TEST_SCHEMA, TEST_TABLE)};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ListTablesResponse listTablesResponse = this.metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        TEST_CATALOG, TEST_SCHEMA, null, 1));
        Assert.assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ClickHouseMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        Object[][] nextValues = {{TEST_SCHEMA, "testTable2"}};
        TableName[] nextExpected = {new TableName(TEST_SCHEMA, "testTable2")};
        ResultSet nextResultSet = mockResultSet(schema, nextValues, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(nextResultSet);

        listTablesResponse = this.metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        TEST_CATALOG, TEST_SCHEMA, "1", 1));
        Assert.assertEquals("2", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayout_whenSQLExceptionOccurs_throwsRuntimeException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.metadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        ClickHouseMetadataHandler metadataHandler = new ClickHouseMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        metadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits_withDefaultPartitions_returnsWildcardSplit()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ClickHouseMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {ClickHouseMetadataHandler.PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"*"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.metadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.metadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.metadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(ClickHouseMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "*"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplits_withQueryPassThroughEnabled_returnsPassthroughSplit()
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        // Create constraints with QPT enabled
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.isQueryPassThrough()).thenReturn(true);

        // Create a mock partitions block
        com.amazonaws.athena.connector.lambda.data.Block partitions = Mockito.mock(com.amazonaws.athena.connector.lambda.data.Block.class);
        Mockito.when(partitions.getRowCount()).thenReturn(1);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(
                this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName,
                partitions, new ArrayList<>(), constraints, null);

        // This should call setupQueryPassthroughSplit and return QPT splits
        GetSplitsResponse response = this.metadataHandler.doGetSplits(this.blockAllocator, getSplitsRequest);

        Assert.assertNotNull("Response should not be null", response);
        Assert.assertEquals("Catalog name should match", TEST_CATALOG, response.getCatalogName());
        Assert.assertNotNull("Splits should not be null", response.getSplits());
        Assert.assertEquals("Splits count should match", 1, response.getSplits().size());
    }

    @Test
    public void doGetSplits_withQueryPassThroughDisabled_returnsNormalSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.isQueryPassThrough()).thenReturn(false);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ClickHouseMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {ClickHouseMetadataHandler.PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"*"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.metadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.metadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.metadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        Assert.assertNotNull(getSplitsResponse);
        Assert.assertEquals(TEST_CATALOG, getSplitsResponse.getCatalogName());
        Assert.assertNotNull(getSplitsResponse.getSplits());

        // Should create normal splits, not QPT splits
        Assert.assertTrue("Should create normal splits when QPT is disabled", getSplitsResponse.getSplits().size() > 0);
    }

    @Test
    public void doGetDataSourceCapabilities_withQueryPassthroughEnabled_returnsCapabilities()
    {
        // Test with QPT enabled in config options
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("query_passthrough", "true");

        ClickHouseMetadataHandler metadataHandlerWithQPT = new ClickHouseMetadataHandler(
                databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, configOptions);

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);

        GetDataSourceCapabilitiesResponse response = metadataHandlerWithQPT.doGetDataSourceCapabilities(this.blockAllocator, request);

        Assert.assertNotNull("Response should not be null", response);
        Assert.assertEquals("Catalog name should match", TEST_CATALOG, response.getCatalogName());
        Assert.assertNotNull("Capabilities should not be null", response.getCapabilities());

        // Verify QPT capability is present when enabled
        Assert.assertFalse("Should have capabilities when QPT is enabled", response.getCapabilities().isEmpty());
    }
    
    @Test
    public void doListSchemaNames_withValidCatalog_returnsSchemaNames()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        
        String[] schemaColumns = {"DATABASE_SCHEMA"};
        Object[][] schemaValues = {{TEST_SCHEMA}, {"default"}, {"system"}};
        ResultSet schemaResultSet = mockResultSet(schemaColumns, schemaValues, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(ClickHouseMetadataHandler.LIST_SCHEMA_QUERY)).thenReturn(schemaResultSet);
        
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);
        ListSchemasResponse listSchemasResponse = this.metadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        
        Assert.assertNotNull(listSchemasResponse);
        Assert.assertEquals(TEST_CATALOG, listSchemasResponse.getCatalogName());
        Assert.assertNotNull(listSchemasResponse.getSchemas());
        // information_schema is filtered out, so we get testSchema, default, system
        Assert.assertTrue(listSchemasResponse.getSchemas().contains(TEST_SCHEMA));
        Assert.assertTrue(listSchemasResponse.getSchemas().contains("default"));
        Assert.assertTrue(listSchemasResponse.getSchemas().contains("system"));
    }
    
    @Test(expected = SQLException.class)
    public void doListSchemaNames_whenSQLExceptionOccurs_throwsSQLException()
            throws Exception
    {
        Mockito.when(this.connection.createStatement()).thenThrow(new SQLException("Connection failed"));
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);
        
        this.metadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
    }
    
    @Test
    public void doGetTable_withValidRequest_returnsTableSchemaWithColumns()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] columnsSchema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        int[] columnTypes = {Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER};
        Object[][] values = {
                {Types.INTEGER, 12, "col1", 0, 0},
                {Types.VARCHAR, 255, "col2", 0, 0}
        };
        ResultSet resultSet = mockResultSet(columnsSchema, columnTypes, values, new AtomicInteger(-1));
        
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        Mockito.when(this.connection.getMetaData().getColumns(TEST_CATALOG, inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(this.connection.getCatalog()).thenReturn(TEST_CATALOG);
        
        GetTableResponse getTableResponse = this.metadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
        
        Assert.assertNotNull(getTableResponse.getSchema());
        // Schema includes data columns plus partition column(s)
        Assert.assertTrue("Schema should have at least 2 data columns", getTableResponse.getSchema().getFields().size() >= 2);
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());
    }

    @Test
    public void doGetDataSourceCapabilities_withQueryPassthroughDisabled_returnsResponse()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);
        
        GetDataSourceCapabilitiesResponse response = this.metadataHandler.doGetDataSourceCapabilities(this.blockAllocator, request);
        
        Assert.assertNotNull("Response should not be null", response);
        Assert.assertEquals("Catalog name should match", TEST_CATALOG, response.getCatalogName());
    }

    @Test
    public void constructor_withDatabaseConnectionConfig_usesEmptyJdbcProperties()
    {
        Assert.assertTrue(ClickHouseConstants.JDBC_PROPERTIES.isEmpty());
        Assert.assertFalse(ClickHouseConstants.JDBC_PROPERTIES.containsKey("databaseTerm"));
    }

    @Test
    public void doListTables_withUnlimitedPageSize_returnsAllTables()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{TEST_SCHEMA, TEST_TABLE}, {TEST_SCHEMA, "testTable2"}};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ListTablesResponse listTablesResponse = this.metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        TEST_CATALOG, TEST_SCHEMA, null, UNLIMITED_PAGE_SIZE_VALUE));

        Assert.assertNull(listTablesResponse.getNextToken());
        Assert.assertEquals(2, listTablesResponse.getTables().size());
        Assert.assertArrayEquals(
                new TableName[]{new TableName(TEST_SCHEMA, TEST_TABLE), new TableName(TEST_SCHEMA, "testTable2")},
                listTablesResponse.getTables().toArray());
    }

    @Test
    public void doListTables_whenNoTables_returnsEmptyListAndNullNextToken()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ClickHouseMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        ResultSet resultSet = mockResultSet(new String[]{"TABLE_SCHEM", "TABLE_NAME"}, new Object[][]{}, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ListTablesResponse listTablesResponse = this.metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        TEST_CATALOG, TEST_SCHEMA, null, 1));

        Assert.assertTrue(listTablesResponse.getTables().isEmpty());
        Assert.assertNull(listTablesResponse.getNextToken());
    }

    @Test(expected = NumberFormatException.class)
    public void doListTables_withInvalidNextToken_throwsNumberFormatException()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        this.metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        TEST_CATALOG, TEST_SCHEMA, "not-a-number", 1));
    }

    @Test(expected = SQLException.class)
    public void doListTables_whenPrepareStatementThrowsSQLException_throwsSQLException()
            throws Exception
    {
        Mockito.when(this.connection.prepareStatement(ClickHouseMetadataHandler.LIST_PAGINATED_TABLES_QUERY))
                .thenThrow(new SQLException("Failed to prepare statement"));
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        this.metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, TEST_QUERY_ID,
                        TEST_CATALOG, TEST_SCHEMA, null, 1));
    }

    @Test
    public void doGetSplits_withContinuationToken_returnsNoRemainingSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.metadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(
                this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName, constraints, partitionSchema, partitionCols);
        GetTableLayoutResponse getTableLayoutResponse = this.metadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(
                this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName,
                getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
        GetSplitsResponse getSplitsResponse = this.metadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        Assert.assertTrue(getSplitsResponse.getSplits().isEmpty());
        Assert.assertNull(getSplitsResponse.getContinuationToken());
    }

    @Test(expected = SQLException.class)
    public void doGetTable_whenGetColumnsThrowsSQLException_throwsSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        Mockito.when(this.connection.getMetaData().getColumns(
                TEST_CATALOG, inputTableName.getSchemaName(), inputTableName.getTableName(), null))
                .thenThrow(new SQLException("Failed to read columns"));
        Mockito.when(this.connection.getCatalog()).thenReturn(TEST_CATALOG);

        this.metadataHandler.doGetTable(
                new BlockAllocatorImpl(),
                new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }
}