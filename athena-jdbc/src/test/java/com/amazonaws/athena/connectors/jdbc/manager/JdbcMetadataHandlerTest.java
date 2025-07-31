/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.SCHEMA_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;

public class JdbcMetadataHandlerTest
        extends TestBase
{
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField("testPartitionCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();

    private JdbcMetadataHandler jdbcMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private FederatedIdentity federatedIdentity;
    private Connection connection;
    private BlockAllocator blockAllocator;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private ResultSet resultSetName;

    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = mock(JdbcConnectionFactory.class);
        this.connection = mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "fakedatabase",
                "fakedatabase://jdbc:fakedatabase://hostname/${testSecret}", "testSecret");
        this.jdbcMetadataHandler = new JdbcMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of())
        {
            @Override
            public Schema getPartitionSchema(final String catalogName)
            {
                return PARTITION_SCHEMA;
            }

            @Override
            public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
            {
            }

            @Override
            public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
            {
                return null;
            }
        };
        this.federatedIdentity = mock(FederatedIdentity.class);
        this.blockAllocator = mock(BlockAllocator.class);
        String[] columnNames = new String[] {"TABLE_SCHEM", "TABLE_NAME"};
        String[][] tableNameValues = new String[][]{new String[] {"testSchema", "testTable"}};
        this.resultSetName = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        Mockito.when(this.connection.getMetaData().getTables(Mockito.eq(connection.getCatalog()), any(), Mockito.eq(null), Mockito.eq(new String[] {"TABLE", "VIEW", "EXTERNAL TABLE"}))).thenReturn(this.resultSetName);
    }

    @Test
    public void getJdbcConnectionFactory()
    {
        Assert.assertEquals(this.jdbcConnectionFactory, this.jdbcMetadataHandler.getJdbcConnectionFactory());
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        String[] schema = {"TABLE_SCHEM"};
        Object[][] values = {{"testDB"}, {"testdb2"}, {"information_schema"}};
        String[] expected = {"testDB", "testdb2"};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Mockito.when(connection.getMetaData().getSchemas()).thenReturn(resultSet);
        ListSchemasResponse listSchemasResponse = this.jdbcMetadataHandler.doListSchemaNames(this.blockAllocator, new ListSchemasRequest(this.federatedIdentity, "testQueryId", "testCatalog"));
        Assert.assertArrayEquals(expected, listSchemasResponse.getSchemas().toArray());
    }

    @Test
    public void doListTables()
            throws Exception
    {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testtable2"}};
        TableName[] expected = {new TableName("testSchema", "testTable"), new TableName("testSchema", "testtable2")};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);
        ListTablesResponse listTablesResponse = this.jdbcMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, UNLIMITED_PAGE_SIZE_VALUE));
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());
    }

    @Test
    public void doListTablesFakePaginationSinglePage()
            throws Exception {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testtable2"}, {"testSchema", "testtable3"}, {"testSchema", "testtable4"}, {"testSchema", "testtable5"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);

        // Test: null token with pageSize 1 (single item with next token at 1)
        ListTablesResponse listTablesResponse = this.jdbcMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, 1));

        TableName[] expected = {new TableName("testSchema", "testTable")};
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());
        Assert.assertEquals("1", listTablesResponse.getNextToken());
    }

    @Test
    public void doListTablesFakePaginationMultiPage()
            throws Exception {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testTable2"}, {"testSchema", "testTable3"}, {"testSchema", "testTable4"}, {"testSchema", "testTable5"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);

        // Test: null token with pageSize 5 (all items)
        ListTablesResponse listTablesResponse = this.jdbcMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, 5));

        TableName[] expected = {new TableName("testSchema", "testTable"), new TableName("testSchema", "testTable2"), new TableName("testSchema", "testTable3"), new TableName("testSchema", "testTable4"), new TableName("testSchema", "testTable5")};
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());
        Assert.assertEquals(null, listTablesResponse.getNextToken());
    }

    @Test
    public void doListTablesFakePaginationUnlimitedPageSize()
            throws Exception {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testTable2"}, {"testSchema", "testTable3"}, {"testSchema", "testTable4"}, {"testSchema", "testTable5"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);

        // Test: startToken at index "1" with pageSize unlimited (all items)
        ListTablesResponse listTablesResponse = this.jdbcMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "1", UNLIMITED_PAGE_SIZE_VALUE));

        TableName[] expected = {new TableName("testSchema", "testTable2"), new TableName("testSchema", "testTable3"), new TableName("testSchema", "testTable4"), new TableName("testSchema", "testTable5")};
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());
        Assert.assertEquals(null, listTablesResponse.getNextToken());
    }

    @Test(expected = AthenaConnectorException.class)
    public void doListTablesNumberFormatException()
            throws Exception {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testTable2"}, {"testSchema", "testTable3"}, {"testSchema", "testTable4"}, {"testSchema", "testTable5"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);

        // Test: startToken is not a valid number with pageSize unlimited (all items)
        this.jdbcMetadataHandler.doListTables(this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                "testCatalog", "testSchema", "not_a_valid_number", UNLIMITED_PAGE_SIZE_VALUE));
    }

    @Test(expected = AthenaConnectorException.class)
    public void doListTablesInvalidArgumentExceptionNegativeToken()
            throws Exception {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testTable2"}, {"testSchema", "testTable3"}, {"testSchema", "testTable4"}, {"testSchema", "testTable5"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);

        // Test: startToken is not a valid number with pageSize unlimited. (all items)
        this.jdbcMetadataHandler.doListTables(this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                "testCatalog", "testSchema", "-1", UNLIMITED_PAGE_SIZE_VALUE));
    }

    @Test(expected = AthenaConnectorException.class)
    public void doListTablesInvalidArgumentExceptionNegativePageSize()
            throws Exception {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testTable2"}, {"testSchema", "testTable3"}, {"testSchema", "testTable4"}, {"testSchema", "testTable5"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);

        // Test: pageSize is negative and invalid.
        this.jdbcMetadataHandler.doListTables(this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                "testCatalog", "testSchema", "0", -3));
    }

    @Test
    public void doListTablesEscaped()
            throws Exception
    {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"test_Schema", "testTable"}, {"test_Schema", "testtable2"}};
        TableName[] expected = {new TableName("test_Schema", "testTable"), new TableName("test_Schema", "testtable2")};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Mockito.when(connection.getMetaData().getTables("testCatalog", "test\\_Schema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenReturn("\\");
        ListTablesResponse listTablesResponse = this.jdbcMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "test_Schema", null, UNLIMITED_PAGE_SIZE_VALUE));
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void doListTablesEscapedException()
            throws Exception
    {
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenReturn("_");
        this.jdbcMetadataHandler.doListTables(this.blockAllocator, new ListTablesRequest(this.federatedIdentity,
                "testQueryId", "testCatalog", "test_Schema", null, UNLIMITED_PAGE_SIZE_VALUE));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "TYPE_NAME"};
        Object[][] values = {
                {Types.INTEGER, 12, "testCol1", 0, 0, "_int4"},
                {Types.VARCHAR, 25, "testCol2", 0, 0, "VARCHAR"},
                {Types.TIMESTAMP, 93, "testCol3", 0, 0, "_timestamp"},
                {Types.TIMESTAMP_WITH_TIMEZONE, 93, "testCol4", 0, 0, "_timestamp"},
                {Types.ARRAY, 0, "testCol5", 0, 0, "_array"}
        };
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addListField("testCol5", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName("testSchema", "testTable");
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);

        GetTableResponse getTableResponse = this.jdbcMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doGetQueryPassthroughSchema()
            throws Exception
    {
        String query = "select testCol1 from testTable";

        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "TYPE_NAME"};
        Object[][] values = {
                {Types.INTEGER, 12, "testCol1", 0, 0, "_int4"}
        };
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName("testSchema", "testTable");

        ResultSetMetaData resultSetMetadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(resultSetMetadata.getColumnCount()).thenReturn(values.length);
        Mockito.when(resultSetMetadata.getColumnName(1)).thenReturn((String) values[0][2]);
        Mockito.when(resultSetMetadata.getColumnLabel(1)).thenReturn((String) values[0][2]);
        Mockito.when(resultSetMetadata.getPrecision(1)).thenReturn((Integer) values[0][3]);
        Mockito.when(resultSetMetadata.getColumnDisplaySize(1)).thenReturn((Integer) values[0][4]);
        Mockito.when(resultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);

        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        Mockito.when(preparedStatement.getMetaData()).thenReturn(resultSetMetadata);
        Mockito.when(connection.prepareStatement(query)).thenReturn(preparedStatement);

        Map<String, String> queryPassthroughParameters = Map.of(
                SCHEMA_FUNCTION_NAME, "system.query",
                ENABLE_QUERY_PASSTHROUGH, "true",
                NAME, "query",
                SCHEMA_NAME, "system",
                QUERY, query);

        GetTableResponse getTableResponse = this.jdbcMetadataHandler.doGetQueryPassthroughSchema(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, queryPassthroughParameters));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }


    @Test
    public void doGetTableCaseInsensitive()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        Object[][] values1 = {{"testSchema", "testTable"}, {"testSchema", "testTable2"}};

        setupMocksDoGetTableCaseInsensitive(inputTableName, values1, "testTable");

        GetTableResponse getTableResponse = this.jdbcMetadataHandler.doGetTable(this.blockAllocator,
                new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals("testTable", getTableResponse.getTableName().getTableName());
    }

    @Test
    public void doGetTableNoColumns()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");

        GetTableResponse getTableResponse = this.jdbcMetadataHandler.doGetTable(this.blockAllocator,
                new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals("testTable", getTableResponse.getTableName().getTableName());
        Assert.assertTrue(getTableResponse.getSchema().getFields().isEmpty());
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), Mockito.isNull()))
                .thenThrow(new SQLException());
        this.jdbcMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doListSchemaNamesSQLException()
            throws Exception
    {
        Mockito.when(this.connection.getMetaData().getSchemas()).thenThrow(new SQLException());
        this.jdbcMetadataHandler.doListSchemaNames(this.blockAllocator, new ListSchemasRequest(this.federatedIdentity, "testQueryId", "testCatalog"));
    }

    @Test(expected = SQLException.class)
    public void doListTablesSQLException()
            throws Exception
    {
        Mockito.when(this.connection.getMetaData().getTables(nullable(String.class), nullable(String.class), Mockito.isNull(), any())).thenThrow(new SQLException());
        this.jdbcMetadataHandler.doListTables(this.blockAllocator, new ListTablesRequest(this.federatedIdentity,
                "testQueryId", "testCatalog", "testSchema", null, UNLIMITED_PAGE_SIZE_VALUE));
    }

    private void setupMocksDoGetTableCaseInsensitive(TableName inputTableName, Object[][] resultSetRows,
                                                     String expectedTableName) throws Exception
    {
        // mock first call to getSchema() to simulate no table found for original lowercase table name
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{Types.INTEGER, 12, "testCol1", 0, 0}};

        // mock second call to getSchema()
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(),
                        expectedTableName, null))
                .thenReturn(resultSet);
    }
}
