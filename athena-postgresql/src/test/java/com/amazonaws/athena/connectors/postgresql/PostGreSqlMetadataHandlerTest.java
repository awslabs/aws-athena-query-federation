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
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

public class PostGreSqlMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(PostGreSqlMetadataHandlerTest.class);

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "postgres",
            "postgres://jdbc:postgresql://hostname/user=A&password=B");
    private PostGreSqlMetadataHandler postGreSqlMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;

    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        this.postGreSqlMetadataHandler = new PostGreSqlMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
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

        listTablesResponse = this.postGreSqlMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "1", 1));
        Assert.assertEquals("2", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
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
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(connection);
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
        final String expectedQuery = String.format(PostGreSqlMetadataHandler.GET_PARTITIONS_QUERY, tableName.getTableName(), tableName.getSchemaName());
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
                .addListField("decimal_array", new ArrowType.Decimal(38, 2))
                .addListField("string_array", new ArrowType.Utf8())
                .addListField("uuid_array", new ArrowType.Utf8());
        postGreSqlMetadataHandler.getPartitionSchema("testCatalog").getFields()
                .forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        ResultSet caseInsensitiveSchemaResult = Mockito.mock(ResultSet.class);
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE (schema_name = ? or lower(schema_name) = ?)";
        PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
        preparedSchemaStatement.setString(1, "testschema");
        preparedSchemaStatement.setString(2, "testschema");

        String[] columnNames = new String[] {"schema_name"};
        String[][] tableNameValues = new String[][]{new String[] {"testSchema"}};
        caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));

        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult);

        TableName inputTableName = new TableName("testSchema", "testtable");
        columnNames = new String[] {"table_name"};
        tableNameValues = new String[][]{new String[] {"testTable"}};
        ResultSet resultSetName = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        sql = "SELECT table_name FROM information_schema.tables WHERE (table_name = ? or lower(table_name) = ?) AND table_schema = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, "testtable");
        preparedStatement.setString(2, "testtable");
        preparedStatement.setString(3, "testSchema");
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
       String sql = "SELECT schema_name FROM information_schema.schemata WHERE (schema_name = ? or lower(schema_name) = ?)";
       PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
       preparedSchemaStatement.setString(1, "testschema");
       preparedSchemaStatement.setString(2, "testschema");

       String[] columnNames = new String[] {"schema_name"};
       String[][] tableNameValues = new String[][]{new String[] {"testSchema"}};
       caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));

       Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult);

       // Simulates table look up in information_schema.tables and returns empty result set, because materialized views are stored separately
       ResultSet caseInsensitiveTableResult = Mockito.mock(ResultSet.class);
       sql = "SELECT table_name FROM information_schema.tables WHERE (table_name = ? or lower(table_name) = ?) AND table_schema = ?";
       PreparedStatement preparedStatement = connection.prepareStatement(sql);
       preparedStatement.setString(1, "testmatview");
       preparedStatement.setString(2, "testmatview");
       preparedStatement.setString(3, "testSchema");

       Mockito.when(preparedStatement.executeQuery()).thenReturn(caseInsensitiveTableResult);
       Mockito.when(caseInsensitiveTableResult.next()).thenReturn(false);

       // Simulates Materialized View look up in pg_catalog.pgmatviews system table
       sql = "select matviewname as \"TABLE_NAME\" from pg_catalog.pg_matviews mv where (matviewname = ? or lower(matviewname) = ?) and schemaname = ?";
       preparedStatement = connection.prepareStatement(sql);
       preparedStatement.setString(1, "testmatview");
       preparedStatement.setString(2, "testmatview");
       preparedStatement.setString(3, "testSchema");

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
}
