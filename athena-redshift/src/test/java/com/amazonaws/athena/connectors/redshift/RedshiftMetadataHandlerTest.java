/*-
 * #%L
 * athena-redshift
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
package com.amazonaws.athena.connectors.redshift;

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
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;


public class RedshiftMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftMetadataHandlerTest.class);

    private String FILTER_PUSHDOWN = DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.getOptimization();
    private String LIMIT_PUSHDOWN = DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.getOptimization();
    private String COMPLEX_EXPRESSION_PUSHDOWN = DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.getOptimization();
    private String TOP_N_PUSHDOWN = DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.getOptimization();
    private String QUERY_PASSTHROUGH = "supports_query_passthrough";

    private String SORTED_RANGE_SET = FilterPushdownSubType.SORTED_RANGE_SET.getSubType();
    private String NULLABLE_COMPARISON = FilterPushdownSubType.NULLABLE_COMPARISON.getSubType();
    private String INTEGER_CONSTANT = LimitPushdownSubType.INTEGER_CONSTANT.getSubType();
    private String SUPPORTED_FUNCTIONS = ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES.getSubType();
    private String SUPPORTS_ORDER_BY = TopNPushdownSubType.SUPPORTS_ORDER_BY.getSubType();

    private String CATALOG_NAME = "testCatalog";
    private int FILTER_PUSHDOWN_SIZE = 2;
    private int LIMIT_PUSHDOWN_SIZE = 1;
    private int COMPLEX_EXPRESSION_SIZE = 1;
    private int TOP_N_PUSHDOWN_SIZE = 1;
    private int QUERY_PASSTHROUGH_SIZE = 1;

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "redshift",
            "redshift://jdbc:redshift://hostname/user=A&password=B");
    private RedshiftMetadataHandler redshiftMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.athena = Mockito.mock(AthenaClient.class);
        this.redshiftMetadataHandler = new RedshiftMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())
                        .addField(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.redshiftMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doListPaginatedTables()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(RedshiftMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}};
        TableName[] expected = {new TableName("testSchema", "testTable")};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // insensitive search schema
        String sql = "SELECT nspname FROM pg_namespace WHERE lower(nspname) = ?";
        PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
        preparedSchemaStatement.setString(1, "testSchema");

        String[] columnNames = new String[] {"nspname"};
        String[][] tableNameValues = new String[][]{new String[] {"testSchema"}};
        ResultSet caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult, caseInsensitiveSchemaResult);

        ListTablesResponse listTablesResponse = this.redshiftMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, 1));
        Assert.assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(RedshiftMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        Object[][] nextValues = {{"testSchema", "testTable2"}};
        TableName[] nextExpected = {new TableName("testSchema", "testTable2")};
        ResultSet nextResultSet = mockResultSet(schema, nextValues, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(nextResultSet);

        ResultSet caseInsensitiveSchemaResult2 = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult2);

        listTablesResponse = this.redshiftMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "1", 2));
        Assert.assertNull(listTablesResponse.getNextToken());
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
    }


    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.redshiftMetadataHandler.getPartitionSchema("testCatalogName");
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

        GetTableLayoutResponse getTableLayoutResponse = this.redshiftMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

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
        Schema partitionSchema = this.redshiftMetadataHandler.getPartitionSchema("testCatalogName");
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

        GetTableLayoutResponse getTableLayoutResponse = this.redshiftMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

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
        Schema partitionSchema = this.redshiftMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        RedshiftMetadataHandler redshiftMetadataHandler = new RedshiftMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        redshiftMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.redshiftMetadataHandler.getPartitionSchema("testCatalogName");
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

        GetTableLayoutResponse getTableLayoutResponse = this.redshiftMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.redshiftMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

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
        Schema partitionSchema = this.redshiftMetadataHandler.getPartitionSchema("testCatalogName");
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

        GetTableLayoutResponse getTableLayoutResponse = this.redshiftMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
        GetSplitsResponse getSplitsResponse = this.redshiftMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

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
        redshiftMetadataHandler.getPartitionSchema("testCatalog").getFields()
                .forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        String sql = "SELECT nspname FROM pg_namespace WHERE lower(nspname) = ?";
        PreparedStatement preparedSchemaStatement = connection.prepareStatement(sql);
        preparedSchemaStatement.setString(1, "testSchema");

        String[] columnNames = new String[] {"nspname"};
        String[][] tableNameValues = new String[][]{new String[] {"testSchema"}};
        ResultSet caseInsensitiveSchemaResult = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        Mockito.when(preparedSchemaStatement.executeQuery()).thenReturn(caseInsensitiveSchemaResult, caseInsensitiveSchemaResult);


        TableName inputTableName = new TableName("testSchema", "testtable");
        columnNames = new String[] {"table_name"};
        tableNameValues = new String[][]{new String[] {"testTable"}};
        ResultSet resultSetName = mockResultSet(columnNames, tableNameValues, new AtomicInteger(-1));
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND lower(table_name) = ?";
        PreparedStatement preparedStatement = this.connection.prepareStatement(sql);
        preparedStatement.setString(1, "testSchema");
        preparedStatement.setString(2, "testtable");
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSetName);
        String resolvedTableName = "testTable";
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), resolvedTableName, null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = this.redshiftMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        logger.info("Schema: {}", getTableResponse.getSchema());

        TableName expectedTableName = new TableName("testSchema", "testTable");
        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(expectedTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());

        logger.info("doGetTableWithArrayColumns - exit");
    }

    @Test
    public void doGetDataSourceCapabilitiesWithoutQueryPassthrough()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, "testQueryId", CATALOG_NAME);

        JdbcQueryPassthrough mockQueryPassthrough = Mockito.mock(JdbcQueryPassthrough.class);
        Mockito.doNothing().when(mockQueryPassthrough).addQueryPassthroughCapabilityIfEnabled(any(), eq(ImmutableMap.of()));

        RedshiftMetadataHandler handler = new RedshiftMetadataHandler(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, ImmutableMap.of());

        java.lang.reflect.Field field = JdbcMetadataHandler.class.getDeclaredField("jdbcQueryPassthrough");
        field.setAccessible(true);
        field.set(handler, mockQueryPassthrough);

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(blockAllocator, request);

        verifyCommonCapabilities(response, true);
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        // Verify no query passthrough capability
        Assert.assertNull(capabilities.get(QUERY_PASSTHROUGH));
    }

    @Test
    public void doGetDataSourceCapabilitiesWithQueryPassthrough()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, "testQueryId", CATALOG_NAME);

        JdbcQueryPassthrough mockQueryPassthrough = Mockito.mock(JdbcQueryPassthrough.class);
        Mockito.doAnswer(invocation -> {
            ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = invocation.getArgument(0);
            capabilities.put(QUERY_PASSTHROUGH, Collections.singletonList(new OptimizationSubType(QUERY_PASSTHROUGH, Collections.emptyList())));
            return null;
        }).when(mockQueryPassthrough).addQueryPassthroughCapabilityIfEnabled(any(), eq(ImmutableMap.of("query_passthrough", "true")));

        RedshiftMetadataHandler handler = new RedshiftMetadataHandler(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, ImmutableMap.of("query_passthrough", "true"));
        java.lang.reflect.Field field = JdbcMetadataHandler.class.getDeclaredField("jdbcQueryPassthrough");
        field.setAccessible(true);
        field.set(handler, mockQueryPassthrough);

        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(blockAllocator, request);

        verifyCommonCapabilities(response, true);
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        // Verify query passthrough capability
        List<OptimizationSubType> passthrough = capabilities.get(QUERY_PASSTHROUGH);
        Assert.assertNotNull(passthrough);
        Assert.assertEquals(QUERY_PASSTHROUGH_SIZE, passthrough.size());
        Assert.assertEquals(QUERY_PASSTHROUGH, passthrough.get(0).getSubType());
    }

    private void verifyCommonCapabilities(GetDataSourceCapabilitiesResponse response, boolean expectNonEmptyComplexProperties)
    {
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        logger.info("Capabilities: {}", capabilities);

        Assert.assertEquals(CATALOG_NAME, response.getCatalogName());

        // Verify filter pushdown capabilities
        List<OptimizationSubType> filterPushdown = capabilities.get(FILTER_PUSHDOWN);
        if (filterPushdown == null) {
            logger.error("Filter pushdown capability is missing");
            Assert.fail("Expected " + FILTER_PUSHDOWN + " capability to be present");
        }
        Assert.assertEquals(FILTER_PUSHDOWN_SIZE, filterPushdown.size());
        Assert.assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals(SORTED_RANGE_SET)));
        Assert.assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals(NULLABLE_COMPARISON)));

        // Verify limit pushdown capabilities
        List<OptimizationSubType> limitPushdown = capabilities.get(LIMIT_PUSHDOWN);
        if (limitPushdown == null) {
            logger.error("Limit pushdown capability is missing");
            Assert.fail("Expected " + LIMIT_PUSHDOWN + " capability to be present");
        }
        Assert.assertEquals(LIMIT_PUSHDOWN_SIZE, limitPushdown.size());
        Assert.assertTrue(limitPushdown.stream().anyMatch(subType -> subType.getSubType().equals(INTEGER_CONSTANT)));

        // Verify complex expression pushdown capabilities
        List<OptimizationSubType> complexExpressionPushdown = capabilities.get(COMPLEX_EXPRESSION_PUSHDOWN);
        if (complexExpressionPushdown == null) {
            logger.error("Complex expression pushdown capability is missing");
            Assert.fail("Expected " + COMPLEX_EXPRESSION_PUSHDOWN + " capability to be present");
        }
        Assert.assertEquals(COMPLEX_EXPRESSION_SIZE, complexExpressionPushdown.size());
        Assert.assertTrue(complexExpressionPushdown.stream().anyMatch(subType ->
                subType.getSubType().equals(SUPPORTED_FUNCTIONS) &&
                        (expectNonEmptyComplexProperties ? subType.getProperties().size() > 0 : subType.getProperties().isEmpty())));

        // Verify top N pushdown capabilities
        List<OptimizationSubType> topNPushdown = capabilities.get(TOP_N_PUSHDOWN);
        if (topNPushdown == null) {
            logger.error("Top N pushdown capability is missing");
            Assert.fail("Expected " + TOP_N_PUSHDOWN + " capability to be present");
        }
        Assert.assertEquals(TOP_N_PUSHDOWN_SIZE, topNPushdown.size());
        Assert.assertTrue(topNPushdown.stream().anyMatch(subType -> subType.getSubType().equals(SUPPORTS_ORDER_BY)));
    }
}