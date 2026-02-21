/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.sqlserver.resolver.SQLServerJDBCCaseResolver;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import com.microsoft.sqlserver.jdbc.SQLServerException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.PARTITION_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;

public class SqlServerMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SqlServerMetadataHandlerTest.class);
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_CATALOG = "testCatalogName";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SqlServerConstants.NAME,
    		  "sqlserver://jdbc:sqlserver://hostname;databaseName=fakedatabase");
    private SqlServerMetadataHandler sqlServerMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator allocator;

    @Before
    public void setup()
            throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        logger.info(" this.connection.." + this.connection);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"user\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.sqlServerMetadataHandler = new SqlServerMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new SQLServerJDBCCaseResolver(SqlServerConstants.NAME));
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.allocator = new BlockAllocatorImpl();
    }

    @Test
    public void getPartitionSchema()
    {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.sqlServerMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = createEmptyConstraint();
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement rowCountPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.ROW_COUNT_QUERY)).thenReturn(rowCountPreparedStatement);
        ResultSet rowCountResultSet = mockResultSet(new String[] {"ROW_COUNT"}, new int[] {Types.INTEGER}, new Object[][] {{2}}, new AtomicInteger(-1));
        Mockito.when(rowCountPreparedStatement.executeQuery()).thenReturn(rowCountResultSet);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {PARTITION_NUMBER};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"2"}, {"3"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PreparedStatement partFuncPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITION_DETAILS_QUERY)).thenReturn(partFuncPreparedStatement);
        ResultSet partFuncResultSet = mockResultSet(new String[] {"PARTITION FUNCTION", "PARTITIONING COLUMN"}, new int[] {Types.VARCHAR, Types.VARCHAR}, new Object[][] {{"pf", "pc"}}, new AtomicInteger(-1));
        Mockito.when(partFuncPreparedStatement.executeQuery()).thenReturn(partFuncResultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        GetTableLayoutResponse getTableLayoutResponse = this.sqlServerMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(Arrays.asList("[partition_number : 1:::pf:::pc]", "[partition_number : 2:::pf:::pc]", "[partition_number : 3:::pf:::pc]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = createEmptyConstraint();
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {PARTITION_NUMBER};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PreparedStatement rowCountPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.ROW_COUNT_QUERY)).thenReturn(rowCountPreparedStatement);
        ResultSet rowCountResultSet = mockResultSet(new String[] {"ROW_COUNT"}, new int[] {Types.INTEGER}, new Object[][] {{0}}, new AtomicInteger(-1));
        Mockito.when(rowCountPreparedStatement.executeQuery()).thenReturn(rowCountResultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        GetTableLayoutResponse getTableLayoutResponse = this.sqlServerMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(List.of("[partition_number : 0]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = createEmptyConstraint();
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        SqlServerMetadataHandler sqlServerMetadataHandler = new SqlServerMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new SQLServerJDBCCaseResolver(SqlServerConstants.NAME));

        sqlServerMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

     @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = createEmptyConstraint();
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement viewCheckPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.VIEW_CHECK_QUERY)).thenReturn(viewCheckPreparedStatement);
        ResultSet viewCheckqueryResultSet = mockResultSet(new String[] {"TYPE_DESC"}, new int[] {Types.VARCHAR}, new Object[][] {{"TABLE"}}, new AtomicInteger(-1));
        Mockito.when(viewCheckPreparedStatement.executeQuery()).thenReturn(viewCheckqueryResultSet);

        PreparedStatement rowCountPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.ROW_COUNT_QUERY)).thenReturn(rowCountPreparedStatement);
        ResultSet rowCountResultSet = mockResultSet(new String[] {"ROW_COUNT"}, new int[] {Types.INTEGER}, new Object[][] {{2}}, new AtomicInteger(-1));
        Mockito.when(rowCountPreparedStatement.executeQuery()).thenReturn(rowCountResultSet);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {PARTITION_NUMBER};
        int[] types = {Types.INTEGER};
        Object[][] values = {{2}, {3}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PreparedStatement partFuncPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITION_DETAILS_QUERY)).thenReturn(partFuncPreparedStatement);
        ResultSet partFuncResultSet = mockResultSet(new String[] {"PARTITION FUNCTION", "PARTITIONING COLUMN"}, new int[] {Types.VARCHAR, Types.VARCHAR}, new Object[][] {{"pf", "pc"}}, new AtomicInteger(-1));
        Mockito.when(partFuncPreparedStatement.executeQuery()).thenReturn(partFuncResultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.sqlServerMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.sqlServerMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "1",
                "PARTITIONING_COLUMN", "pc",
                "PARTITION_FUNCTION", "pf"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "2",
                "PARTITIONING_COLUMN", "pc",
                "PARTITION_FUNCTION", "pf"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "3",
                "PARTITIONING_COLUMN", "pc",
                "PARTITION_FUNCTION", "pf"));
        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsWithNoPartition()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = createEmptyConstraint();
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement viewCheckPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.VIEW_CHECK_QUERY)).thenReturn(viewCheckPreparedStatement);
        ResultSet viewCheckqueryResultSet = mockResultSet(new String[] {"TYPE_DESC"}, new int[] {Types.VARCHAR}, new Object[][] {{"TABLE"}}, new AtomicInteger(-1));
        Mockito.when(viewCheckPreparedStatement.executeQuery()).thenReturn(viewCheckqueryResultSet);

        PreparedStatement rowCountPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.ROW_COUNT_QUERY)).thenReturn(rowCountPreparedStatement);
        ResultSet rowCountResultSet = mockResultSet(new String[] {"ROW_COUNT"}, new int[] {Types.INTEGER}, new Object[][] {{0}}, new AtomicInteger(-1));
        Mockito.when(rowCountPreparedStatement.executeQuery()).thenReturn(rowCountResultSet);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {PARTITION_NUMBER};
        int[] types = {Types.INTEGER};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.sqlServerMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.sqlServerMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(PARTITION_NUMBER, "0"));
        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsContinuation()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = createEmptyConstraint();
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement rowCountPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.ROW_COUNT_QUERY)).thenReturn(rowCountPreparedStatement);
        ResultSet rowCountResultSet = mockResultSet(new String[] {"ROW_COUNT"}, new int[] {Types.INTEGER}, new Object[][] {{2}}, new AtomicInteger(-1));
        Mockito.when(rowCountPreparedStatement.executeQuery()).thenReturn(rowCountResultSet);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {PARTITION_NUMBER};
        int[] types = {Types.INTEGER};
        Object[][] values = {{2}, {3}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        PreparedStatement partFuncPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.GET_PARTITION_DETAILS_QUERY)).thenReturn(partFuncPreparedStatement);
        ResultSet partFuncResultSet = mockResultSet(new String[] {"PARTITION FUNCTION", "PARTITIONING COLUMN"}, new int[] {Types.VARCHAR, Types.VARCHAR}, new Object[][] {{"pf", "pc"}}, new AtomicInteger(-1));
        Mockito.when(partFuncPreparedStatement.executeQuery()).thenReturn(partFuncResultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.sqlServerMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "2");
        GetSplitsResponse getSplitsResponse = this.sqlServerMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "3",
                "PARTITIONING_COLUMN", "pc",
                "PARTITION_FUNCTION", "pf"));
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doListPaginatedTables()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}};
        TableName[] expected = {new TableName("testSchema", "testTable")};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ListTablesResponse listTablesResponse = this.sqlServerMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, 1));
        assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(sqlServerMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        Object[][] nextValues = {{"testSchema", "testTable2"}};
        TableName[] nextExpected = {new TableName("testSchema", "testTable2")};
        ResultSet nextResultSet = mockResultSet(schema, nextValues, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(nextResultSet);

        listTablesResponse = this.sqlServerMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "1", 1));
        assertEquals("2", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
    } 

    @Test
    public void doGetTable()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{Types.INTEGER, 12, "testCol1", 0, 0}, {Types.VARCHAR, 25, "testCol2", 0, 0},
                {Types.TIMESTAMP, 93, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, "testCol4", 0, 0}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        GetTableResponse getTableResponse = this.sqlServerMetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
        assertEquals(expected, getTableResponse.getSchema());
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        ListSchemasRequest listSchemasRequest = Mockito.mock(ListSchemasRequest.class);
        Mockito.when(listSchemasRequest.getCatalogName()).thenReturn("fakedatabase");
        assertEquals(new ListSchemasResponse("schemas", Collections.emptyList()).toString(),
                sqlServerMetadataHandler.doListSchemaNames(this.allocator, listSchemasRequest).toString());
    }

    @Test
    public void doGetTable_SqlServerSpecificTypes_ReturnsMappedArrowTypes() throws Exception {
            BlockAllocator blockAllocator = new BlockAllocatorImpl();
            String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
            Object[][] values = {
                    {Types.BIT, 1, "col_bit", 0, 0},
                    {Types.TINYINT, 3, "col_tinyint", 0, 0},
                    {Types.NUMERIC, 10, "col_numeric", 2, 10},
                    {Types.DOUBLE, 15, "col_smallmoney", 2, 10}, // simulate SMALLMONEY
                    {Types.DATE, 0, "col_date", 0, 0},
                    {Types.TIMESTAMP, 0, "col_datetime", 0, 0}, // simulate DATETIME
                    {Types.TIMESTAMP, 0, "col_datetime2", 0, 0}, // simulate DATETIME2
                    {Types.TIMESTAMP, 0, "col_smalldatetime", 0, 0}, // simulate SMALLDATETIME
                    {Types.TIMESTAMP_WITH_TIMEZONE, 0, "col_datetimeoffset", 0, 0}
            };

            AtomicInteger rowNumber = new AtomicInteger(-1);
            ResultSet resultSet = mockResultSet(schema, values, rowNumber);

            // Mocking sys.columns response (DATA_TYPE column as string names from enum)
            ResultSet columnTypesResultSet = mockResultSet(
                    new String[]{"COLUMN_NAME", "DATA_TYPE"},
                    new Object[][]{
                            {"col_bit", "BIT"},
                            {"col_tinyint", "TINYINT"},
                            {"col_numeric", "NUMERIC"},
                            {"col_smallmoney", "SMALLMONEY"},
                            {"col_date", "DATE"},
                            {"col_datetime", "DATETIME"},
                            {"col_datetime2", "DATETIME2"},
                            {"col_smalldatetime", "SMALLDATETIME"},
                            {"col_datetimeoffset", "DATETIMEOFFSET"}
                    },
                    new AtomicInteger(-1)
            );

            SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_bit", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_tinyint", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_numeric", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_smallmoney", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_date", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_datetime", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_datetime2", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_smalldatetime", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_datetimeoffset", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
            PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
            Schema expected = expectedSchemaBuilder.build();

            TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");

            mockMetadataConnection(resultSet, columnTypesResultSet);

            GetTableResponse getTableResponse = this.sqlServerMetadataHandler.doGetTable(
                    blockAllocator,
                    new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap())
            );

            assertEquals(expected, getTableResponse.getSchema());
    }

    @Test
    public void doGetTable_UnsupportedType_FallsBackToVarchar() throws Exception {
            BlockAllocator blockAllocator = new BlockAllocatorImpl();
            String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
            Object[][] values = {
                    {Types.OTHER, 255, "col_sql_variant", 0, 0}, // simulate unsupported SQL_VARIANT
            };

            AtomicInteger rowNumber = new AtomicInteger(-1);
            ResultSet resultSet = mockResultSet(schema, values, rowNumber);

            // Mocking sys.columns response (DATA_TYPE column as string names from enum)
            ResultSet columnTypesResultSet = mockResultSet(
                    new String[]{"COLUMN_NAME", "DATA_TYPE"},
                    new Object[][]{
                            {"col_sql_variant", "SQL_VARIANT"}
                    },
                    new AtomicInteger(-1)
            );

            SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("col_sql_variant", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
            Schema expected = expectedSchemaBuilder.build();

            TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");

            mockMetadataConnection(resultSet, columnTypesResultSet);

            GetTableResponse getTableResponse = this.sqlServerMetadataHandler.doGetTable(
                    blockAllocator,
                    new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap())
            );

            assertEquals(expected, getTableResponse.getSchema());
    }

    @Test
    public void convertDatasourceTypeToArrow_SqlServerSpecificTypes_ReturnsMappedArrowTypes() throws SQLException {
            ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
            Map<String, String> configOptions = new HashMap<>();
            int precision = 0;

            // Map of SQL Server data type -> expected ArrowType
            Map<String, ArrowType> expectedMappings = new HashMap<>();
            expectedMappings.put("BIT", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType());
            expectedMappings.put("TINYINT", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType());
            expectedMappings.put("NUMERIC", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType());
            expectedMappings.put("SMALLMONEY", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType());
            expectedMappings.put("DATE", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType());
            expectedMappings.put("DATETIME", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());
            expectedMappings.put("DATETIME2", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());
            expectedMappings.put("SMALLDATETIME", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());
            expectedMappings.put("DATETIMEOFFSET", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType());

            int index = 1;
            for (Map.Entry<String, ArrowType> entry : expectedMappings.entrySet()) {
                String sqlServerType = entry.getKey();
                ArrowType expectedArrowType = entry.getValue();

                Mockito.when(metaData.getColumnTypeName(index)).thenReturn(sqlServerType);

                Optional<ArrowType> actual = sqlServerMetadataHandler.convertDatasourceTypeToArrow(index, precision, configOptions, metaData);

                assertEquals(expectedArrowType, actual.get());

                index++;
            }
    }

    @Test
    public void doGetDataSourceCapabilities_DefaultRequest_ReturnsExpectedCapabilities()
    {
        BlockAllocator allocator = new BlockAllocatorImpl();
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(federatedIdentity, "testQueryId", "testCatalog");

        GetDataSourceCapabilitiesResponse response =
                sqlServerMetadataHandler.doGetDataSourceCapabilities(allocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        assertEquals("testCatalog", response.getCatalogName());

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
    public void getPartitions_whenViewDatabaseStateDenied_fallsBackToSinglePartition()
            throws Exception
    {
        runPermissionDeniedFallbackTest(
                "VIEW DATABASE STATE permission denied in database 'test_db'.");
    }

    @Test
    public void getPartitions_whenViewDatabasePerformanceStateDenied_fallsBackToSinglePartition()
            throws Exception
    {
        runPermissionDeniedFallbackTest(
                "VIEW DATABASE PERFORMANCE STATE permission denied in database 'test_db'.");
    }

    @Test(expected = SQLServerException.class)
    public void getPartitions_whenOtherSqlServerException_rethrows()
            throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        mockViewCheckAsTable();
        mockRowCountGreaterThanZero();

        SQLServerException ex = Mockito.mock(SQLServerException.class);
        Mockito.when(ex.getMessage()).thenReturn("Some other SQL error");

        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        Mockito.when(ps.executeQuery()).thenThrow(ex);
        Mockito.when(this.connection.prepareStatement(
                SqlServerMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(ps);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields()
                .stream().map(Field::getName).collect(Collectors.toSet());

        GetTableLayoutRequest req = new GetTableLayoutRequest(
                this.federatedIdentity,
                TEST_QUERY_ID,
                TEST_CATALOG,
                tableName,
                createEmptyConstraint(),
                partitionSchema,
                partitionCols);

        BlockWriter blockWriter = Mockito.mock(BlockWriter.class);
        QueryStatusChecker checker = Mockito.mock(QueryStatusChecker.class);

        this.sqlServerMetadataHandler.getPartitions(blockWriter, req, checker);
    }

    private void runPermissionDeniedFallbackTest(String exceptionMessage)
            throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        mockViewCheckAsTable();
        mockRowCountGreaterThanZero();

        SQLServerException ex = Mockito.mock(SQLServerException.class);
        Mockito.when(ex.getMessage()).thenReturn(exceptionMessage);

        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        Mockito.when(ps.executeQuery()).thenThrow(ex);
        Mockito.when(this.connection.prepareStatement(
                SqlServerMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(ps);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.sqlServerMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields()
                .stream().map(Field::getName).collect(Collectors.toSet());

        GetTableLayoutRequest req = new GetTableLayoutRequest(
                this.federatedIdentity,
                TEST_QUERY_ID,
                TEST_CATALOG,
                tableName,
                createEmptyConstraint(),
                partitionSchema,
                partitionCols);

        BlockWriter blockWriter = Mockito.mock(BlockWriter.class);
        QueryStatusChecker checker = Mockito.mock(QueryStatusChecker.class);

        this.sqlServerMetadataHandler.getPartitions(blockWriter, req, checker);

        ArgumentCaptor<BlockWriter.RowWriter> captor =
                ArgumentCaptor.forClass(BlockWriter.RowWriter.class);
        Mockito.verify(blockWriter).writeRows(captor.capture());

        Block block = Mockito.mock(Block.class);
        captor.getValue().writeRows(block, 0);
        Mockito.verify(block).setValue(eq(PARTITION_NUMBER), eq(0), eq("0"));
    }

    private void mockViewCheckAsTable() throws Exception
    {
        PreparedStatement viewCheckStatement = Mockito.mock(PreparedStatement.class);
        ResultSet viewCheckResultSet = mockResultSet(new String[] {"TYPE_DESC"}, new int[] {Types.VARCHAR}, new Object[][] {{"TABLE"}}, new AtomicInteger(-1));
        Mockito.when(viewCheckStatement.executeQuery()).thenReturn(viewCheckResultSet);
        Mockito.when(this.connection.prepareStatement(SqlServerMetadataHandler.VIEW_CHECK_QUERY)).thenReturn(viewCheckStatement);
    }

    private void mockRowCountGreaterThanZero() throws Exception
    {
        PreparedStatement rowCountStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(SqlServerMetadataHandler.ROW_COUNT_QUERY)).thenReturn(rowCountStatement);
    }

    private void mockMetadataConnection(ResultSet columnsResultSet, ResultSet dataTypeResultSet)
            throws Exception
    {
        Connection metadataConn = Mockito.mock(Connection.class);
        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(stmt.executeQuery()).thenReturn(dataTypeResultSet);
        Mockito.when(metadataConn.prepareStatement(Mockito.anyString())).thenReturn(stmt);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(metadataConn);

        DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(metadata.getSearchStringEscape()).thenReturn("\\");
        Mockito.when(metadata.getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenReturn(columnsResultSet);
        Mockito.when(metadataConn.getMetaData()).thenReturn(metadata);
        Mockito.when(metadataConn.getCatalog()).thenReturn("testCatalog");
    }

    private Constraints createEmptyConstraint()
    {
        return new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );
    }
}
