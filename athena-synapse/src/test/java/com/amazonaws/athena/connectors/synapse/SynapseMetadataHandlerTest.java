/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.synapse.resolver.SynapseJDBCCaseResolver;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.synapse.SynapseMetadataHandler.PARTITION_BOUNDARY_FROM;
import static com.amazonaws.athena.connectors.synapse.SynapseMetadataHandler.PARTITION_BOUNDARY_TO;
import static com.amazonaws.athena.connectors.synapse.SynapseMetadataHandler.PARTITION_COLUMN;
import static com.amazonaws.athena.connectors.synapse.SynapseMetadataHandler.PARTITION_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SynapseMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SynapseMetadataHandlerTest.class);
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "TESTSCHEMA";
    private static final String TEST_TABLE = "TESTTABLE";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_DB_NAME = "fakedatabase";
    private static final String TEST_HOSTNAME = "hostname";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_USER = "testUser";
    private static final String TEST_PASS = "testPassword";
    private static final String TEST_JDBC_URL = String.format("synapse://jdbc:sqlserver://%s;databaseName=%s", TEST_HOSTNAME, TEST_DB_NAME);

    private final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, SynapseConstants.NAME,
            TEST_JDBC_URL);
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private SynapseMetadataHandler synapseMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
            throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.jdbcConnectionFactory = mock(JdbcConnectionFactory.class, RETURNS_DEEP_STUBS);
        this.connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        logger.info(" this.connection..{}", this.connection);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = mock(SecretsManagerClient.class);
        this.athena = mock(AthenaClient.class);
        when(this.secretsManager.getSecretValue(eq(GetSecretValueRequest.builder().secretId(TEST_SECRET).build())))
                .thenReturn(GetSecretValueResponse.builder()
                        .secretString(String.format("{\"user\": \"%s\", \"password\": \"%s\"}", TEST_USER, TEST_PASS))
                        .build());
        this.synapseMetadataHandler = new SynapseMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, 
                this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new SynapseJDBCCaseResolver(SynapseConstants.NAME));
        this.federatedIdentity = mock(FederatedIdentity.class);
    }

    @Test
    public void getPartitionSchema()
    {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.synapseMetadataHandler.getPartitionSchema(TEST_CATALOG));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        String[] columns = {"ROW_COUNT", PARTITION_NUMBER, PARTITION_COLUMN, "PARTITION_BOUNDARY_VALUE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{2, null, null, null}, {0, "1", "id", "100000"}, {0, "2", "id", "300000"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Statement st = mock(Statement.class);
        when(this.connection.createStatement()).thenReturn(st);
        when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(Arrays.asList("[partition_number : 1::: :::100000:::id]", "[partition_number : 2:::100000:::300000:::id]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(new String[]{"ROW_COUNT"}, new int[]{Types.INTEGER}, values, new AtomicInteger(-1));

        Statement st = mock(Statement.class);
        when(this.connection.createStatement()).thenReturn(st);
        when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }

        assertEquals(Collections.singletonList("[partition_number : 0]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = mock(JdbcConnectionFactory.class);
        when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        SynapseMetadataHandler synapseMetadataHandler = new SynapseMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new SynapseJDBCCaseResolver(SynapseConstants.NAME));

        synapseMetadataHandler.doGetTableLayout(mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        String[] columns = {"ROW_COUNT", PARTITION_NUMBER, PARTITION_COLUMN, "PARTITION_BOUNDARY_VALUE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{2, null, null, null}, {0, 1, "id", "0"}, {0, 2, "id", "105"}, {0, 3, "id", "327"}, {0, 4, "id", null}};

        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Statement st = mock(Statement.class);
        when(this.connection.createStatement()).thenReturn(st);
        when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.synapseMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        // TODO: Not sure why this is a set of maps, but I'm not going to change it
        // other than mechanically making it java 8 compatible
        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                PARTITION_BOUNDARY_FROM, " ",
                PARTITION_NUMBER, "1",
                PARTITION_COLUMN, "id",
                PARTITION_BOUNDARY_TO, "0"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_BOUNDARY_FROM, "0",
                PARTITION_NUMBER, "2",
                PARTITION_COLUMN, "id",
                PARTITION_BOUNDARY_TO, "105"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_BOUNDARY_FROM, "105",
                PARTITION_NUMBER, "3",
                PARTITION_COLUMN, "id",
                PARTITION_BOUNDARY_TO, "327"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_BOUNDARY_FROM, "327",
                PARTITION_NUMBER, "4",
                PARTITION_COLUMN, "id",
                PARTITION_BOUNDARY_TO, "null")
        );

        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsWithNoPartition()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(new String[]{"ROW_COUNT"}, new int[]{Types.INTEGER}, values, new AtomicInteger(-1));

        Statement st = mock(Statement.class);
        when(this.connection.createStatement()).thenReturn(st);
        when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.synapseMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

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
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        String[] columns = {"ROW_COUNT", PARTITION_NUMBER, PARTITION_COLUMN, "PARTITION_BOUNDARY_VALUE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{2, null, null, null}, {0, 1, "id", "0"}, {0, 2, "id", "105"}, {0, 3, "id", "327"}, {0, 4, "id", null}};

        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Statement st = mock(Statement.class);
        when(this.connection.createStatement()).thenReturn(st);
        when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "2");
        GetSplitsResponse getSplitsResponse = this.synapseMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                PARTITION_BOUNDARY_FROM, "105",
                PARTITION_NUMBER, "3",
                PARTITION_COLUMN, "id",
                PARTITION_BOUNDARY_TO, "327"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_BOUNDARY_FROM, "327",
                PARTITION_NUMBER, "4",
                PARTITION_COLUMN, "id",
                PARTITION_BOUNDARY_TO, "null"));
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"DATA_TYPE", "COLUMN_NAME", "PRECISION", "SCALE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{Types.INTEGER, "testCol1", 0, 0}, {Types.VARCHAR, "testCol2", 0, 0},
                    {Types.TIMESTAMP, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, "testCol4", 0, 0}};
        ResultSet resultSet = mockResultSet(schema, types, values, new AtomicInteger(-1));

        String[] columns = {"DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "COLUMN_NAME"};
        int[] types2 = {Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR};
        Object[][] values2 = {{Types.INTEGER, 12, 0, "testCol1"}, {Types.VARCHAR, 25, 0, "testCol2"},
                {Types.TIMESTAMP, 93, 0, "testCol3"}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, 0, "testCol4"}};
        ResultSet resultSet2 = mockResultSet(columns, types2, values2, new AtomicInteger(-1));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        PreparedStatement stmt = mock(PreparedStatement.class);
        when(connection.prepareStatement(nullable(String.class))).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(resultSet);

        when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://hostname;databaseName=fakedatabase");

        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        when(connection.getCatalog()).thenReturn(TEST_CATALOG);
        when(connection.getMetaData().getColumns(TEST_CATALOG, inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet2);

        GetTableResponse getTableResponse = this.synapseMetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
        assertEquals(expected, getTableResponse.getSchema());
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());
    }

    @Test
    public void doDataTypeConversion()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"DATA_TYPE", "COLUMN_NAME", "PRECISION", "SCALE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{Types.INTEGER, "testCol1", 0, 0}, {Types.VARCHAR, "testCol2", 0, 0},
                {Types.TIMESTAMP, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, "testCol4", 0, 0}};
        ResultSet resultSet = mockResultSet(schema, types, values, new AtomicInteger(-1));

        String[] columns = {"DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "COLUMN_NAME"};
        int[] types2 = {Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR};
        Object[][] values2 = {{Types.INTEGER, 12, 0, "testCol1"}, {Types.VARCHAR, 25, 0, "testCol2"},
                {Types.TIMESTAMP, 93, 0, "testCol3"}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, 0, "testCol4"}};
        ResultSet resultSet2 = mockResultSet(columns, types2, values2, new AtomicInteger(-1));

        PreparedStatement stmt = mock(PreparedStatement.class);
        when(connection.prepareStatement(nullable(String.class))).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(resultSet);

        when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://hostname-ondemand;databaseName=fakedatabase");

        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        when(connection.getCatalog()).thenReturn(TEST_CATALOG);
        when(connection.getMetaData().getColumns(TEST_CATALOG, inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet2);

        GetTableResponse getTableResponse = this.synapseMetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());
    }

    @Test
    public void doListTables() throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, TEST_SCHEMA, null, 3);

        DatabaseMetaData mockDatabaseMetaData = mock(DatabaseMetaData.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(connection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockDatabaseMetaData.getTables(any(), any(), any(), any())).thenReturn(mockResultSet);

        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(3)).thenReturn("TESTTABLE").thenReturn("testtable").thenReturn("testTABLE");
        when(mockResultSet.getString(2)).thenReturn(TEST_SCHEMA);

        mockStatic(JDBCUtil.class);
        when(JDBCUtil.getSchemaTableName(mockResultSet)).thenReturn(new TableName(TEST_SCHEMA, TEST_TABLE))
                .thenReturn(new TableName(TEST_SCHEMA, "testtable"))
                .thenReturn(new TableName(TEST_SCHEMA, "testTABLE"));

        when(this.jdbcConnectionFactory.getConnection(any())).thenReturn(connection);

        ListTablesResponse listTablesResponse = this.synapseMetadataHandler.doListTables(blockAllocator, listTablesRequest);

        TableName[] expectedTables = {
                new TableName(TEST_SCHEMA, TEST_TABLE),
                new TableName(TEST_SCHEMA, "testTABLE"),
                new TableName(TEST_SCHEMA, "testtable")
        };

        assertEquals(Arrays.toString(expectedTables), listTablesResponse.getTables().toString());
    }

    @Test
    public void testConvertDatasourceTypeToArrow_withSynapseSpecificTypes() throws SQLException {
            ResultSetMetaData metaData = mock(ResultSetMetaData.class);
            Map<String, String> configOptions = new HashMap<>();
            int precision = 0;

            // Map of Synapse data type -> expected ArrowType
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
                String synapseType = entry.getKey();
                ArrowType expectedArrowType = entry.getValue();

                when(metaData.getColumnTypeName(index)).thenReturn(synapseType);

                Optional<ArrowType> actual = synapseMetadataHandler.convertDatasourceTypeToArrow(index, precision, configOptions, metaData);

                assertTrue("Expected ArrowType to be present", actual.isPresent());
                assertEquals(expectedArrowType, actual.get());

                index++;
            }
    }

    @Test
    public void testDoDataTypeConversion_withAzureServerless() throws Exception {
            BlockAllocator blockAllocator = new BlockAllocatorImpl();
            String[] schema = {"DATA_TYPE", "COLUMN_NAME", "PRECISION", "SCALE"};
            int[] types = {Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER};

            Object[][] values = {
                    // VARCHAR group
                    {"varchar", "testCol1", 0, 0}, // varchar
                    {"char", "testCol2", 0, 0}, // char
                    {"binary", "testCol3", 0, 0}, // binary
                    {"nchar", "testCol4", 0, 0}, // nchar
                    {"nvarchar", "testCol5", 0, 0}, // nvarchar
                    {"varbinary", "testCol6", 0, 0}, // varbinary
                    {"time", "testCol7", 0, 0}, // time
                    {"uniqueidentifier", "testCol8", 0, 0}, // uniqueidentifier

                    // Boolean
                    {"bit", "testCol9", 0, 0},

                    // Integer group
                    {"tinyint", "testCol10", 0, 0},
                    {"smallint", "testCol11", 0, 0},
                    {"int", "testCol12", 0, 0},
                    {"bigint", "testCol13", 0, 0},

                    // Decimal
                    {"decimal", "testCol14", 10, 2},
                    {"float", "testCol15", 0, 0}, // float
                    {"float", "testCol16", 0, 0}, // float
                    {"real", "testCol17", 0, 0}, // real

                    // Dates
                    {"date", "testCol18", 0, 0},
                    {"datetime", "testCol19", 0, 0}, // datetime/datetime2
                    {"datetimeoffset", "testCol20", 0, 0} // datetimeoffset
            };

            ResultSet resultSet = mockResultSet(schema, types, values, new AtomicInteger(-1));

            SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol5", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol6", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol7", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol8", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());

            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol9", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType()).build());

            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol10", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol11", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol12", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol13", org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType()).build());

            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol14", new ArrowType.Decimal(10, 2, 256)).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol15", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol16", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol17", org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType()).build());

            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol18", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol19", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol20", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());

            PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
            Schema expected = expectedSchemaBuilder.build();

            PreparedStatement stmt = mock(PreparedStatement.class);
            when(connection.prepareStatement(nullable(String.class))).thenReturn(stmt);
            when(stmt.executeQuery()).thenReturn(resultSet);

            when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://test-ondemand.sql.azuresynapse.net;databaseName=fakedatabase");

            TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
            when(connection.getCatalog()).thenReturn(TEST_CATALOG);

            GetTableResponse getTableResponse = this.synapseMetadataHandler.doGetTable(
                    blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));

            // Compare schemas ignoring order
            assertTrue("Schemas do not match when ignoring order", schemasMatchIgnoringOrder(expected, getTableResponse.getSchema()));
            assertEquals(inputTableName, getTableResponse.getTableName());
            assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());

    }

    // Helper method to compare schemas ignoring field order
    private boolean schemasMatchIgnoringOrder(Schema expected, Schema actual)
    {
        if (expected == actual) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }
        List<Field> expectedFields = expected.getFields();
        List<Field> actualFields = actual.getFields();
        if (expectedFields.size() != actualFields.size()) {
            return false;
        }

        Map<String, ArrowType> expectedFieldMap = expectedFields.stream()
                .collect(Collectors.toMap(
                        Field::getName,
                        Field::getType,
                        (t1, t2) -> t1, // Merge function to handle duplicate keys (not expected here)
                        LinkedHashMap::new
                ));
        Map<String, ArrowType> actualFieldMap = actualFields.stream()
                .collect(Collectors.toMap(
                        Field::getName,
                        Field::getType,
                        (t1, t2) -> t1,
                        LinkedHashMap::new
                ));

        return expectedFieldMap.equals(actualFieldMap);
    }

    @Test
    public void testDoGetDataSourceCapabilities()
    {
        BlockAllocator allocator = new BlockAllocatorImpl();
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);

        GetDataSourceCapabilitiesResponse response =
                synapseMetadataHandler.doGetDataSourceCapabilities(allocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        assertEquals(TEST_CATALOG, response.getCatalogName());

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
}
