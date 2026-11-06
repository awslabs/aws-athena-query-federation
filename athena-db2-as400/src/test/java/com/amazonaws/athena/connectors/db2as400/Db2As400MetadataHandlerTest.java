/*-
 * #%L
 * athena-db2-as400
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
package com.amazonaws.athena.connectors.db2as400;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
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
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
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
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.db2as400.Db2As400MetadataHandler.PARTITION_NUMBER;
import static org.mockito.ArgumentMatchers.nullable;

public class Db2As400MetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(Db2As400MetadataHandlerTest.class);
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_CATALOG_NAME = "testCatalogName";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_SCHEMA_UPPER = "TESTSCHEMA";
    private static final String TEST_TABLE_UPPER = "TESTTABLE";
    private static final String TEST_SCHEMA_LOWER = "testschema";
    private static final String TEST_TABLE_LOWER = "testtable";
    private static final String TEST_SCHEMA_MIXED = "testSCHEMA";
    private static final String TEST_TABLE_MIXED = "testTABLE";
    private static final String TEST_PARTITIONING_COLUMN = "PC";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_COL3 = "testCol3";
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, Db2As400Constants.NAME,
            "db2as400://jdbc:as400://testhost;user=dummy;password=dummy;");
    private Db2As400MetadataHandler db2As400MetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private BlockAllocator blockAllocator;
    private AthenaClient athena;

    @Before
    public void setup() throws Exception {
        System.setProperty("aws.region", "us-east-1");
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        logger.info(" this.connection.."+ this.connection);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"user\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.db2As400MetadataHandler = new Db2As400MetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = new BlockAllocatorImpl();
    }

    @Test
    public void getPartitionSchema_forCatalog_returnsPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.db2As400MetadataHandler.getPartitionSchema(TEST_CATALOG_NAME));
    }

    @Test
    public void doGetSplits_withNoPartitions_returnsSplits()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        Schema schema = this.db2As400MetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> cols = schema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, schema, cols);

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[] {"DATAPARTITIONID"}, new int[] {Types.INTEGER}, new Object[][] {{}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.db2As400MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(cols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.db2As400MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(PARTITION_NUMBER, "0"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplits_withPartitions_returnsSplits()
            throws Exception {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[]{"TABLE_PARTITION"}, new int[]{Types.INTEGER}, new Object[][]{{0},{1},{2}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        PreparedStatement colNamePreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.COLUMN_INFO_QUERY)).thenReturn(colNamePreparedStatement);
        ResultSet colNameResultSet = mockResultSet(new String[]{"COLUMN_NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_PARTITIONING_COLUMN}}, new AtomicInteger(-1));
        Mockito.when(colNamePreparedStatement.executeQuery()).thenReturn(colNameResultSet);
        Mockito.when(colNameResultSet.next()).thenReturn(true);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.db2As400MetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.db2As400MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.db2As400MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "0",
                db2As400MetadataHandler.PARTITIONING_COLUMN, TEST_PARTITIONING_COLUMN),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "1",
                db2As400MetadataHandler.PARTITIONING_COLUMN, TEST_PARTITIONING_COLUMN),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "2",
                db2As400MetadataHandler.PARTITIONING_COLUMN, TEST_PARTITIONING_COLUMN));

        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayout_whenPartitionsExistButColumnNameMissing_throwsRuntimeException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[]{"TABLE_PARTITION"}, new int[]{Types.INTEGER}, new Object[][]{{0}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        // Partitions exist, but column-info query returns no rows
        PreparedStatement colNamePreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.COLUMN_INFO_QUERY)).thenReturn(colNamePreparedStatement);
        ResultSet colNameResultSet = mockResultSet(new String[]{"COLUMN_NAME"}, new int[]{Types.VARCHAR}, new Object[][]{}, new AtomicInteger(-1));
        Mockito.when(colNamePreparedStatement.executeQuery()).thenReturn(colNameResultSet);

        Schema partitionSchema = this.db2As400MetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        this.db2As400MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);
    }

    @Test
    public void doGetSplits_withContinuationToken_returnsRemainingSplits()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[]{"TABLE_PARTITION"}, new int[]{Types.INTEGER}, new Object[][]{{0}, {1}, {2}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        PreparedStatement colNamePreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.COLUMN_INFO_QUERY)).thenReturn(colNamePreparedStatement);
        ResultSet colNameResultSet = mockResultSet(new String[]{"COLUMN_NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_PARTITIONING_COLUMN}}, new AtomicInteger(-1));
        Mockito.when(colNamePreparedStatement.executeQuery()).thenReturn(colNameResultSet);
        Mockito.when(colNameResultSet.next()).thenReturn(true);

        Schema partitionSchema = this.db2As400MetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);
        GetTableLayoutResponse getTableLayoutResponse = this.db2As400MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);

        // Start from partition index 1
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName,
                getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
        GetSplitsResponse getSplitsResponse = this.db2As400MetadataHandler.doGetSplits(new BlockAllocatorImpl(), getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
                com.google.common.collect.ImmutableMap.of(
                        PARTITION_NUMBER, "1",
                        db2As400MetadataHandler.PARTITIONING_COLUMN, TEST_PARTITIONING_COLUMN),
                com.google.common.collect.ImmutableMap.of(
                        PARTITION_NUMBER, "2",
                        db2As400MetadataHandler.PARTITIONING_COLUMN, TEST_PARTITIONING_COLUMN));

        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
        Assert.assertNull(getSplitsResponse.getContinuationToken());
    }
    
    @Test
    public void doGetSplits_withQueryPassthrough_returnsSingleSplit()
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.db2As400MetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());

        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put("query", "SELECT * FROM test");
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(
                this.federatedIdentity,
                TEST_QUERY_ID,
                TEST_CATALOG,
                tableName,
                Mockito.mock(Block.class),
                new ArrayList<>(partitionCols),
                constraints,
                null);

        GetSplitsResponse getSplitsResponse = this.db2As400MetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);
        Assert.assertEquals(1, getSplitsResponse.getSplits().size());
        Assert.assertEquals(TEST_CATALOG, getSplitsResponse.getCatalogName());
    }

    @Test
    public void doGetDataSourceCapabilities_withDefaultConfig_returnsCapabilities()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);
        GetDataSourceCapabilitiesResponse response = db2As400MetadataHandler.doGetDataSourceCapabilities(blockAllocator, request);

        // AS400 advertises QPT only (enabled by default)
        Assert.assertEquals(TEST_CATALOG, response.getCatalogName());
        Assert.assertNotNull(response.getCapabilities().get("SYSTEM.QUERY"));
        Assert.assertNull(response.getCapabilities().get(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.getOptimization()));
        Assert.assertNull(response.getCapabilities().get(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.getOptimization()));
    }

    @Test
    public void doGetTable_withExistingTable_returnsTableMetadata()
            throws Exception
    {
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[] {"SCHEMA_NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{TEST_SCHEMA_UPPER}, {TEST_SCHEMA_LOWER}, {TEST_SCHEMA_MIXED}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tablePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tablePstmt);
        ResultSet tableResultSet = mockResultSet(new String[] {"TABLE_NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{TEST_TABLE_UPPER}, {TEST_TABLE_LOWER}, {TEST_TABLE_MIXED}}, new AtomicInteger(-1));
        Mockito.when(tablePstmt.executeQuery()).thenReturn(tableResultSet);

        PreparedStatement dataTypePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.COLUMN_INFO_QUERY)).thenReturn(dataTypePstmt);
        Object[][] colTypevalues = {{"TESTCOL1", "INTEGER"}, {"TESTCOL2", "VARCHAR"}, {"TESTCOL3", "TIMESTAMP"}};
        ResultSet dataTypeResultSet = mockResultSet(new String[] {"COLUMN_NAME", "DATA_TYPE"}, new int[] {Types.VARCHAR, Types.VARCHAR}, colTypevalues, new AtomicInteger(-1));
        Mockito.when(dataTypePstmt.executeQuery()).thenReturn(dataTypeResultSet);

        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{Types.INTEGER, 12, TEST_COL1, 0, 0}, {Types.VARCHAR, 25, TEST_COL2, 0, 0},
                {Types.TIMESTAMP, 93, TEST_COL3, 0, 0}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL2, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL3, org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        Mockito.when(connection.getMetaData().getColumns(TEST_CATALOG, TEST_SCHEMA_UPPER, TEST_TABLE_UPPER, null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn(TEST_CATALOG);

        TableName inputTableName = new TableName(TEST_SCHEMA_UPPER, TEST_TABLE_UPPER);
        GetTableResponse getTableResponse = this.db2As400MetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(new TableName(TEST_SCHEMA_UPPER, TEST_TABLE_UPPER), getTableResponse.getTableName());
        Assert.assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());
    }

    @Test(expected = SQLException.class)
    public void doGetTable_withLowerCaseSchemaAndTable_throwsSQLException()
            throws Exception
    {
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{TEST_SCHEMA_UPPER}, {TEST_SCHEMA_LOWER}, {TEST_SCHEMA_MIXED}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{TEST_TABLE_UPPER}, {TEST_TABLE_LOWER}, {TEST_TABLE_MIXED}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(TEST_SCHEMA_LOWER, TEST_TABLE_LOWER);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTable_withLowerCaseSchema_throwsSQLException()
            throws Exception {
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_SCHEMA_UPPER}, {TEST_SCHEMA_LOWER}, {TEST_SCHEMA_MIXED}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tablePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tablePstmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_TABLE_UPPER}, {TEST_TABLE_LOWER}, {TEST_TABLE_MIXED}}, new AtomicInteger(-1));
        Mockito.when(tablePstmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(TEST_SCHEMA_LOWER, TEST_TABLE_UPPER);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTable_withLowerCaseTable_throwsSQLException()
            throws Exception {
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_SCHEMA_UPPER}, {TEST_SCHEMA_LOWER}, {TEST_SCHEMA_MIXED}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_TABLE_UPPER}, {TEST_TABLE_LOWER}, {TEST_TABLE_MIXED}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(TEST_SCHEMA_UPPER, TEST_TABLE_LOWER);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTable_withMixedCaseTable_throwsSQLException()
            throws Exception {
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_SCHEMA_UPPER}, {TEST_SCHEMA_LOWER}, {TEST_SCHEMA_MIXED}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_TABLE_LOWER}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(TEST_SCHEMA_UPPER, TEST_TABLE_MIXED);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doListSchemaNames_whenSchemasExist_returnsSchemaNames() throws Exception {
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        String[][] schemaNames = {{TEST_SCHEMA_UPPER}, {TEST_SCHEMA_LOWER}, {TEST_SCHEMA_MIXED}};
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, schemaNames, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        ListSchemasResponse listSchemasResponse = this.db2As400MetadataHandler.doListSchemaNames(this.blockAllocator, listSchemasRequest);
        String[] expectedSchemas = {TEST_SCHEMA_UPPER, TEST_SCHEMA_LOWER, TEST_SCHEMA_MIXED};
        Assert.assertEquals(Arrays.toString(expectedSchemas), listSchemasResponse.getSchemas().toString());
    }

    @Test
    public void doListTables_whenTablesExist_returnsTables() throws Exception {
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, TEST_SCHEMA_UPPER, null, 0);

        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(stmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{TEST_TABLE_UPPER}, {TEST_TABLE_LOWER}, {TEST_TABLE_MIXED}}, new AtomicInteger(-1));
        Mockito.when(stmt.executeQuery()).thenReturn(tableResultSet);

        ListTablesResponse listTablesResponse = this.db2As400MetadataHandler.doListTables(this.blockAllocator, listTablesRequest);
        TableName[] expectedTables = {new TableName(TEST_SCHEMA_UPPER, TEST_TABLE_UPPER),
                new TableName(TEST_SCHEMA_UPPER, TEST_TABLE_LOWER),
                new TableName(TEST_SCHEMA_UPPER, TEST_TABLE_MIXED)};
        Assert.assertEquals(Arrays.toString(expectedTables), listTablesResponse.getTables().toString());
    }
}

