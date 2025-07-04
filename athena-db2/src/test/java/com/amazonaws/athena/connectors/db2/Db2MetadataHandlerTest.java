/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.db2.resolver.Db2JDBCCaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.NULLIF_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.db2.Db2Constants.PARTITION_NUMBER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;

public class Db2MetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(Db2MetadataHandlerTest.class);
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();

    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "TESTSCHEMA";
    private static final String TEST_TABLE = "TESTTABLE";

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, Db2Constants.NAME,
            "dbtwo://jdbc:db2://hostname:50001/dummydatabase:user=dummyuser;password=dummypwd");
    private Db2MetadataHandler db2MetadataHandler;
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
        logger.info(" this.connection..{}", this.connection);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"user\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.db2MetadataHandler = new Db2MetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new Db2JDBCCaseResolver(Db2Constants.NAME));
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        this.blockAllocator.close();
    }

    @Test
    public void getPartitionSchema_whenCalled_returnsPartitionSchema()
    {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.db2MetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetSplits_withNoPartition_returnsSplits()
            throws Exception
    {
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        TableName tableName = new TableName("testSchema", "testTable");

        Schema schema = this.db2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> cols = schema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, schema, cols);

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[] {"DATAPARTITIONID"}, new int[] {Types.INTEGER}, new Object[][] {{}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.db2MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(cols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.db2MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(PARTITION_NUMBER, "0"));
        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplits_withPartitions_returnsSplits()
            throws Exception
    {
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[]{"DATAPARTITIONID"}, new int[]{Types.INTEGER}, new Object[][]{{0}, {1}, {2}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        PreparedStatement colNamePreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.COLUMN_INFO_QUERY)).thenReturn(colNamePreparedStatement);
        ResultSet colNameResultSet = mockResultSet(new String[]{"COLNAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"PC"}}, new AtomicInteger(-1));
        Mockito.when(colNamePreparedStatement.executeQuery()).thenReturn(colNameResultSet);
        Mockito.when(colNameResultSet.next()).thenReturn(true);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.db2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.db2MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.db2MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "0",
                db2MetadataHandler.PARTITIONING_COLUMN, "PC"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "1",
                db2MetadataHandler.PARTITIONING_COLUMN, "PC"),
            com.google.common.collect.ImmutableMap.of(
                PARTITION_NUMBER, "2",
                db2MetadataHandler.PARTITIONING_COLUMN, "PC"));

        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplits_withQueryPassthrough_returnsSingleSplit()
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.db2MetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toSet());

        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put("query", "SELECT * FROM test");
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        Block partitions = Mockito.mock(Block.class);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(
                this.federatedIdentity,
                TEST_QUERY_ID,
                TEST_CATALOG,
                tableName,
                partitions,
                new ArrayList<>(partitionCols),
                constraints,
                null);

        GetSplitsResponse getSplitsResponse = this.db2MetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        // Verify that exactly one split was returned for query passthrough
        assertEquals(1, getSplitsResponse.getSplits().size());
        assertEquals(TEST_CATALOG, getSplitsResponse.getCatalogName());
    }

    @Test
    public void doGetTable_whenTableExists_returnsTableMetadata()
            throws Exception
    {
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{TEST_SCHEMA}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tablePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tablePstmt);
        ResultSet tableResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{TEST_TABLE}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tablePstmt.executeQuery()).thenReturn(tableResultSet);

        PreparedStatement dataTypePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.COLUMN_INFO_QUERY)).thenReturn(dataTypePstmt);
        Object[][] colTypevalues = {
            {"TESTCOL1", "INTEGER"},
            {"TESTCOL2", "VARCHAR"},
            {"TESTCOL3", "TIMESTAMP"},
            {"structCol", "STRUCT"},
            {"realCol", "REAL"},
            {"doubleCol", "DOUBLE"},
            {"decfloatCol", "DECFLOAT"}
        };
        ResultSet dataTypeResultSet = mockResultSet(new String[] {"colname", "typename"}, new int[] {Types.VARCHAR, Types.VARCHAR}, colTypevalues, new AtomicInteger(-1));
        Mockito.when(dataTypePstmt.executeQuery()).thenReturn(dataTypeResultSet);

        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "TYPE_NAME"};
        Object[][] values = {
            {Types.INTEGER, 12, "testCol1", 0, 0, "INTEGER"},
            {Types.VARCHAR, 25, "testCol2", 0, 0, "VARCHAR"},
            {Types.TIMESTAMP, 93, "testCol3", 0, 0, "TIMESTAMP"},
            {Types.STRUCT, 0, "structCol", 0, 0, "STRUCT"},
            {Types.REAL, 0, "realCol", 0, 0, "REAL"},
            {Types.DOUBLE, 0, "doubleCol", 0, 0, "DOUBLE"},
            {Types.DECIMAL, 0, "decfloatCol", 0, 0, "DECFLOAT"},
            {Types.OTHER, 0, "unsupportedCol", 0, 0, "UNSUPPORTED"}
        };
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, new int[] {Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR}, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("structCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("realCol", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("doubleCol", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("decfloatCol", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("unsupportedCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        Mockito.when(connection.getMetaData().getColumns(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn(TEST_CATALOG);

        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        GetTableResponse getTableResponse = this.db2MetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));

        assertEquals(expected, getTableResponse.getSchema());
        assertEquals(new TableName(TEST_SCHEMA, TEST_TABLE), getTableResponse.getTableName());
        assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTable_whenTableNotFound_throwsRuntimeException()
            throws Exception
    {
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{TEST_SCHEMA}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tablePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tablePstmt);
        // Return empty result set to simulate table not found
        ResultSet tableResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {}, new AtomicInteger(-1));
        Mockito.when(tablePstmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        this.db2MetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableCaseSensitivity()
            throws Exception
    {
        String schemaName = "testschema";
        String tableName = "testtable";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(schemaName, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableCaseSensitivity2()
            throws Exception
    {
        String schemaName = "testschema";
        String tableName = "TESTTABLE";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tablePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tablePstmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tablePstmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(schemaName, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableCaseSensitivity3()
            throws Exception
    {
        String tableName = "testtable";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(TEST_SCHEMA, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableCaseSensitivity4()
            throws Exception
    {
        String tableName = "testTABLE";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"testtable"}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(TEST_SCHEMA, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doListSchemaNames_whenCalled_returnsSchemaNames() throws Exception
    {
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, "queryId", "testCatalog");

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        String[][] schemaNames = {{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}};
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, schemaNames, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        ListSchemasResponse listSchemasResponse = this.db2MetadataHandler.doListSchemaNames(this.blockAllocator, listSchemasRequest);
        String[] expectedSchemas = {"TESTSCHEMA", "testschema", "testSCHEMA"};
        assertEquals(Arrays.toString(expectedSchemas), listSchemasResponse.getSchemas().toString());
    }

    @Test
    public void doListTables_whenCalled_returnsTables() throws Exception
    {
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, TEST_SCHEMA, null, UNLIMITED_PAGE_SIZE_VALUE);

        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(stmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(stmt.executeQuery()).thenReturn(tableResultSet);

        ListTablesResponse listTablesResponse = this.db2MetadataHandler.doListTables(this.blockAllocator, listTablesRequest);
        TableName[] expectedTables = {new TableName("TESTSCHEMA", "TESTTABLE"),
                new TableName("TESTSCHEMA", "testtable"),
                new TableName("TESTSCHEMA", "testTABLE")};
        assertEquals(Arrays.toString(expectedTables), listTablesResponse.getTables().toString());
    }

    @Test
    public void doListPaginatedTables()
            throws Exception
    {
        //Test 1: Testing Single table returned in request of page size 1 and nextToken null
        Object[][] values = {{"testSchema", "testTable"}};
        TableName[] expected = {new TableName("testSchema", "testTable")};
        executePaginatedTableTest(values, expected, null, 1, "1");

        // Test 2: Testing next table returned of page size 1 and nextToken 1
        executePaginatedTableTest(values, expected, "1", 1, "2");

        // Test 3: Testing single table returned when requesting pageSize 2 signifying end of pagination where nextToken is null.
        executePaginatedTableTest(values, expected, "2", 2, null);

        // Test 4: Testing unlimited page size (UNLIMITED_PAGE_SIZE_VALUE) which should set pageSize to Integer.MAX_VALUE
        values = new Object[][]{{"testSchema", "testTable1"}, {"testSchema", "testTable2"}};
        expected = new TableName[]{new TableName("testSchema", "testTable1"), new TableName("testSchema", "testTable2")};
        // Use a non-null token to force pagination path, which will trigger the listPaginatedTables method
        // With unlimited page size, there should be no next token (null) since all results are returned
        executePaginatedTableTest(values, expected, "0", UNLIMITED_PAGE_SIZE_VALUE, null);
    }

    /**
     * Helper method to execute paginated table test and verify results
     * @param values the table data to return in the ResultSet
     * @param expected the expected TableName array
     * @param nextToken the nextToken to use in the request
     * @param pageSize the pageSize to use in the request
     * @param expectedNextToken the expected nextToken in the response
     * @throws Exception if there's an error during execution
     */
    private void executePaginatedTableTest(Object[][] values,
                                           TableName[] expected, String nextToken, int pageSize,
                                           String expectedNextToken) throws Exception {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2Constants.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        ListTablesResponse listTablesResponse = this.db2MetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", nextToken, pageSize));

        assertEquals(expectedNextToken, listTablesResponse.getNextToken());
        assertArrayEquals(expected, listTablesResponse.getTables().toArray());
    }


    @Test
    public void doGetDataSourceCapabilities_whenCalled_returnsCapabilities()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);
        GetDataSourceCapabilitiesResponse response = db2MetadataHandler.doGetDataSourceCapabilities(blockAllocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        // Filter pushdown
        List<OptimizationSubType> filterPushdowns = capabilities.get(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.getOptimization());
        assertNotNull("Filter pushdown capabilities should be present", filterPushdowns);

        List<String> filterSubTypes = filterPushdowns.stream()
                .map(OptimizationSubType::getSubType)
                .collect(Collectors.toList());
        assertTrue("Should support sorted range set pushdown",
                filterSubTypes.contains(FilterPushdownSubType.SORTED_RANGE_SET.getSubType()));
        assertTrue("Should support nullable comparison pushdown",
                filterSubTypes.contains(FilterPushdownSubType.NULLABLE_COMPARISON.getSubType()));

        // Complex expression pushdown
        List<OptimizationSubType> complexExprPushdowns = capabilities.get(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.getOptimization());
        assertNotNull("Complex expression pushdown capabilities should be present", complexExprPushdowns);
        OptimizationSubType complexSubType = complexExprPushdowns.get(0);
        assertEquals("Complex expression subtype should be supported function expression types",
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES.getSubType(), complexSubType.getSubType());
        List<String> supportedFunctions = complexSubType.getProperties();
        assertFalse("NULLIF function should not be supported",
                supportedFunctions.contains(NULLIF_FUNCTION_NAME));

        // TOP N pushdown
        List<OptimizationSubType> topNPushdowns = capabilities.get(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.getOptimization());
        assertNotNull("TOP N pushdown capabilities should be present", topNPushdowns);
        List<String> topNSubTypes = topNPushdowns.stream()
                .map(OptimizationSubType::getSubType)
                .collect(Collectors.toList());
        assertTrue("Should support ORDER BY pushdown",
                topNSubTypes.contains(TopNPushdownSubType.SUPPORTS_ORDER_BY.getSubType()));

        // LIMIT pushdown
        List<OptimizationSubType> limitPushdowns = capabilities.get(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.getOptimization());
        assertNotNull("LIMIT pushdown capabilities should be present", limitPushdowns);
        List<String> limitSubTypes = limitPushdowns.stream()
                .map(OptimizationSubType::getSubType)
                .collect(Collectors.toList());
        assertTrue("Should support integer constant limit pushdown",
                limitSubTypes.contains(LimitPushdownSubType.INTEGER_CONSTANT.getSubType()));
    }
}
