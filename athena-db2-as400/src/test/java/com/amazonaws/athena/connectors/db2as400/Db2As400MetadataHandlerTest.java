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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
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
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
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
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.nullable;

public class Db2As400MetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(Db2As400MetadataHandlerTest.class);
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField("PARTITION_NUMBER", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", Db2As400Constants.NAME,
            "db2as400://jdbc:as400://testhost;user=dummy;password=dummy;");
    private Db2As400MetadataHandler db2As400MetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private AWSSecretsManager secretsManager;
    private BlockAllocator blockAllocator;
    private AmazonAthena athena;

    @Before
    public void setup() throws Exception {
        System.setProperty("aws.region", "us-east-1");
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        logger.info(" this.connection.."+ this.connection);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"user\": \"testUser\", \"password\": \"testPassword\"}"));
        this.db2As400MetadataHandler = new Db2As400MetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = new BlockAllocatorImpl();
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(Db2As400MetadataHandler.PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.db2As400MetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetSplitsWithNoPartition()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        Schema schema = this.db2As400MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> cols = schema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, schema, cols);

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[] {"DATAPARTITIONID"}, new int[] {Types.INTEGER}, new Object[][] {{}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.db2As400MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(cols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.db2As400MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(db2As400MetadataHandler.PARTITION_NUMBER, "0"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplits()
            throws Exception {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement partitionPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.PARTITION_QUERY)).thenReturn(partitionPreparedStatement);
        ResultSet partitionResultSet = mockResultSet(new String[]{"TABLE_PARTITION"}, new int[]{Types.INTEGER}, new Object[][]{{0},{1},{2}}, new AtomicInteger(-1));
        Mockito.when(partitionPreparedStatement.executeQuery()).thenReturn(partitionResultSet);

        PreparedStatement colNamePreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.COLUMN_INFO_QUERY)).thenReturn(colNamePreparedStatement);
        ResultSet colNameResultSet = mockResultSet(new String[]{"COLUMN_NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"PC"}}, new AtomicInteger(-1));
        Mockito.when(colNamePreparedStatement.executeQuery()).thenReturn(colNameResultSet);
        Mockito.when(colNameResultSet.next()).thenReturn(true);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.db2As400MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.db2As400MetadataHandler.doGetTableLayout(this.blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.db2As400MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                db2As400MetadataHandler.PARTITION_NUMBER, "0",
                db2As400MetadataHandler.PARTITIONING_COLUMN, "PC"),
            com.google.common.collect.ImmutableMap.of(
                db2As400MetadataHandler.PARTITION_NUMBER, "1",
                db2As400MetadataHandler.PARTITIONING_COLUMN, "PC"),
            com.google.common.collect.ImmutableMap.of(
                db2As400MetadataHandler.PARTITION_NUMBER, "2",
                db2As400MetadataHandler.PARTITIONING_COLUMN, "PC"));

        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String schemaName = "TESTSCHEMA";
        String tableName = "TESTTABLE";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[] {"SCHEMA_NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tablePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tablePstmt);
        ResultSet tableResultSet = mockResultSet(new String[] {"TABLE_NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tablePstmt.executeQuery()).thenReturn(tableResultSet);

        PreparedStatement dataTypePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.COLUMN_INFO_QUERY)).thenReturn(dataTypePstmt);
        Object[][] colTypevalues = {{"TESTCOL1", "INTEGER"}, {"TESTCOL2", "VARCHAR"}, {"TESTCOL3", "TIMESTAMP"}};
        ResultSet dataTypeResultSet = mockResultSet(new String[] {"COLUMN_NAME", "DATA_TYPE"}, new int[] {Types.VARCHAR, Types.VARCHAR}, colTypevalues, new AtomicInteger(-1));
        Mockito.when(dataTypePstmt.executeQuery()).thenReturn(dataTypeResultSet);

        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{Types.INTEGER, 12, "testCol1", 0, 0}, {Types.VARCHAR, 25, "testCol2", 0, 0},
                {Types.TIMESTAMP, 93, "testCol3", 0, 0}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        Mockito.when(connection.getMetaData().getColumns("testCatalog", schemaName, tableName, null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        GetTableResponse getTableResponse = this.db2As400MetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(new TableName(schemaName, tableName), getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
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
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[] {"NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(schemaName, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableCaseSensitivity2()
            throws Exception {
        String schemaName = "testschema";
        String tableName = "TESTTABLE";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tablePstmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tablePstmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tablePstmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(schemaName, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableCaseSensitivity3()
            throws Exception {
        String schemaName = "TESTSCHEMA";
        String tableName = "testtable";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(schemaName, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableCaseSensitivity4()
            throws Exception {
        String schemaName = "TESTSCHEMA";
        String tableName = "testTABLE";

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}}, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(tableStmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"testtable"}}, new AtomicInteger(-1));
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);

        TableName inputTableName = new TableName(schemaName, tableName);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.db2As400MetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doListSchemaNames() throws Exception {
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, "queryId", "testCatalog");

        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        String[][] schemaNames = {{"TESTSCHEMA"}, {"testschema"}, {"testSCHEMA"}};
        ResultSet schemaResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, schemaNames, new AtomicInteger(-1));
        Mockito.when(statement.executeQuery(Db2As400Constants.QRY_TO_LIST_SCHEMAS)).thenReturn(schemaResultSet);

        ListSchemasResponse listSchemasResponse = this.db2As400MetadataHandler.doListSchemaNames(this.blockAllocator, listSchemasRequest);
        String[] expectedSchemas = {"TESTSCHEMA", "testschema", "testSCHEMA"};
        Assert.assertEquals(Arrays.toString(expectedSchemas), listSchemasResponse.getSchemas().toString());
    }

    @Test
    public void doListTables() throws Exception {
        String schemaName = "TESTSCHEMA";
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, "queryId", "testCatalog", schemaName, null, 0);

        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Db2As400Constants.QRY_TO_LIST_TABLES_AND_VIEWS)).thenReturn(stmt);
        ResultSet tableResultSet = mockResultSet(new String[]{"NAME"}, new int[]{Types.VARCHAR}, new Object[][]{{"TESTTABLE"}, {"testtable"}, {"testTABLE"}}, new AtomicInteger(-1));
        Mockito.when(stmt.executeQuery()).thenReturn(tableResultSet);

        ListTablesResponse listTablesResponse = this.db2As400MetadataHandler.doListTables(this.blockAllocator, listTablesRequest);
        TableName[] expectedTables = {new TableName("TESTSCHEMA", "TESTTABLE"),
                new TableName("TESTSCHEMA", "testtable"),
                new TableName("TESTSCHEMA", "testTABLE")};
        Assert.assertEquals(Arrays.toString(expectedTables), listTablesResponse.getTables().toString());
    }
}

