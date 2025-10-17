/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.saphana.resolver.SaphanaJDBCCaseResolver;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.SAPHANA_NAME;
import static org.mockito.ArgumentMatchers.nullable;

public class SaphanaMetadataHandlerTest
        extends TestBase
{
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SaphanaConstants.SAPHANA_NAME,
            "saphana://jdbc:sap://hostname/user=A&password=B");
    private SaphanaMetadataHandler saphanaMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator blockAllocator;
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();


    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.saphanaMetadataHandler = new SaphanaMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new SaphanaJDBCCaseResolver(SAPHANA_NAME));
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.saphanaMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = new HashSet<>(Arrays.asList(BLOCK_PARTITION_COLUMN_NAME)); //partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {BLOCK_PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);


        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Arrays.asList("[part_id : p0]", "[part_id : p1]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getSchemaName());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {BLOCK_PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Collections.singletonList("[part_id : 0]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        SaphanaMetadataHandler saphanaMetadataHandler = new SaphanaMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new SaphanaJDBCCaseResolver(SAPHANA_NAME));

        saphanaMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        String[] columns = {BLOCK_PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.saphanaMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "p0"));
        expectedSplits.add(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doListPaginatedTables()
            throws Exception
    {
        SaphanaMetadataHandler metadataHandler = new SaphanaMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new SaphanaJDBCCaseResolver(SAPHANA_NAME, CaseResolver.FederationSDKCasingMode.NONE));
        BlockAllocator blockAllocator = new BlockAllocatorImpl();

        // Test 1: Testing Single table returned in request of page size 1 and nextToken null
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(saphanaMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}};
        TableName[] expected = {new TableName("testSchema", "testTable")};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ListTablesResponse listTablesResponse = metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, 1));

        Assert.assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        // Test 2: Testing next table returned of page size 1 and nextToken 1
        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(saphanaMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        values = new Object[][]{{"testSchema", "testTable2"}};
        expected = new TableName[]{new TableName("testSchema", "testTable2")};
        resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        listTablesResponse = metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "1", 1));
        Assert.assertEquals("2", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        // Test 3: Testing single table returned when requesting pageSize 2 signifying end of pagination where nextToken is null
        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(saphanaMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        values = new Object[][]{{"testSchema", "testTable2"}};
        expected = new TableName[]{new TableName("testSchema", "testTable2")};
        resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        listTablesResponse = metadataHandler.doListTables(blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                "testCatalog", "testSchema", "2", 2));
        Assert.assertEquals(null, listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        // Test 4: nextToken is 2 and pageSize is UNLIMITED. Return all tables starting from index 2.
        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(saphanaMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        values = new Object[][]{{"testSchema", "testTable3"}, {"testSchema", "testTable4"}};
        expected = new TableName[]{new TableName("testSchema", "testTable3"), new TableName("testSchema", "testTable4")};
        resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        listTablesResponse = metadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "2", ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE));
        Assert.assertEquals(null, listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        // Test 5: AthenaConnectorException with negative nextToken value
        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(saphanaMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        values = new Object[][]{{"testSchema", "testTable3"}, {"testSchema", "testTable4"}};
        expected = new TableName[]{new TableName("testSchema", "testTable3"), new TableName("testSchema", "testTable4")};
        resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Assert.assertThrows(AthenaConnectorException.class, () -> this.saphanaMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "-1", 3)));
        Assert.assertEquals(null, listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        // Test 6: AthenaConnectorException with negative pageSize value
        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(saphanaMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        values = new Object[][]{{"testSchema", "testTable3"}, {"testSchema", "testTable4"}};
        expected = new TableName[]{new TableName("testSchema", "testTable3"), new TableName("testSchema", "testTable4")};
        resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Assert.assertThrows(AthenaConnectorException.class, () -> this.saphanaMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", "0", -3)));
        Assert.assertEquals(null, listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

    }

    @Test
    public void doGetSplitsContinuation()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(SaphanaConstants.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {BLOCK_PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
        GetSplitsResponse getSplitsResponse = this.saphanaMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "p1"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
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
        
        // Mock schema case resolution queries - return single schema match
        String[] schemaQuerySchema = {"SCHEMA_NAME"};
        Object[][] schemaQueryValues = {{"TESTSCHEMA"}};
        AtomicInteger schemaRowNumber = new AtomicInteger(-1);
        ResultSet schemaResultSet = mockResultSet(schemaQuerySchema, schemaQueryValues, schemaRowNumber);

        PreparedStatement schemaStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement("select * from SYS.SCHEMAS where lower(SCHEMA_NAME) = ?")).thenReturn(schemaStmt);
        Mockito.when(schemaStmt.executeQuery()).thenReturn(schemaResultSet);
        
        // Mock table case resolution queries - return single table match
        String[] tableQuerySchema = {"TABLE_NAME"};
        Object[][] tableQueryValues = {{"TESTTABLE"}};
        AtomicInteger tableRowNumber = new AtomicInteger(-1);
        ResultSet tableResultSet = mockResultSet(tableQuerySchema, tableQueryValues, tableRowNumber);
        
        PreparedStatement tableStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement("select * from SYS.TABLES where SCHEMA_NAME = ? and lower(TABLE_NAME) = ?")).thenReturn(tableStmt);
        Mockito.when(tableStmt.executeQuery()).thenReturn(tableResultSet);
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        GetTableResponse getTableResponse = this.saphanaMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doGetSplitsForView()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement viewCheckPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(SaphanaConstants.VIEW_CHECK_QUERY)).thenReturn(viewCheckPreparedStatement);
        ResultSet viewCheckqueryResultSet = mockResultSet(new String[] {"VIEW_NAME"}, new int[] {Types.VARCHAR}, new Object[][] {{"COVID19"}}, new AtomicInteger(-1));
        Mockito.when(viewCheckPreparedStatement.executeQuery()).thenReturn(viewCheckqueryResultSet);

        Schema partitionSchema = this.saphanaMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.saphanaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.saphanaMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "0"));

        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());

        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.saphanaMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test (expected = RuntimeException.class)
    public void doGetTableNoColumns() throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        this.saphanaMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doGetTableWithAnnotation()
            throws Exception
    {
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

        String schemaName = "testSchema";
        String tableName = "testTable";
        TableName inputTableName = new TableName(schemaName, tableName + "@schemaCase=upper&tableCase=lower");

        SaphanaMetadataHandler saphanaMetadataHandlerWithAnnotation = new SaphanaMetadataHandler(databaseConnectionConfig,
            this.secretsManager, this.athena, this.jdbcConnectionFactory, 
            com.google.common.collect.ImmutableMap.of(), 
            new SaphanaJDBCCaseResolver(SAPHANA_NAME, CaseResolver.FederationSDKCasingMode.ANNOTATION));
        Mockito.when(connection.getMetaData().getColumns("testCatalog", schemaName.toUpperCase(), tableName.toLowerCase(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = saphanaMetadataHandlerWithAnnotation.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(new TableName(schemaName.toUpperCase(), tableName.toLowerCase()), getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doGetTableNotFoundWithAnnotation()
            throws Exception
    {
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

        String schemaName = "testSchema";
        String tableName = "testTable";
        TableName inputTableName = new TableName(schemaName, tableName + "@schemaCase=upper&tableCase=lower");

        Mockito.when(connection.getMetaData().getColumns("testCatalog", schemaName, tableName.toLowerCase(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

        Assert.assertThrows(Exception.class, () -> this.saphanaMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap())));
    }

    @Test
    public void doGetTableWithNoneCasing()
            throws Exception
    {
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

        String schemaName = "TestSchema";
        String tableName = "TestTable";
        TableName inputTableName = new TableName(schemaName, tableName);

        SaphanaMetadataHandler saphanaMetadataHandlerNone = new SaphanaMetadataHandler(databaseConnectionConfig,
                this.secretsManager, this.athena, this.jdbcConnectionFactory,
                com.google.common.collect.ImmutableMap.of(CaseResolver.CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()),
                new SaphanaJDBCCaseResolver(SAPHANA_NAME, CaseResolver.FederationSDKCasingMode.NONE));

        Mockito.when(connection.getMetaData().getColumns("testCatalog", schemaName, tableName, null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = saphanaMetadataHandlerNone.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(new TableName(schemaName, tableName), getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }
}
