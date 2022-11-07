/*-
 * #%L
 * athena-cloudera-hive
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class HiveMetadataHandlerTest
        extends TestBase {

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", HiveConstants.HIVE_NAME,
            "jdbc:hive2://3.236.108.56:21050/athena;${testSecret}", "testSecret");
    private HiveMetadataHandler hiveMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private BlockAllocator blockAllocator;

    @BeforeClass
    public static void dataSetUP() {
        System.setProperty("aws.region", "us-west-2");
    }

    @Before
    public void setup()
            throws Exception
    {
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        this.hiveMetadataHandler = new HiveMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory);
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);

    }


    @Test
    public void getPartitionSchema() {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.hiveMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout()
            throws Exception {

        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP","case_instance"},  {"DATE","case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = new HashSet<>(Arrays.asList("partition"));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId",
                "testCatalogName",tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value3},{value2}};
        String[] columns3 = {"col"};
        int[] types3 = {Types.VARCHAR};
        Object[][] values4 = {{"PARTITIONED:true"}};
        ResultSet resultSet2 = mockResultSet(columns3, types3, values4, new AtomicInteger(-1));
        Mockito.when(jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(connection);
        String tableName =getTableLayoutRequest.getTableName().getTableName().toUpperCase();
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + tableName)).thenReturn(preparestatement1);
        final String getPartitionExistsSql = "show table extended like "  + tableName;
        final String getPartitionDetailsSql = "show partitions "  + getTableLayoutRequest.getTableName().getTableName().toUpperCase();
        Statement statement1 = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement1);
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(statement1.executeQuery(getPartitionDetailsSql)).thenReturn(resultSet1);
        Mockito.when(statement1.executeQuery(getPartitionExistsSql)).thenReturn(resultSet2);
        Mockito.when(resultSet2.getString(1)).thenReturn("PARTITIONED:true");
        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues.get(0), "[partition :  case_date=02-01-2000 and case_number=1 and case_instance=89898990 and case_location='Hyderabad']");
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP","case_instance"},  {"DATE","case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = new HashSet<>(Arrays.asList("partition"));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId",
                "testCatalogName",tempTableName, constraints, partitionSchema, partitionCols);
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {};
        Mockito.when(jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(connection);
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY+tempTableName.getTableName())).thenReturn(preparestatement1);
        final String getPartitionDetailsSql = "show partitions "  + getTableLayoutRequest.getTableName().getTableName().toUpperCase();
        Statement statement1 = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement1);
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(statement1.executeQuery(getPartitionDetailsSql)).thenReturn(resultSet1);
        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Arrays.asList("[partition : *]"));
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);
        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        HiveMetadataHandler implalaMetadataHandler = new HiveMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory);
        implalaMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP","case_instance"},  {"DATE","case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        String[] columns3 = {"col"};
        int[] types3 = {Types.VARCHAR};
        Object[][] values4 = {{"Partitioned:true"}};
        ResultSet resultSet2 = mockResultSet(columns3, types3, values4, new AtomicInteger(-1));
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = new HashSet<>(Arrays.asList("partition"));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId",
                "testCatalogName",tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value2},{value3}};
        Mockito.when(jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(connection);
        String tableName =getTableLayoutRequest.getTableName().getTableName().toUpperCase();
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY+tableName)).thenReturn(preparestatement1);
        final String getPartitionExistsSql = "show table extended like "  + tableName;
        final String getPartitionDetailsSql = "show partitions "  + getTableLayoutRequest.getTableName().getTableName().toUpperCase();
        Statement statement1 = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement1);
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(statement1.executeQuery(getPartitionDetailsSql)).thenReturn(resultSet1);
        Mockito.when(statement1.executeQuery(getPartitionExistsSql)).thenReturn(resultSet2);
        Mockito.when(resultSet2.getString(1)).thenReturn("PARTITIONED:true");
        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tempTableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.hiveMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);
        Assert.assertEquals(2, getSplitsResponse.getSplits().size());
    }


    @Test
    public void decodeContinuationToken() throws Exception
    {
        TableName tableName = new TableName("testSchema", "testTable");
        Constraints constraints = Mockito.mock(Constraints.class);
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());

        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName",
                tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId",
                "testCatalogName", tableName, getTableLayoutResponse.getPartitions(),
                new ArrayList<>(partitionCols), constraints, "1");

        Integer splitRequestToken=0;
        if (getSplitsRequest.hasContinuationToken()) {
            splitRequestToken=Integer.valueOf(getSplitsRequest.getContinuationToken());
        }

        Assert.assertNotNull(splitRequestToken.toString());

    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP","case_instance"},  {"DATE","case_date"}, {"BINARY","case_binary"}, {"DOUBLE","case_double"},
                {"FLOAT","case_float"},  {"BOOLEAN","case_boolean"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        String[] schema1 = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values1 = {{Types.INTEGER, 12, "case_number", 0, 0}, {Types.VARCHAR, 25, "case_location", 0, 0},
                {Types.TIMESTAMP, 93, "case_instance", 0, 0}, {Types.DATE, 91, "case_date", 0, 0},{Types.VARBINARY, 91, "case_binary", 0, 0},
                {Types.DOUBLE, 91, "case_double", 0, 0},{Types.FLOAT, 91, "case_float", 0, 0}, {Types.BOOLEAN, 1, "case_boolean", 0, 0}};
        ResultSet resultSet1 = mockResultSet(schema1, values1, new AtomicInteger(-1));
        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + inputTableName.getTableName().toUpperCase())).thenReturn(preparestatement1);
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        Mockito.when(this.connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet1);
        Mockito.when(this.connection.getCatalog()).thenReturn("testCatalog");
        GetTableResponse getTableResponse = this.hiveMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName));
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }
    @Test
    public void doGetTableNoColumns() throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        this.hiveMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName));
    }
    @Test
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        Mockito.when(this.connection.getMetaData().getColumns(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new SQLException());
        this.hiveMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName));
    }
}
