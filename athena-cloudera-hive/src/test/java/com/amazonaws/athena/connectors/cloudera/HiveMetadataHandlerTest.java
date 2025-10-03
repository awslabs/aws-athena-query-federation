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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;

public class HiveMetadataHandlerTest
        extends TestBase
{
    private static final String CATALOG_NAME = "testCatalog";
    private static final String QUERY_ID = "queryId";
    private static final String BASE_CONNECTION_STRING = "hive://jdbc:hive2://testHost:21050/default;AuthMech=3;";
    private static final String SECRET_NAME = "testSecret";

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(CATALOG_NAME, HiveConstants.HIVE_NAME,
            BASE_CONNECTION_STRING + "${" + SECRET_NAME + "}", SECRET_NAME);
    private HiveMetadataHandler hiveMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator blockAllocator;

    @BeforeClass
    public static void dataSetUP()
    {
        System.setProperty("aws.region", "us-west-2");
    }

    @Before
    public void setup()
            throws Exception
    {
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(SECRET_NAME).build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.hiveMetadataHandler = new HiveMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.hiveMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP", "case_instance"},  {"DATE", "case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = new HashSet<>(Arrays.asList("partition"));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId",
                "testCatalogName", tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value3}, {value2}};
        String[] columns3 = {"col"};
        int[] types3 = {Types.VARCHAR};
        Object[][] values4 = {{"PARTITIONED:true"}};
        ResultSet resultSet2 = mockResultSet(columns3, types3, values4, new AtomicInteger(-1));
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        String tableName = getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + tableName)).thenReturn(preparestatement1);
        final String getPartitionExistsSql = "show table extended in " + getTableLayoutRequest.getTableName().getSchemaName() + " like "  + getTableLayoutRequest.getTableName().getTableName().toUpperCase();
        final String getPartitionDetailsSql = "show partitions "  + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
        Statement statement1 = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement1);
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(statement1.executeQuery(getPartitionDetailsSql)).thenReturn(resultSet1);
        Mockito.when(statement1.executeQuery(getPartitionExistsSql)).thenReturn(resultSet2);
        Mockito.when(resultSet2.getString(1)).thenReturn("PARTITIONED:true");
        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(2, actualValues.size());
        Assert.assertEquals("[partition :  case_date=02-01-2000 and case_number=1 and case_instance=89898990 and case_location='Hyderabad']", actualValues.get(0));
        Assert.assertEquals("[partition :  case_date=01-01-2000 and case_number=0 and case_instance=89898989 and case_location is NULL]", actualValues.get(1));
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP", "case_instance"},  {"DATE", "case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = new HashSet<>(Arrays.asList("partition"));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId",
                "testCatalogName", tempTableName, constraints, partitionSchema, partitionCols);
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {};
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + tempTableName.getQualifiedTableName())).thenReturn(preparestatement1);
        final String getPartitionDetailsSql = "show partitions "  + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
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
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);
        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        HiveMetadataHandler implalaMetadataHandler = new HiveMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        implalaMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP", "case_instance"},  {"DATE", "case_date"}};
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
                "testCatalogName", tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value2}, {value3}};
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        String tableName = getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + tableName)).thenReturn(preparestatement1);
        final String getPartitionExistsSql = "show table extended in " + getTableLayoutRequest.getTableName().getSchemaName() + " like "  + getTableLayoutRequest.getTableName().getTableName().toUpperCase();
        final String getPartitionDetailsSql = "show partitions "  + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
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

        Integer splitRequestToken = 0;
        if (getSplitsRequest.hasContinuationToken()) {
            splitRequestToken = Integer.valueOf(getSplitsRequest.getContinuationToken());
        }

        Assert.assertNotNull(splitRequestToken.toString());
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP", "case_instance"},  {"DATE", "case_date"}, {"BINARY", "case_binary"}, {"DOUBLE", "case_double"},
                {"FLOAT", "case_float"},  {"BOOLEAN", "case_boolean"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        String[] schema1 = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values1 = {{Types.INTEGER, 12, "case_number", 0, 0}, {Types.VARCHAR, 25, "case_location", 0, 0},
                {Types.TIMESTAMP, 93, "case_instance", 0, 0}, {Types.DATE, 91, "case_date", 0, 0}, {Types.VARBINARY, 91, "case_binary", 0, 0},
                {Types.DOUBLE, 91, "case_double", 0, 0}, {Types.FLOAT, 91, "case_float", 0, 0}, {Types.BOOLEAN, 1, "case_boolean", 0, 0}};
        ResultSet resultSet1 = mockResultSet(schema1, values1, new AtomicInteger(-1));
        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + inputTableName.getQualifiedTableName().toUpperCase())).thenReturn(preparestatement1);
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        Mockito.when(this.connection.getMetaData().getColumns(CATALOG_NAME, inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet1);
        Mockito.when(this.connection.getCatalog()).thenReturn(CATALOG_NAME);
        GetTableResponse getTableResponse = this.hiveMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, inputTableName, Collections.emptyMap()));
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals(CATALOG_NAME, getTableResponse.getCatalogName());
    }

    @Test
    public void doGetTableNoColumns() throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        this.hiveMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.hiveMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, inputTableName, Collections.emptyMap()));
    }

    @Test
    public void testGetCredentialProvider_withSecret() throws Exception
    {
        DatabaseConnectionConfig configWithSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, HiveConstants.HIVE_NAME,
                BASE_CONNECTION_STRING + "${" + SECRET_NAME + "}", SECRET_NAME);

        CredentialsProvider provider = captureCredentialsProvider(configWithSecret);

        assertNotNull("CredentialsProvider should not be null when secret is configured", provider);
        assertTrue("CredentialsProvider should be an instance of HiveCredentialsProvider",
                provider instanceof HiveCredentialsProvider);
    }

    @Test
    public void testGetCredentialProvider_withoutSecret() throws Exception
    {
        DatabaseConnectionConfig configWithoutSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, HiveConstants.HIVE_NAME,
                BASE_CONNECTION_STRING);

        CredentialsProvider provider = captureCredentialsProvider(configWithoutSecret);

        assertNull("CredentialsProvider should be null when no secret is configured", provider);
    }

    /**
     * Captures the CredentialsProvider used by the JDBC connection factory
     */
    private CredentialsProvider captureCredentialsProvider(DatabaseConnectionConfig config) throws Exception
    {
        HiveMetadataHandler handler = new HiveMetadataHandler(
                config, secretsManager, athena, jdbcConnectionFactory,
                com.google.common.collect.ImmutableMap.of());

        handler.doListSchemaNames(new BlockAllocatorImpl(),
                new ListSchemasRequest(federatedIdentity, QUERY_ID, CATALOG_NAME));

        ArgumentCaptor<CredentialsProvider> captor = ArgumentCaptor.forClass(CredentialsProvider.class);
        verify(jdbcConnectionFactory).getConnection(captor.capture());
        return captor.getValue();
    }
}
