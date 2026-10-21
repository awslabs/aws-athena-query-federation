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
package com.amazonaws.athena.connectors.hortonworks;

import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;

public class HiveMetadataHandlerTest
        extends TestBase
{
    private static final String CATALOG_NAME = "testCatalog";
    private static final String QUERY_ID = "queryId";
    private static final String SECRET_NAME = "testSecret";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String PARTITION_COLUMN_NAME = "partition";

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(CATALOG_NAME, HiveConstants.HIVE_NAME,
            "jdbc:hive2://3.236.108.56:21050/athena;${" + SECRET_NAME + "}", SECRET_NAME);
    private HiveMetadataHandler hiveMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
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
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(SECRET_NAME).build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.hiveMetadataHandler = new HiveMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);

    }

    private void stubPartitionMetadataQueries(String tableName, ResultSet describeResult, ResultSet partitionResult, ResultSet extendedResult)
            throws SQLException
    {
        PreparedStatement preparestatement = Mockito.mock(PreparedStatement.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + tableName.toUpperCase())).thenReturn(preparestatement);
        Mockito.when(this.connection.createStatement()).thenReturn(statement);
        Mockito.when(preparestatement.executeQuery()).thenReturn(describeResult);
        Mockito.when(statement.executeQuery("show partitions " + tableName.toUpperCase())).thenReturn(partitionResult);
        Mockito.when(statement.executeQuery("show table extended like " + tableName.toUpperCase())).thenReturn(extendedResult);
    }

    @Test
    public void getPartitionSchema_whenCatalogProvided_returnsPartitionColumn() {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.hiveMetadataHandler.getPartitionSchema(CATALOG_NAME));
    }

    @Test
    public void doGetTableLayout_whenTableIsPartitioned_returnsPartitionValues()
            throws Exception {

        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP","case_instance"},  {"DATE","case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = new HashSet<>(Arrays.asList(PARTITION_COLUMN_NAME));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, QUERY_ID,
                CATALOG_NAME,tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value2},{value3}};
        String[] columns3 = {"col"};
        int[] types3 = {Types.VARCHAR};
        Object[][] values4 = {{"PARTITIONED:true"}};
        ResultSet resultSet2 = mockResultSet(columns3, types3, values4, new AtomicInteger(-1));
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        stubPartitionMetadataQueries(TEST_TABLE, resultSet, resultSet1, resultSet2);
        Mockito.when(resultSet2.getString(1)).thenReturn("PARTITIONED:true");
        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(expectedValues.get(0), "[partition :  case_date=02-01-2000 and case_number=1 and case_instance=89898990 and case_location='Hyderabad']");
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        assertEquals(expectedSchemaBuilder.build(), getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }

    @Test
    public void doGetTableLayout_whenTableHasNoPartitions_returnsWildcardPartition()
            throws Exception {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"data_type", "col_name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP","case_instance"},  {"DATE","case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = new HashSet<>(Arrays.asList(PARTITION_COLUMN_NAME));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, QUERY_ID,
                CATALOG_NAME,tempTableName, constraints, partitionSchema, partitionCols);
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        ResultSet resultSet1 = mockResultSet(columns2, types2, new Object[][] {}, new AtomicInteger(-1));
        stubPartitionMetadataQueries(TEST_TABLE, resultSet, resultSet1, mockResultSet(new String[] {"col"}, new int[] {Types.VARCHAR}, new Object[][] {}, new AtomicInteger(-1)));
        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(expectedValues, Arrays.asList("[partition : *]"));
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayout_whenConnectionFails_throwsRuntimeException()
            throws Exception {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, QUERY_ID,
                CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);
        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        HiveMetadataHandler hiveMetadataHandlerWithError = new HiveMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        hiveMetadataHandlerWithError.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits_whenTableIsPartitioned_returnsSplitPerPartition()
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
        TableName tempTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = new HashSet<>(Arrays.asList(PARTITION_COLUMN_NAME));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, QUERY_ID,
                CATALOG_NAME, tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value2}, {value3}};
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        stubPartitionMetadataQueries(TEST_TABLE, resultSet, resultSet1, resultSet2);
        Mockito.when(resultSet2.getString(1)).thenReturn("PARTITIONED:true");
        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, tempTableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.hiveMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);
        assertEquals(2, getSplitsResponse.getSplits().size());
    }


    @Test
    public void decodeContinuationToken_whenTokenPresent_returnsToken() throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Constraints constraints = Mockito.mock(Constraints.class);
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());

        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME,
                tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.hiveMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, QUERY_ID,
                CATALOG_NAME, tableName, getTableLayoutResponse.getPartitions(),
                new ArrayList<>(partitionCols), constraints, "1");

        Integer splitRequestToken=0;
        if (getSplitsRequest.hasContinuationToken()) {
            splitRequestToken=Integer.valueOf(getSplitsRequest.getContinuationToken());
        }

       assertNotNull(splitRequestToken.toString());

    }

    @Test
    public void doGetTable_whenTableExists_returnsTableNameAndCatalog()
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
        PreparedStatement preparestatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(HiveMetadataHandler.GET_METADATA_QUERY + inputTableName.getTableName().toUpperCase())).thenReturn(preparestatement);
        Mockito.when(preparestatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        Mockito.when(this.connection.getMetaData().getColumns(CATALOG_NAME, inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet1);
        Mockito.when(this.connection.getCatalog()).thenReturn(CATALOG_NAME);
        GetTableResponse getTableResponse = this.hiveMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, inputTableName, Collections.emptyMap()));
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals(CATALOG_NAME, getTableResponse.getCatalogName());
    }

    @Test
    public void doGetTable_whenTableHasNoColumns_returnsWithoutError() throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        this.hiveMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTable_whenMetadataThrows_propagatesSqlException()
            throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.hiveMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doGetSplits_whenQueryPassthroughEnabled_returnsSingleSplitCarryingTheQuery()
            throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.hiveMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        String query = "SELECT * FROM testSchema.testTable WHERE testCol1 = 1";
        Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<String, String>()
                .put(QUERY, query)
                .put(SCHEMA_FUNCTION_NAME, "system.query")
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", "query")
                .put("schema", "system")
                .build();
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME, tableName,
                Mockito.mock(Block.class), new ArrayList<>(partitionCols), constraints, null);

        GetSplitsResponse getSplitsResponse = this.hiveMetadataHandler.doGetSplits(this.blockAllocator, getSplitsRequest);

        assertEquals(1, getSplitsResponse.getSplits().size());
        assertEquals(CATALOG_NAME, getSplitsResponse.getCatalogName());
        assertEquals(query, getSplitsResponse.getSplits().iterator().next().getProperties().get(QUERY));
    }

    @Test
    public void doGetDataSourceCapabilities_whenCapabilitiesRequested_returnsFilterComplexAndLimitPushdowns()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(this.federatedIdentity, QUERY_ID, CATALOG_NAME);
        GetDataSourceCapabilitiesResponse response = this.hiveMetadataHandler.doGetDataSourceCapabilities(this.blockAllocator, request);
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        List<String> filterSubTypes = capabilities.get(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.getOptimization())
                .stream().map(OptimizationSubType::getSubType).collect(Collectors.toList());
        assertTrue(filterSubTypes.contains(FilterPushdownSubType.SORTED_RANGE_SET.getSubType()));
        assertTrue(filterSubTypes.contains(FilterPushdownSubType.NULLABLE_COMPARISON.getSubType()));

        OptimizationSubType complexSubType = capabilities.get(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.getOptimization()).get(0);
        assertEquals(ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES.getSubType(), complexSubType.getSubType());

        List<String> limitSubTypes = capabilities.get(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.getOptimization())
                .stream().map(OptimizationSubType::getSubType).collect(Collectors.toList());
        assertTrue(limitSubTypes.contains(LimitPushdownSubType.INTEGER_CONSTANT.getSubType()));
    }
}
