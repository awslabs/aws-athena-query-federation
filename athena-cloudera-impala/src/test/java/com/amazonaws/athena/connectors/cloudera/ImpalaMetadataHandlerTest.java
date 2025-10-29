/*-
 * #%L
 * athena-cloudera-impala
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web services
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
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
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
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;

public class ImpalaMetadataHandlerTest
        extends TestBase
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_PARTITION = "partition";

    private DatabaseConnectionConfig databaseConnectionConfig;
    private ImpalaMetadataHandler impalaMetadataHandler;
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
        final String testImpalaConnectionString = "impala://jdbc:impala://localhost:10000/athena;{" + TEST_SECRET + "}";
        this.blockAllocator = new BlockAllocatorImpl();
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, ImpalaConstants.IMPALA_NAME,
                testImpalaConnectionString, TEST_SECRET);
        this.impalaMetadataHandler = new ImpalaMetadataHandler(this.databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @After
    public void tearDown()
    {
        this.blockAllocator.close();
    }

    @Test
    public void getPartitionSchema()
    {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(TEST_PARTITION, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        String[] schema = {"type", "name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP", "case_instance"},  {"DATE", "case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = new HashSet<>(Arrays.asList(TEST_PARTITION));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID,
                TEST_CATALOG, tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value3}, {value2}};
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ImpalaMetadataHandler.GET_METADATA_QUERY + tempTableName.getQualifiedTableName().toUpperCase())).thenReturn(preparestatement1);
        final String getPartitionDetailsSql = "show files in "  + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
        Statement statement1 = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement1);
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(statement1.executeQuery(getPartitionDetailsSql)).thenReturn(resultSet1);
        GetTableLayoutResponse getTableLayoutResponse = this.impalaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(2, expectedValues.size());
        assertEquals("[partition :  case_date=02-01-2000 and case_number=1 and case_instance=89898990 and case_location='Hyderabad']", expectedValues.get(0));
        assertEquals("[partition :  case_date=01-01-2000 and case_number=0 and case_instance=89898989 and case_location is NULL]", expectedValues.get(1));
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }

   @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
   {
       String[] schema = {"type", "name"};
       Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
               {"TIMESTAMP", "case_instance"},  {"DATE", "case_date"}};
       AtomicInteger rowNumber = new AtomicInteger(-1);
       ResultSet resultSet = mockResultSet(schema, values, rowNumber);
       Constraints constraints = Mockito.mock(Constraints.class);
       TableName tempTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
       Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
       Set<String> partitionCols = new HashSet<>(Arrays.asList(TEST_PARTITION));
       GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID,
               TEST_CATALOG, tempTableName, constraints, partitionSchema, partitionCols);
       String[] columns2 = {"Partition"};
       int[] types2 = {Types.VARCHAR};
       Object[][] values1 = {};
       Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
       String tableName = getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
       PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
       Mockito.when(this.connection.prepareStatement(ImpalaMetadataHandler.GET_METADATA_QUERY + tableName)).thenReturn(preparestatement1);
       final String getPartitionDetailsSql = "show files in "  + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
       Statement statement1 = Mockito.mock(Statement.class);
       Mockito.when(this.connection.createStatement()).thenReturn(statement1);
       ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
       Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
       Mockito.when(statement1.executeQuery(getPartitionDetailsSql)).thenReturn(resultSet1);
       GetTableLayoutResponse getTableLayoutResponse = this.impalaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
       List<String> expectedValues = new ArrayList<>();
       for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
           expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
       }
       assertEquals(expectedValues, Arrays.asList("[partition : *]"));
       SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
       expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
       Schema expectedSchema = expectedSchemaBuilder.build();
       assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
       assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tableName, constraints, partitionSchema, partitionCols);
        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        ImpalaMetadataHandler implalaMetadataHandler = new ImpalaMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());
        implalaMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        String[] schema = {"type", "name"};
        Object[][] values = {{"INTEGER", "case_number"}, {"VARCHAR", "case_location"},
                {"TIMESTAMP", "case_instance"},  {"DATE", "case_date"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = new HashSet<>(Arrays.asList(TEST_PARTITION));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID,
                TEST_CATALOG, tempTableName, constraints, partitionSchema, partitionCols);
        String value2 = "case_date=01-01-2000/case_number=0/case_instance=89898989/case_location=__HIVE_DEFAULT_PARTITION__";
        String value3 = "case_date=02-01-2000/case_number=1/case_instance=89898990/case_location=Hyderabad";
        String[] columns2 = {"Partition"};
        int[] types2 = {Types.VARCHAR};
        Object[][] values1 = {{value2}, {value3}};
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        String tableName = getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ImpalaMetadataHandler.GET_METADATA_QUERY + tableName)).thenReturn(preparestatement1);
        final String getPartitionDetailsSql = "show files in "  + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase();
        Statement statement1 = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(statement1);
        ResultSet resultSet1 = mockResultSet(columns2, types2, values1, new AtomicInteger(-1));
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);
        Mockito.when(statement1.executeQuery(getPartitionDetailsSql)).thenReturn(resultSet1);
        GetTableLayoutResponse getTableLayoutResponse = this.impalaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, tempTableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.impalaMetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);
        assertEquals(2, getSplitsResponse.getSplits().size());
    }

    @Test
    public void testDoGetSplits_withQueryPassthrough()
    {
        // Setup test data
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toSet());

        Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<String, String>()
                .put(QUERY, "SELECT * FROM testSchema.testTable WHERE testCol1 = 1")
                .put(SCHEMA_FUNCTION_NAME, "system.query")
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", "query")
                .put("schema", "system")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        // Create the request objects
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

        // Execute the method
        GetSplitsResponse getSplitsResponse = this.impalaMetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        // Verify that exactly one split was returned for query passthrough
        assertEquals(1, getSplitsResponse.getSplits().size());
        assertEquals(TEST_CATALOG, getSplitsResponse.getCatalogName());
    }

    @Test
    public void testDoGetSplits_withContinuationToken()
    {
        // Setup test data
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toSet());

        Constraints constraints =  new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        // Create block with partitions
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(TEST_PARTITION, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType());
        Block partitionsBlock = blockAllocator.createBlock(schemaBuilder.build());

        // Add two partitions - we'll request the second one using continuation token
        partitionsBlock.setValue(TEST_PARTITION, 0, "partition_0");
        partitionsBlock.setValue(TEST_PARTITION, 1, "partition_1");
        partitionsBlock.setRowCount(2);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(
                this.federatedIdentity,
                TEST_QUERY_ID,
                TEST_CATALOG,
                tableName,
                partitionsBlock,
                new ArrayList<>(partitionCols),
                constraints,
                "0"); // Request continuation from first partition

        // Execute the method
        GetSplitsResponse getSplitsResponse = this.impalaMetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        // Verify that both partitions are returned
        assertEquals(2, getSplitsResponse.getSplits().size());
        Set<String> expectedPartitions = new HashSet<>(Arrays.asList("partition_0", "partition_1"));
        Set<String> actualPartitions = getSplitsResponse.getSplits().stream()
                .map(split -> split.getProperties().get(TEST_PARTITION))
                .collect(Collectors.toSet());
        assertEquals(expectedPartitions, actualPartitions);
        assertEquals(TEST_CATALOG, getSplitsResponse.getCatalogName());
    }

    @Test
    public void decodeContinuationToken() throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Constraints constraints = Mockito.mock(Constraints.class);
        Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG,
                tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.impalaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID,
                TEST_CATALOG, tableName, getTableLayoutResponse.getPartitions(),
                new ArrayList<>(partitionCols), constraints, "1");

        Integer splitRequestToken = 0;
        if (getSplitsRequest.hasContinuationToken()) {
            splitRequestToken = Integer.valueOf(getSplitsRequest.getContinuationToken());
        }

        assertNotNull(splitRequestToken.toString());
    }

    @Test
    public void doGetTable() throws Exception
    {
        String[] metadataSchema = {"type", "name"};
        Object[][] metadataValues = {
                {"INTEGER", "case_number"},
                {"VARCHAR", "case_location"},
                {"TIMESTAMP", "case_instance"},
                {"DATE", "case_date"},
                {"BINARY", "case_binary"},
                {"DOUBLE", "case_double"},
                {"FLOAT", "case_float"},
                {"BOOLEAN", "case_boolean"},
                {"INTERVAL", "case_unsupported"} // should fallback to VARCHAR for unsupported type
        };
        AtomicInteger metadataIndex = new AtomicInteger(-1);
        ResultSet metadataResultSet = mockResultSet(metadataSchema, metadataValues, metadataIndex);

        // Column schema result set returned from getColumns()
        String[] columnSchema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] columnValues = {
                {Types.INTEGER, 12, "case_number", 0, 0},
                {Types.VARCHAR, 25, "case_location", 0, 0},
                {Types.TIMESTAMP, 93, "case_instance", 0, 0},
                {Types.DATE, 91, "case_date", 0, 0},
                {Types.VARBINARY, 91, "case_binary", 0, 0},
                {Types.DOUBLE, 91, "case_double", 0, 0},
                {Types.FLOAT, 91, "case_float", 0, 0},
                {Types.BOOLEAN, 1, "case_boolean", 0, 0},
                {Types.OTHER, 12, "case_unsupported", 0, 0}
        };
        ResultSet columnResultSet = mockResultSet(columnSchema, columnValues, new AtomicInteger(-1));

        // Mocking connection and metadata behavior
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(
                ImpalaMetadataHandler.GET_METADATA_QUERY + inputTableName.getQualifiedTableName().toUpperCase()
        )).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(metadataResultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        Mockito.when(this.connection.getMetaData().getColumns(
                TEST_CATALOG, inputTableName.getSchemaName(), inputTableName.getTableName(), null
        )).thenReturn(columnResultSet);
        Mockito.when(this.connection.getCatalog()).thenReturn(TEST_CATALOG);

        // Execute
        GetTableResponse getTableResponse = this.impalaMetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));

        // Validate
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());

        assertEquals(org.apache.arrow.vector.types.Types.MinorType.INT.getType(), getTableResponse.getSchema().findField("case_number").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), getTableResponse.getSchema().findField("case_location").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType(), getTableResponse.getSchema().findField("case_instance").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType(), getTableResponse.getSchema().findField("case_date").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType(), getTableResponse.getSchema().findField("case_binary").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType(), getTableResponse.getSchema().findField("case_double").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType(), getTableResponse.getSchema().findField("case_float").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.BIT.getType(), getTableResponse.getSchema().findField("case_boolean").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), getTableResponse.getSchema().findField("case_unsupported").getType());
    }

    @Test
    public void doGetTableNoColumns() throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        this.impalaMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.impalaMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test
    public void testDoGetDataSourceCapabilities()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);
        GetDataSourceCapabilitiesResponse response = impalaMetadataHandler.doGetDataSourceCapabilities(blockAllocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        // filter pushdown
        List<OptimizationSubType> filterPushdowns = capabilities.get(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.getOptimization());
        assertNotNull("Filter pushdown capabilities should be present", filterPushdowns);

        List<String> filterSubTypes = filterPushdowns.stream()
                .map(OptimizationSubType::getSubType)
                .collect(Collectors.toList());
        assertTrue("Should support sorted range set pushdown",
                filterSubTypes.contains(FilterPushdownSubType.SORTED_RANGE_SET.getSubType()));
        assertTrue("Should support nullable comparison pushdown",
                filterSubTypes.contains(FilterPushdownSubType.NULLABLE_COMPARISON.getSubType()));

        // complex expression pushdown
        List<OptimizationSubType> complexExprPushdowns = capabilities.get(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.getOptimization());
        assertNotNull("Complex expression pushdown capabilities should be present", complexExprPushdowns);
        OptimizationSubType complexSubType = complexExprPushdowns.get(0);
        assertEquals("Complex expression subtype should be supported function expression types",
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES.getSubType(), complexSubType.getSubType());
        List<String> supportedFunctions = complexSubType.getProperties();
        assertFalse("NULLIF function should not be supported",
                supportedFunctions.contains("NULLIF"));
        assertFalse("IS_DISTINCT_FROM operator should not be supported",
                supportedFunctions.contains("IS_DISTINCT_FROM"));

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

    @Test
    public void testDoGetTableLayout_withViewTable() throws Exception
    {
        // Setup metadata with VIEW table type
        String[] schema = {"type", "name"};
        Object[][] values = {{"VIEW", "TableType"}, {"INTEGER", "case_number"}, {"VARCHAR", "case_location"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tempTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.impalaMetadataHandler.getPartitionSchema(TEST_CATALOG);
        Set<String> partitionCols = new HashSet<>(List.of(TEST_PARTITION));
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID,
                TEST_CATALOG, tempTableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparestatement1 = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(ImpalaMetadataHandler.GET_METADATA_QUERY + tempTableName.getQualifiedTableName().toUpperCase())).thenReturn(preparestatement1);
        Mockito.when(preparestatement1.executeQuery()).thenReturn(resultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.impalaMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }

        // Should return ALL_PARTITIONS when tableType is not null
        assertEquals(List.of("[partition : *]"), expectedValues);
        assertEquals(tempTableName, getTableLayoutResponse.getTableName());
    }
}
