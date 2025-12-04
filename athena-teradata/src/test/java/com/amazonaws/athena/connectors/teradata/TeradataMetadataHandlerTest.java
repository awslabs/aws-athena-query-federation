/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SimpleBlockWriter;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

import static org.mockito.ArgumentMatchers.nullable;

public class TeradataMetadataHandlerTest
        extends TestBase
{
    // Test constants
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_CATALOG_NAME = "testCatalogName";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_PARTITION_COLUMN = "partition";
    private static final String TEST_PARTITION_P0 = "p0";
    private static final String TEST_PARTITION_P1 = "p1";
    private static final String TEST_COLUMN_1 = "testCol1";
    private static final String TEST_COLUMN_2 = "testCol2";
    private static final String TEST_COLUMN_3 = "testCol3";
    private static final String TEST_SECRET_ID = "testSecret";
    private static final String TEST_USER = "testUser";
    private static final String TEST_PASSWORD = "testPassword";
    private static final String TEST_SECRET_JSON = "{\"username\": \"" + TEST_USER + "\", \"password\": \"" + TEST_PASSWORD + "\"}";
    private static final String PARTITION_COUNT_CONFIG_KEY = "partitioncount";
    private static final String PARTITION_COUNT_CONFIG_VALUE = "1000";
    private static final String PARTITION_COUNT_CONFIG_KEY_ALT = "partition_count";
    private static final String PARTITION_COUNT_CONFIG_VALUE_ALT = "500";
    private static final String PARTITION_COUNT_EXCEEDING = "2000";
    private static final String PARTITION_COUNT_BELOW_LIMIT = "100";
    private static final String QUERY_PARAMETER_ONE = "1";
    private static final String ALL_PARTITIONS = "*";
    private static final String VIEW_KIND = "V";
    private static final String TABLE_KIND_COLUMN = "tablekind";
    private static final String PARTITION_COUNT_COLUMN = "partition_count";
    private static final String DATA_TYPE_COLUMN = "DATA_TYPE";
    private static final String COLUMN_SIZE_COLUMN = "COLUMN_SIZE";
    private static final String COLUMN_NAME_COLUMN = "COLUMN_NAME";
    private static final String DECIMAL_DIGITS_COLUMN = "DECIMAL_DIGITS";
    private static final String NUM_PREC_RADIX_COLUMN = "NUM_PREC_RADIX";
    private static final String TYPE_NAME_COLUMN = "TYPE_NAME";
    private static final String DECIMAL_TYPE_NAME = "DECIMAL";
    private static final String OTHER_TYPE_NAME = "OTHER";
    private static final String VIEW_CHECK_FAILED_MESSAGE = "View check failed";
    private static final String INVALID_PARTITION_FIELD_MESSAGE = "Invalid Partition field.";
    private static final String SUPPORTS_FILTER_PUSHDOWN = "supports_filter_pushdown";
    private static final String SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN = "supports_complex_expression_pushdown";
    private static final String SUPPORTS_TOP_N_PUSHDOWN = "supports_top_n_pushdown";
    private static final String SORTED_RANGE_SET = "sorted_range_set";
    private static final String NULLABLE_COMPARISON = "nullable_comparison";
    private static final String SUPPORTED_FUNCTION_EXPRESSION_TYPES = "supported_function_expression_types";
    private static final String EXPECTED_PARTITION_P0_ROW = "[partition : p0]";
    private static final String EXPECTED_PARTITION_P1_ROW = "[partition : p1]";
    private static final String EXPECTED_PARTITION_ALL_ROW = "[partition : *]";
    private static final String PARTITION_PREFIX = "p";
    private static final String JDBC_URL = "teradata://jdbc:teradata://hostname/user=xxx&password=xxx";
    private static final String CONTINUATION_TOKEN_ONE = "1";
    private static final int MAX_SPLITS_COUNT = 1000_000;
    private static final int MAX_SPLITS_COUNT_PLUS_ONE = 1000_001;

    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(TEST_PARTITION_COLUMN, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, TeradataConstants.TERADATA_NAME, JDBC_URL);
    private TeradataMetadataHandler teradataMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator blockAllocator;
    private Schema partitionSchema;
    private QueryStatusChecker queryStatusChecker;

    @Before
    public void setup() throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(TEST_SECRET_ID).build()))).thenReturn(GetSecretValueResponse.builder().secretString(TEST_SECRET_JSON).build());
        this.teradataMetadataHandler = new TeradataMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(PARTITION_COUNT_CONFIG_KEY, PARTITION_COUNT_CONFIG_VALUE));
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = new BlockAllocatorImpl();
        this.partitionSchema = this.teradataMetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
    }

    @After
    public void tearDown() throws Exception
    {
        if (this.blockAllocator != null) {
            this.blockAllocator.close();
        }
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.teradataMetadataHandler.getPartitionSchema(TEST_CATALOG_NAME));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.teradataMetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> partitionCols = new HashSet<>(Arrays.asList(TEST_PARTITION_COLUMN)); //partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        String GET_PARTITIONS_QUERY = "Select DISTINCT " + TEST_PARTITION_COLUMN + " FROM " + getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName() + " where 1= ?";
        Mockito.when(this.connection.prepareStatement(GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {TEST_PARTITION_COLUMN};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{TEST_PARTITION_P0}, {TEST_PARTITION_P1}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);
        GetTableLayoutResponse getTableLayoutResponse = this.teradataMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(Arrays.asList(EXPECTED_PARTITION_P0_ROW, EXPECTED_PARTITION_P1_ROW), expectedValues);
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, QUERY_PARAMETER_ONE);
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.teradataMetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        String GET_PARTITIONS_QUERY = "Select DISTINCT " + TEST_PARTITION_COLUMN + " FROM " + getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName() + " where 1= ?";
        Mockito.when(this.connection.prepareStatement(GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {TEST_PARTITION_COLUMN};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.teradataMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        Assert.assertEquals(expectedValues, Collections.singletonList(EXPECTED_PARTITION_ALL_ROW));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, QUERY_PARAMETER_ONE);
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.teradataMetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        TeradataMetadataHandler teradataMetadataHandler = new TeradataMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        teradataMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        String[] columns = {TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.teradataMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        String GET_PARTITIONS_QUERY = "Select DISTINCT " + TEST_PARTITION_COLUMN + " FROM " + getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName() + " where 1= ?";
        Mockito.when(this.connection.prepareStatement(GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        GetTableLayoutResponse getTableLayoutResponse = this.teradataMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.teradataMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION_P0));
        expectedSplits.add(Collections.singletonMap(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION_P1));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsContinuation()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema partitionSchema = this.teradataMetadataHandler.getPartitionSchema(TEST_CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        String GET_PARTITIONS_QUERY = "Select DISTINCT " + TEST_PARTITION_COLUMN + " FROM " + getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName() + " where 1= ?";
        Mockito.when(this.connection.prepareStatement(GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {TEST_PARTITION_COLUMN};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{TEST_PARTITION_P0}, {TEST_PARTITION_P1}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.teradataMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, CONTINUATION_TOKEN_ONE);
        GetSplitsResponse getSplitsResponse = this.teradataMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(TEST_PARTITION_COLUMN, TEST_PARTITION_P1));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsForView()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement viewCheckPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(TeradataMetadataHandler.VIEW_CHECK_QUERY)).thenReturn(viewCheckPreparedStatement);
        ResultSet viewCheckqueryResultSet = mockResultSet(new String[]{TABLE_KIND_COLUMN}, new int[]{Types.VARCHAR}, new Object[][]{{VIEW_KIND}}, new AtomicInteger(-1));
        Mockito.when(viewCheckPreparedStatement.executeQuery()).thenReturn(viewCheckqueryResultSet);

        Schema partitionSchema = this.teradataMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);
        GetTableLayoutResponse getTableLayoutResponse = this.teradataMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.teradataMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);
        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, ALL_PARTITIONS));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String[] schema = {DATA_TYPE_COLUMN, COLUMN_SIZE_COLUMN, COLUMN_NAME_COLUMN, DECIMAL_DIGITS_COLUMN, NUM_PREC_RADIX_COLUMN};
        Object[][] values = {{Types.INTEGER, 12, TEST_COLUMN_1, 0, 0}, {Types.VARCHAR, 25, TEST_COLUMN_2, 0, 0},
                {Types.TIMESTAMP, 93, TEST_COLUMN_3, 0, 0}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COLUMN_1, org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COLUMN_2, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COLUMN_3, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Mockito.when(connection.getMetaData().getColumns(TEST_CATALOG, inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn(TEST_CATALOG);

        GetTableResponse getTableResponse = this.teradataMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals(TEST_CATALOG, getTableResponse.getCatalogName());
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Mockito.when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.teradataMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableNoColumns() throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        this.teradataMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doGetDataSourceCapabilities_returnsExpectedCapabilities()
    {
        BlockAllocator allocator = new BlockAllocatorImpl();
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(federatedIdentity, TEST_QUERY_ID, TEST_CATALOG);

        GetDataSourceCapabilitiesResponse response =
                teradataMetadataHandler.doGetDataSourceCapabilities(allocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        Assert.assertEquals(TEST_CATALOG, response.getCatalogName());

        // Filter pushdown
        List<OptimizationSubType> filterPushdown = capabilities.get(SUPPORTS_FILTER_PUSHDOWN);
        Assert.assertNotNull("Expected supports_filter_pushdown capability to be present", filterPushdown);
        Assert.assertEquals(2, filterPushdown.size());
        Assert.assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals(SORTED_RANGE_SET)));
        Assert.assertTrue(filterPushdown.stream().anyMatch(subType -> subType.getSubType().equals(NULLABLE_COMPARISON)));

        // Complex expression pushdown
        List<OptimizationSubType> complexPushdown = capabilities.get(SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN);
        Assert.assertNotNull("Expected supports_complex_expression_pushdown capability to be present", complexPushdown);
        Assert.assertEquals(1, complexPushdown.size());
        OptimizationSubType complexSubType = complexPushdown.get(0);
        Assert.assertEquals(SUPPORTED_FUNCTION_EXPRESSION_TYPES, complexSubType.getSubType());
        Assert.assertNotNull("Expected function expression types to be present", complexSubType.getProperties());
        Assert.assertFalse("Expected function expression types to be non-empty", complexSubType.getProperties().isEmpty());

        // Top-N pushdown
        List<OptimizationSubType> topNPushdown = capabilities.get(SUPPORTS_TOP_N_PUSHDOWN);
        Assert.assertNotNull("Expected supports_top_n_pushdown capability to be present", topNPushdown);
        Assert.assertEquals(1, topNPushdown.size());
    }

    @Test(expected = SQLException.class)
    public void getPartitions_withViewCheckException_throwsSQLException() throws Exception
    {
        Block block = blockAllocator.createBlock(partitionSchema);
        BlockWriter blockWriter = new SimpleBlockWriter(block);
        GetTableLayoutRequest getTableLayoutRequest = createGetTableLayoutRequest(blockAllocator);

        PreparedStatement viewCheckPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(TeradataMetadataHandler.VIEW_CHECK_QUERY)).thenReturn(viewCheckPreparedStatement);
        Mockito.when(viewCheckPreparedStatement.executeQuery()).thenThrow(new SQLException(VIEW_CHECK_FAILED_MESSAGE));

        this.teradataMetadataHandler.getPartitions(blockWriter, getTableLayoutRequest, queryStatusChecker);
    }

    @Test
    public void getPartitions_withNonPartitionApproach_returnsSinglePartition() throws Exception
    {
        Block block = blockAllocator.createBlock(partitionSchema);
        BlockWriter blockWriter = new SimpleBlockWriter(block);
        GetTableLayoutRequest getTableLayoutRequest = createGetTableLayoutRequest(blockAllocator);

        mockViewCheckReturnsTable();
        mockPartitionCountQuery(getTableLayoutRequest, PARTITION_COUNT_EXCEEDING);

        TeradataMetadataHandler handlerWithLowLimit = new TeradataMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(PARTITION_COUNT_CONFIG_KEY, PARTITION_COUNT_CONFIG_VALUE));
        handlerWithLowLimit.getPartitions(blockWriter, getTableLayoutRequest, queryStatusChecker);

        Assert.assertEquals(1, block.getRowCount());
    }

    @Test
    public void getPartitions_withPartitionCountConfig_returnsMultiplePartitions() throws Exception
    {
        Block block = blockAllocator.createBlock(partitionSchema);
        BlockWriter blockWriter = new SimpleBlockWriter(block);
        GetTableLayoutRequest getTableLayoutRequest = createGetTableLayoutRequest(blockAllocator);

        mockViewCheckReturnsTable();
        mockPartitionCountQuery(getTableLayoutRequest, PARTITION_COUNT_BELOW_LIMIT);

        TeradataMetadataHandler handlerWithPartitionCount = new TeradataMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(PARTITION_COUNT_CONFIG_KEY_ALT, PARTITION_COUNT_CONFIG_VALUE_ALT));
        mockPartitionDetailsQuery(getTableLayoutRequest, new Object[][]{{TEST_PARTITION_P0}, {TEST_PARTITION_P1}});

        handlerWithPartitionCount.getPartitions(blockWriter, getTableLayoutRequest, queryStatusChecker);

        Assert.assertEquals(2, block.getRowCount());
    }

    @Test
    public void getPartitions_withInvalidPartitionField_returnsSinglePartition() throws Exception
    {
        Block block = blockAllocator.createBlock(partitionSchema);
        BlockWriter blockWriter = new SimpleBlockWriter(block);
        GetTableLayoutRequest getTableLayoutRequest = createGetTableLayoutRequest(blockAllocator);

        mockViewCheckReturnsTable();
        mockPartitionCountQuery(getTableLayoutRequest, PARTITION_COUNT_BELOW_LIMIT);
        mockPartitionDetailsQueryWithException(getTableLayoutRequest, new RuntimeException(INVALID_PARTITION_FIELD_MESSAGE));

        this.teradataMetadataHandler.getPartitions(blockWriter, getTableLayoutRequest, queryStatusChecker);

        Assert.assertEquals(1, block.getRowCount());
    }

    @Test
    public void doGetSplits_withQueryPassthrough_returnsSingleSplit() throws Exception
    {
        GetSplitsRequest getSplitsRequest = createGetSplitsRequest(blockAllocator, true);

        GetSplitsResponse getSplitsResponse = this.teradataMetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        Assert.assertNotNull(getSplitsResponse);
        Assert.assertEquals(TEST_CATALOG_NAME, getSplitsResponse.getCatalogName());
        Assert.assertNotNull(getSplitsResponse.getSplits());
        Assert.assertEquals(1, getSplitsResponse.getSplits().size());
    }

    @Test
    public void doGetSplits_withMaxSplitsPerRequest_returnsContinuationToken() throws Exception
    {
        GetSplitsRequest getSplitsRequest = createGetSplitsRequest(blockAllocator, false);

        // Create a block with more partitions than MAX_SPLITS_PER_REQUEST
        populatePartitionsBlock(getSplitsRequest.getPartitions(), MAX_SPLITS_COUNT_PLUS_ONE);

        GetSplitsResponse getSplitsResponse = this.teradataMetadataHandler.doGetSplits(blockAllocator, getSplitsRequest);

        Assert.assertNotNull(getSplitsResponse);
        Assert.assertEquals(MAX_SPLITS_COUNT, getSplitsResponse.getSplits().size());
        Assert.assertNotNull(getSplitsResponse.getContinuationToken());
    }

    @Test
    public void doGetTable_withDecimalType_convertsSuccessfully() throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Object[][] values = {{Types.DECIMAL, 10, TEST_COLUMN_1, 0, 10, DECIMAL_TYPE_NAME}};
        mockGetColumnsResultSet(inputTableName, values);
        GetTableRequest request = new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap());

        GetTableResponse getTableResponse = this.teradataMetadataHandler.doGetTable(this.blockAllocator, request);

        Assert.assertNotNull(getTableResponse);
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void doGetTable_withUnsupportedType_throwsUnsupportedOperationException() throws Exception
    {
        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Object[][] values = {{Types.OTHER, 10, TEST_COLUMN_1, 0, 10, OTHER_TYPE_NAME}};
        mockGetColumnsResultSet(inputTableName, values);
        GetTableRequest request = new GetTableRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG, inputTableName, Collections.emptyMap());

        this.teradataMetadataHandler.doGetTable(this.blockAllocator, request);
    }

    @Test
    public void toArrowType_withDateType_returnsDateArrowType()
    {
        ArrowType arrowType = TeradataMetadataHandler.toArrowType(Types.DATE, 0, 0);
        Assert.assertNotNull(arrowType);
        Assert.assertTrue(arrowType instanceof ArrowType.Date);
        Assert.assertEquals(DateUnit.DAY, ((ArrowType.Date) arrowType).getUnit());
    }

    @Test
    public void toArrowType_withOtherTypes_returnsCorrectTypes()
    {
        ArrowType intType = TeradataMetadataHandler.toArrowType(Types.INTEGER, 0, 0);
        Assert.assertNotNull(intType);

        ArrowType varcharType = TeradataMetadataHandler.toArrowType(Types.VARCHAR, 0, 0);
        Assert.assertNotNull(varcharType);
    }


    private void populatePartitionsBlock(Block partitions, int partitionCount)
    {
        for (int i = 0; i < partitionCount; i++) {
            partitions.setValue(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, i, PARTITION_PREFIX + i);
        }
        partitions.setRowCount(partitionCount);
    }

    private GetTableLayoutRequest createGetTableLayoutRequest(BlockAllocator blockAllocator) throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        return new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);
    }

    private GetSplitsRequest createGetSplitsRequest(BlockAllocator blockAllocator, boolean isQueryPassThrough) throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.isQueryPassThrough()).thenReturn(isQueryPassThrough);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        Block partitions = blockAllocator.createBlock(partitionSchema);
        return new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, partitions, new ArrayList<>(partitionCols), constraints, null);
    }


    private void mockViewCheckReturnsTable() throws SQLException
    {
        PreparedStatement viewCheckPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(TeradataMetadataHandler.VIEW_CHECK_QUERY)).thenReturn(viewCheckPreparedStatement);
        ResultSet viewCheckResultSet = mockResultSet(new String[]{TABLE_KIND_COLUMN}, new int[]{Types.VARCHAR}, new Object[][]{{}}, new AtomicInteger(-1));
        Mockito.when(viewCheckPreparedStatement.executeQuery()).thenReturn(viewCheckResultSet);
    }

    private String buildPartitionCountQuery(GetTableLayoutRequest request)
    {
        return "Select  count(distinct " + TEST_PARTITION_COLUMN + " ) as " + PARTITION_COUNT_COLUMN + " FROM " + request.getTableName().getSchemaName() + "." +
                request.getTableName().getTableName() + " where 1= ?";
    }

    private void mockPartitionCountQuery(GetTableLayoutRequest request, String countValue) throws SQLException
    {
        String getPartitionsCountQuery = buildPartitionCountQuery(request);
        PreparedStatement partitionCountPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(getPartitionsCountQuery)).thenReturn(partitionCountPreparedStatement);
        String[] countColumns = {PARTITION_COUNT_COLUMN};
        int[] countTypes = {Types.VARCHAR};
        Object[][] countValues = {{countValue}};
        ResultSet countResultSet = mockResultSet(countColumns, countTypes, countValues, new AtomicInteger(-1));
        Mockito.when(partitionCountPreparedStatement.executeQuery()).thenReturn(countResultSet);
    }

    private String buildPartitionDetailsQuery(GetTableLayoutRequest request)
    {
        return "Select DISTINCT " + TEST_PARTITION_COLUMN + " FROM " + request.getTableName().getSchemaName() + "." +
                request.getTableName().getTableName() + " where 1= ?";
    }

    private void mockPartitionDetailsQuery(GetTableLayoutRequest request, Object[][] values) throws SQLException
    {
        String getPartitionsQuery = buildPartitionDetailsQuery(request);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(getPartitionsQuery)).thenReturn(preparedStatement);
        String[] columns = {TEST_PARTITION_COLUMN};
        int[] types = {Types.VARCHAR};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    private void mockPartitionDetailsQueryWithException(GetTableLayoutRequest request, RuntimeException exception) throws SQLException
    {
        String getPartitionsQuery = buildPartitionDetailsQuery(request);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(getPartitionsQuery)).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.executeQuery()).thenThrow(exception);
    }

    private void mockGetColumnsResultSet(TableName tableName, Object[][] values) throws SQLException
    {
        String[] schema = {DATA_TYPE_COLUMN, COLUMN_SIZE_COLUMN, COLUMN_NAME_COLUMN, DECIMAL_DIGITS_COLUMN, NUM_PREC_RADIX_COLUMN, TYPE_NAME_COLUMN};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Mockito.when(connection.getMetaData().getColumns(TEST_CATALOG, tableName.getSchemaName(), tableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn(TEST_CATALOG);
    }
}
