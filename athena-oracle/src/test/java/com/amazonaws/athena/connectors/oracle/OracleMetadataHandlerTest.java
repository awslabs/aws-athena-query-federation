/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.Block;
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
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.oracle.resolver.OracleJDBCCaseResolver;
import oracle.jdbc.OracleTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;

public class OracleMetadataHandlerTest
        extends TestBase
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG_NAME = "testCatalog";
    private static final String BASE_CONNECTION_STRING = "oracle://jdbc:oracle:thin:@//testHost:1521/orcl";
    private static final String SECRET_NAME = "testSecret";

    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField("partition_name", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("CATALOG_NAME", ORACLE_NAME,
            BASE_CONNECTION_STRING);
    private OracleMetadataHandler oracleMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator blockAllocator;

    @Before
    public void setup()
            throws Exception
    {
        this.blockAllocator = new BlockAllocatorImpl();
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(SECRET_NAME).build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.oracleMetadataHandler = new OracleMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new OracleJDBCCaseResolver(ORACLE_NAME));
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
    }

    @Test
    public void getPartitionSchema_returnsPartitionSchema()
    {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.oracleMetadataHandler.getPartitionSchema(CATALOG_NAME));
    }

    @Test
    public void doGetTableLayout_withPartitions_returnsPartitionLayout()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "\'TESTTABLE\'");
        Schema partitionSchema = this.oracleMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQUERY_ID", CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"PARTITION_NAME".toLowerCase()};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.oracleMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(expectedValues, Arrays.asList("[partition_name : p0]", "[partition_name : p1]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());

        verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
    }

    @Test
    public void doGetTableLayout_withoutPartitions_returnsDefaultPartition()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "\'TESTTABLE\'");
        Schema partitionSchema = this.oracleMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQUERY_ID", CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"PARTITION_NAME".toLowerCase()};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.oracleMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }
        assertEquals(expectedValues, Collections.singletonList("[partition_name : 0]"));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());

        verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayout_whenSQLExceptionOccurs_throwsRuntimeException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.oracleMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQUERY_ID", CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        OracleMetadataHandler oracleMetadataHandler = new OracleMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new OracleJDBCCaseResolver(ORACLE_NAME));

        oracleMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits_withPartitions_returnsSplitsForAllPartitions()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {OracleMetadataHandler.PARTITION_COLUMN_NAME};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        Schema partitionSchema = this.oracleMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQUERY_ID", CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.oracleMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQUERY_ID", CATALOG_NAME, tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.oracleMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "p0"));
        expectedSplits.add(Collections.singletonMap(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "p1"));
        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplits_withContinuationToken_returnsRemainingSplits()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.oracleMetadataHandler.getPartitionSchema(CATALOG_NAME);
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQUERY_ID", CATALOG_NAME, tableName, constraints, partitionSchema, partitionCols);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

        String[] columns = {"PARTITION_NAME".toLowerCase()};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"p0"}, {"p1"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

        GetTableLayoutResponse getTableLayoutResponse = this.oracleMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQUERY_ID", CATALOG_NAME, tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
        GetSplitsResponse getSplitsResponse = this.oracleMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap("PARTITION_NAME".toLowerCase(), "p1"));
        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplits_withQueryPassthroughArgs_returnsSplitWithPassthroughArgs()
    {
        TableName tableName = new TableName("testSchema", "testTable");

        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put("arg1", "val1");
        passthroughArgs.put("arg2", "val2");
        
        Constraints queryconstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                passthroughArgs,
                null
        );

        Block partitions = Mockito.mock(Block.class);

        GetSplitsRequest request = new GetSplitsRequest(
                federatedIdentity,
                QUERY_ID,
                CATALOG_NAME,
                tableName,
                partitions,
                Collections.emptyList(),
                queryconstraints,
                null
        );

        GetSplitsResponse response = oracleMetadataHandler.doGetSplits(blockAllocator, request);

        // Assertions
        assertEquals(CATALOG_NAME, response.getCatalogName());
        assertEquals(1, response.getSplits().size());

        Map<String, String> actualProps = response.getSplits().iterator().next().getProperties();
        assertEquals(passthroughArgs, actualProps);
    }

    @Test
    public void doListTables_withPagination_returnsPaginatedTables()
            throws Exception
    {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}};
        TableName[] expected = {new TableName("testSchema", "testTable")};
        ResultSet resultSet = mockResultSet(schema, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ListTablesResponse listTablesResponse = this.oracleMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQUERY_ID",
                        "CATALOG_NAME", "testSchema", null, 1));
        assertEquals("1", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());

        preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleMetadataHandler.LIST_PAGINATED_TABLES_QUERY)).thenReturn(preparedStatement);
        Object[][] nextValues = {{"testSchema", "testTable2"}};
        TableName[] nextExpected = {new TableName("testSchema", "testTable2")};
        ResultSet nextResultSet = mockResultSet(schema, nextValues, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(nextResultSet);

        listTablesResponse = this.oracleMetadataHandler.doListTables(
                blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQUERY_ID",
                        "CATALOG_NAME", "testSchema", "1", 1));
        assertEquals("2", listTablesResponse.getNextToken());
        Assert.assertArrayEquals(nextExpected, listTablesResponse.getTables().toArray());
    }

    @Test
    public void doGetTable_returnsTableSchemaWithColumns()
            throws Exception
    {
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {
                {Types.INTEGER, 12, "testCol1", 0, 0},
                {Types.VARCHAR, 25, "testCol2", 0, 0},
                {Types.TIMESTAMP, 7, "testCol3", 0, 0}, // precision = 7, should map to DATEDAY
                {OracleTypes.TIMESTAMPLTZ, 0, "testCol4", 0, 0}, // TIMESTAMP WITH LOCAL TZ → DATEMILLI
                {OracleTypes.TIMESTAMPTZ, 0, "testCol5", 0, 0}, // TIMESTAMP WITH TZ → DATEMILLI
                {Types.NUMERIC, 10, "testCol6", 2, 0}
        };
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol5", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        ArrowType.Decimal testCol6ArrowType = ArrowType.Decimal.createDecimal(10, 2, 128);
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol6", testCol6ArrowType).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        Mockito.when(connection.getMetaData().getColumns("CATALOG_NAME", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("CATALOG_NAME");

        GetTableResponse getTableResponse = this.oracleMetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, "testQUERY_ID", "CATALOG_NAME", inputTableName, Collections.emptyMap()));

        assertEquals(expected, getTableResponse.getSchema());
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals("CATALOG_NAME", getTableResponse.getCatalogName());
    }

    @Test
    public void doGetTable_withUnsupportedColumnTypes_fallsBackToVarchar() throws Exception
    {
            String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};

            Object[][] values = {
                    {Types.CLOB, 0, "clobCol", 0, 0},
                    {Types.SQLXML, 0, "xmlCol", 0, 0}
            };

            AtomicInteger rowNumber = new AtomicInteger(-1);
            ResultSet resultSet = mockResultSet(schema, values, rowNumber);

            SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("clobCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
            expectedSchemaBuilder.addField(FieldBuilder.newBuilder("xmlCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());

            PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
            Schema expected = expectedSchemaBuilder.build();

            TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");

            Mockito.when(connection.getMetaData().getColumns("CATALOG_NAME", inputTableName.getSchemaName(), inputTableName.getTableName(), null))
                    .thenReturn(resultSet);
            Mockito.when(connection.getCatalog()).thenReturn("CATALOG_NAME");

            GetTableResponse getTableResponse = this.oracleMetadataHandler.doGetTable(
                    blockAllocator, new GetTableRequest(this.federatedIdentity, "testQUERY_ID", "CATALOG_NAME", inputTableName, Collections.emptyMap()));

            assertEquals(expected, getTableResponse.getSchema());
            assertEquals(inputTableName, getTableResponse.getTableName());
            assertEquals("CATALOG_NAME", getTableResponse.getCatalogName());
    }

    @Test
    public void doGetDataSourceCapabilities_returnsSupportedCapabilities()
    {
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(federatedIdentity, QUERY_ID, CATALOG_NAME);

        GetDataSourceCapabilitiesResponse response =
                oracleMetadataHandler.doGetDataSourceCapabilities(blockAllocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();

        assertEquals(CATALOG_NAME, response.getCatalogName());

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
        assertTrue(complexPushdown.stream().anyMatch(subType ->
                subType.getSubType().equals("supported_function_expression_types") &&
                        !subType.getProperties().isEmpty()));

        // Top-N pushdown
        List<OptimizationSubType> topNPushdown = capabilities.get("supports_top_n_pushdown");
        assertNotNull("Expected supports_top_n_pushdown capability to be present", topNPushdown);
        assertEquals(1, topNPushdown.size());
    }

    @Test
    public void createCredentialsProvider_withSecret_returnsOracleCredentialsProvider() throws Exception
    {
        DatabaseConnectionConfig configWithSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, ORACLE_NAME,
                BASE_CONNECTION_STRING.replace("@//", "${" + SECRET_NAME + "}@//"), SECRET_NAME);

        CredentialsProvider provider = captureCredentialsProvider(configWithSecret);

        assertNotNull("CredentialsProvider should not be null when secret is configured", provider);
        assertTrue("CredentialsProvider should be an instance of OracleCredentialsProvider",
                provider instanceof OracleCredentialsProvider);
    }

    @Test
    public void createCredentialsProvider_withoutSecret_returnsNull() throws Exception
    {
        DatabaseConnectionConfig configWithoutSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, ORACLE_NAME,
                BASE_CONNECTION_STRING);

        CredentialsProvider provider = captureCredentialsProvider(configWithoutSecret);

        assertNull("CredentialsProvider should be null when no secret is configured", provider);
    }

    /**
     * Captures the CredentialsProvider used by the JDBC connection factory
     */
    private CredentialsProvider captureCredentialsProvider(DatabaseConnectionConfig config) throws Exception
    {
        OracleMetadataHandler handler = new OracleMetadataHandler(
                config, secretsManager, athena, jdbcConnectionFactory,
                com.google.common.collect.ImmutableMap.of(), new OracleJDBCCaseResolver(ORACLE_NAME));

        handler.doListSchemaNames(new BlockAllocatorImpl(),
                new ListSchemasRequest(federatedIdentity, QUERY_ID, CATALOG_NAME));

        ArgumentCaptor<CredentialsProvider> captor = ArgumentCaptor.forClass(CredentialsProvider.class);
        verify(jdbcConnectionFactory).getConnection(captor.capture());
        return captor.getValue();
    }
}
