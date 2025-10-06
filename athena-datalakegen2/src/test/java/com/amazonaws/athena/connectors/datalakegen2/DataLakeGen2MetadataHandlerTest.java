/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
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
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.datalakegen2.resolver.DataLakeGen2CaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2MetadataHandler.PARTITION_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DataLakeGen2MetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DataLakeGen2MetadataHandlerTest.class);
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", DataLakeGen2Constants.NAME,
    		  "datalakegentwo://jdbc:sqlserver://hostname;databaseName=fakedatabase");
    private DataLakeGen2MetadataHandler dataLakeGen2MetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
            throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.jdbcConnectionFactory = mock(JdbcConnectionFactory.class, RETURNS_DEEP_STUBS);
        this.connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        logger.info(" this.connection.."+ this.connection);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = mock(SecretsManagerClient.class);
        this.athena = mock(AthenaClient.class);
        when(this.secretsManager.getSecretValue(eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"user\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.dataLakeGen2MetadataHandler = new DataLakeGen2MetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME));
        this.federatedIdentity = mock(FederatedIdentity.class);
    }

    @Test
    public void getPartitionSchema()
    {
        assertEquals(SchemaBuilder.newBuilder()
                        .addField(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        Schema partitionSchema = this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.dataLakeGen2MetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);
        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
        }

        assertEquals(Collections.singletonList("[partition_number : 0]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();

        assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
        assertEquals(tableName, getTableLayoutResponse.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = mock(JdbcConnectionFactory.class);
        when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);
        when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        DataLakeGen2MetadataHandler dataLakeGen2MetadataHandler = new DataLakeGen2MetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of(), new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME));

        dataLakeGen2MetadataHandler.doGetTableLayout(mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplitsWithNoPartition()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = mock(Constraints.class);
        TableName tableName = new TableName("testSchema", "testTable");

        Schema partitionSchema = this.dataLakeGen2MetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

        GetTableLayoutResponse getTableLayoutResponse = this.dataLakeGen2MetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
        GetSplitsResponse getSplitsResponse = this.dataLakeGen2MetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(PARTITION_NUMBER, "0"));
        assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
        assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{java.sql.Types.INTEGER, 12, "testCol1", 0, 0}, {java.sql.Types.VARCHAR, 25, "testCol2", 0, 0},
                {java.sql.Types.TIMESTAMP, 93, "testCol3", 0, 0}, {java.sql.Types.TIMESTAMP_WITH_TIMEZONE, 93, "testCol4", 0, 0}};
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
        when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        when(connection.getCatalog()).thenReturn("testCatalog");
        when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://hostname;databaseName=fakedatabase");
        
        // Mock the connection's setAutoCommit method
        when(connection.getAutoCommit()).thenReturn(true);
        
        // Mock the data type query result set
        String[] dataTypeSchema = {"COLUMN_NAME", "DATA_TYPE", "PRECISION", "SCALE"};
        Object[][] dataTypeValues = {{"testCol1", "int", 10, 0}, {"testCol2", "varchar", 255, 0}, {"testCol3", "datetime", 23, 3}, {"testCol4", "datetimeoffset", 34, 7}};
        AtomicInteger dataTypeRowNumber = new AtomicInteger(-1);
        ResultSet dataTypeResultSet = mockResultSet(dataTypeSchema, dataTypeValues, dataTypeRowNumber);
        
        // Mock the prepared statement and its execution
        java.sql.PreparedStatement mockPreparedStatement = mock(java.sql.PreparedStatement.class);
        when(connection.prepareStatement(any(String.class))).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(dataTypeResultSet);
        GetTableResponse getTableResponse = this.dataLakeGen2MetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        assertEquals(expected, getTableResponse.getSchema());
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void testGetSchemaWithAzureServerlessEnvironment()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();

        // Mock the data type query result set for Azure serverless with all supported data types
        String[] dataTypeSchema = {"COLUMN_NAME", "DATA_TYPE", "PRECISION", "SCALE"};
        Object[][] dataTypeValues = {
            // Primary Key
            {"id", "int", 10, 0},
            
            // Integer Types
            {"small_int_col", "smallint", 5, 0},
            {"tiny_int_col", "tinyint", 3, 0},
            {"big_int_col", "bigint", 19, 0},
            
            // Decimal/Numeric Types
            {"decimal_col", "decimal", 18, 2},
            {"numeric_col", "numeric", 18, 2},
            {"money_col", "money", 19, 4},
            {"small_money_col", "smallmoney", 10, 4},
            
            // Floating Point Types
            {"float_col", "float", 53, 0},
            {"real_col", "real", 24, 0},
            
            // Character Types
            {"char_col", "char", 10, 0},
            {"varchar_col", "varchar", 255, 0},
            {"nchar_col", "nchar", 10, 0},
            {"nvarchar_col", "nvarchar", 255, 0},
            
            // Binary Types
            {"binary_col", "binary", 16, 0},
            {"varbinary_col", "varbinary", 255, 0},
            
            // Date and Time Types
            {"date_col", "date", 10, 0},
            {"time_col", "time", 16, 7},
            {"datetime_col", "datetime", 23, 3},
            {"datetime2_col", "datetime2", 27, 7},
            {"smalldatetime_col", "smalldatetime", 16, 0},
            {"datetimeoffset_col", "datetimeoffset", 34, 7},
            
            // Special Types
            {"uniqueidentifier_col", "uniqueidentifier", 36, 0},
            
            // Boolean
            {"bit_col", "bit", 1, 0}
        };
        AtomicInteger dataTypeRowNumber = new AtomicInteger(-1);
        ResultSet dataTypeResultSet = mockResultSet(dataTypeSchema, dataTypeValues, dataTypeRowNumber);

        // Mock the prepared statement and its execution
        java.sql.PreparedStatement mockPreparedStatement = mock(java.sql.PreparedStatement.class);
        when(connection.prepareStatement(any(String.class))).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(dataTypeResultSet);

        // Mock Azure serverless URL
        when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://myworkspace-ondemand.sql.azuresynapse.net:1433;database=mydatabase;");
        when(connection.getCatalog()).thenReturn("testCatalog");

        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        GetTableResponse getTableResponse = this.dataLakeGen2MetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        // Verify the response
        assertNotNull(getTableResponse);
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals("testCatalog", getTableResponse.getCatalogName());
        
        // Verify that the schema was built using the direct SQL query approach (Azure serverless path)
        Schema responseSchema = getTableResponse.getSchema();
        assertNotNull(responseSchema);
        assertEquals(25, responseSchema.getFields().size()); // 24 columns from our mock data + 1 partition field
        
        // Verify specific data types are correctly mapped
        List<Field> fields = responseSchema.getFields();
        
        // Verify integer types (based on actual DataLakeGen2MetadataHandler mappings)
        assertTrue("Should contain INT field", fields.stream().anyMatch(f -> f.getName().equals("id") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.INT.getType())));
        assertTrue("Should contain SMALLINT field", fields.stream().anyMatch(f -> f.getName().equals("small_int_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType())));
        assertTrue("Should contain TINYINT field", fields.stream().anyMatch(f -> f.getName().equals("tiny_int_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType())));
        assertTrue("Should contain BIGINT field", fields.stream().anyMatch(f -> f.getName().equals("big_int_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType())));
        
        // Verify decimal types (mapped to DECIMAL in DataLakeGen2MetadataHandler)
        assertTrue("Should contain DECIMAL field", fields.stream().anyMatch(f -> f.getName().equals("decimal_col") && f.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Decimal));
        assertTrue("Should contain NUMERIC field (mapped to FLOAT8)", fields.stream().anyMatch(f -> f.getName().equals("numeric_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType())));
        
        // Verify floating point types
        assertTrue("Should contain FLOAT field", fields.stream().anyMatch(f -> f.getName().equals("float_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType())));
        assertTrue("Should contain REAL field", fields.stream().anyMatch(f -> f.getName().equals("real_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType())));
        
        // Verify character types (all mapped to VARCHAR)
        assertTrue("Should contain CHAR field (mapped to VARCHAR)", fields.stream().anyMatch(f -> f.getName().equals("char_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())));
        assertTrue("Should contain VARCHAR field", fields.stream().anyMatch(f -> f.getName().equals("varchar_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())));
        assertTrue("Should contain NCHAR field (mapped to VARCHAR)", fields.stream().anyMatch(f -> f.getName().equals("nchar_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())));
        assertTrue("Should contain NVARCHAR field (mapped to VARCHAR)", fields.stream().anyMatch(f -> f.getName().equals("nvarchar_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())));
        
        // Verify binary types (mapped to VARBINARY in DataLakeGen2MetadataHandler)
        assertTrue("Should contain BINARY field (mapped to VARBINARY)", fields.stream().anyMatch(f -> f.getName().equals("binary_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType())));
        assertTrue("Should contain VARBINARY field", fields.stream().anyMatch(f -> f.getName().equals("varbinary_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType())));
        
        
        // Verify date/time types (based on actual mappings)
        assertTrue("Should contain DATE field", fields.stream().anyMatch(f -> f.getName().equals("date_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType())));
        assertTrue("Should contain TIME field (mapped to VARCHAR)", fields.stream().anyMatch(f -> f.getName().equals("time_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())));
        assertTrue("Should contain DATETIME field (mapped to DATEMILLI)", fields.stream().anyMatch(f -> f.getName().equals("datetime_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType())));
        assertTrue("Should contain DATETIME2 field (mapped to DATEMILLI)", fields.stream().anyMatch(f -> f.getName().equals("datetime2_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType())));
        assertTrue("Should contain SMALLDATETIME field (mapped to DATEMILLI)", fields.stream().anyMatch(f -> f.getName().equals("smalldatetime_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType())));
        assertTrue("Should contain DATETIMEOFFSET field (mapped to DATEMILLI)", fields.stream().anyMatch(f -> f.getName().equals("datetimeoffset_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType())));
        
        // Verify special types
        assertTrue("Should contain UNIQUEIDENTIFIER field (mapped to VARCHAR)", fields.stream().anyMatch(f -> f.getName().equals("uniqueidentifier_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())));
        assertTrue("Should contain BIT field (mapped to BIT)", fields.stream().anyMatch(f -> f.getName().equals("bit_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.BIT.getType())));
        
        // Verify money types (mapped to FLOAT8 in DataLakeGen2MetadataHandler)
        assertTrue("Should contain MONEY field (mapped to FLOAT8)", fields.stream().anyMatch(f -> f.getName().equals("money_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType())));
        assertTrue("Should contain SMALLMONEY field (mapped to FLOAT8)", fields.stream().anyMatch(f -> f.getName().equals("small_money_col") && f.getType().equals(org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType())));
    }

    @Test
    public void testGetSchemaWithStandardEnvironment()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();

        // Mock the data type query result set
        String[] dataTypeSchema = {"COLUMN_NAME", "DATA_TYPE", "PRECISION", "SCALE"};
        Object[][] dataTypeValues = {{"testCol1", "int", 10, 0}, {"testCol2", "varchar", 255, 0}};
        AtomicInteger dataTypeRowNumber = new AtomicInteger(-1);
        ResultSet dataTypeResultSet = mockResultSet(dataTypeSchema, dataTypeValues, dataTypeRowNumber);

        // Mock the prepared statement and its execution
        java.sql.PreparedStatement mockPreparedStatement = mock(java.sql.PreparedStatement.class);
        when(connection.prepareStatement(any(String.class))).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(dataTypeResultSet);

        // Mock standard SQL Server URL (non-serverless)
        when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://myserver.database.windows.net:1433;database=mydatabase;");
        when(connection.getCatalog()).thenReturn("testCatalog");

        // Mock the getColumns result set for standard environment
        String[] schema = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE"};
        Object[][] values = {{"testCatalog", "TESTSCHEMA", "TESTTABLE", "testCol1", java.sql.Types.INTEGER, "int", 10, 4, 0, 10, 1, "", "", java.sql.Types.INTEGER, 0, 4, 1, "YES"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet getColumnsResultSet = mockResultSet(schema, values, rowNumber);
        when(connection.getMetaData().getColumns("testCatalog", "TESTSCHEMA", "TESTTABLE", null)).thenReturn(getColumnsResultSet);

        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        GetTableResponse getTableResponse = this.dataLakeGen2MetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        // Verify the response
        assertNotNull(getTableResponse);
        assertEquals(inputTableName, getTableResponse.getTableName());
        assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doListTables() throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String schemaName = "TESTSCHEMA";
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, "queryId", "testCatalog", schemaName, null, 3);

        DatabaseMetaData mockDatabaseMetaData = mock(DatabaseMetaData.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(connection.getMetaData()).thenReturn(mockDatabaseMetaData);
        when(mockDatabaseMetaData.getTables(any(), any(), any(), any())).thenReturn(mockResultSet);

        when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(3)).thenReturn("TESTTABLE").thenReturn("testtable").thenReturn("testTABLE");
        when(mockResultSet.getString(2)).thenReturn(schemaName);

        mockStatic(JDBCUtil.class);
        when(JDBCUtil.getSchemaTableName(mockResultSet)).thenReturn(new TableName("TESTSCHEMA", "TESTTABLE"))
                .thenReturn(new TableName("TESTSCHEMA", "testtable"))
                .thenReturn(new TableName("TESTSCHEMA", "testTABLE"));

        when(this.jdbcConnectionFactory.getConnection(any())).thenReturn(connection);

        ListTablesResponse listTablesResponse = this.dataLakeGen2MetadataHandler.doListTables(blockAllocator, listTablesRequest);

        TableName[] expectedTables = {
                new TableName("TESTSCHEMA", "TESTTABLE"),
                new TableName("TESTSCHEMA", "testTABLE"),
                new TableName("TESTSCHEMA", "testtable")
        };

        assertEquals(Arrays.toString(expectedTables), listTablesResponse.getTables().toString());
    }
}
