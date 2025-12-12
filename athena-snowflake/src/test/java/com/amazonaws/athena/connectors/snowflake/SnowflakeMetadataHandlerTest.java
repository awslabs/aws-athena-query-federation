package com.amazonaws.athena.connectors.snowflake;
/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeMetadataHandler.BLOCK_PARTITION_COLUMN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SnowflakeMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeMetadataHandlerTest.class);

    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SnowflakeConstants.SNOWFLAKE_NAME,
            "snowflake://jdbc:snowflake://hostname/?warehouse=warehousename&db=dbname&schema=schemaname&user=xxx&password=xxx");
    private SnowflakeMetadataHandler snowflakeMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private BlockAllocator blockAllocator;
    private Statement mockStatement;
    private S3Client mockS3;
    private BlockAllocatorImpl allocator;
    private SnowflakeMetadataHandler snowflakeMetadataHandlerMocked;

    @Before
    public void setup()
            throws Exception
    {
        this.allocator = new BlockAllocatorImpl();
        this.jdbcConnectionFactory = mock(JdbcConnectionFactory.class , RETURNS_DEEP_STUBS);
        this.connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.secretsManager = mock(SecretsManagerClient.class);
        this.athena = mock(AthenaClient.class);
        when(this.secretsManager.getSecretValue(eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.mockS3 = mock(S3Client.class);
        this.snowflakeMetadataHandler = new SnowflakeMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, mockS3, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        this.federatedIdentity = mock(FederatedIdentity.class);
        this.blockAllocator = mock(BlockAllocator.class);
        this.mockStatement = mock(Statement.class);


        this.federatedIdentity = mock(FederatedIdentity.class);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        snowflakeMetadataHandlerMocked = spy(this.snowflakeMetadataHandler);

        doReturn("arn:aws:iam::123456789012:role/test-role").when(snowflakeMetadataHandlerMocked).getRoleArn(any());
        doReturn("testS3Bucket").when(snowflakeMetadataHandlerMocked).getS3ExportBucket();
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableNoColumns() throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");

        this.snowflakeMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        when(this.connection.getMetaData().getColumns(nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                .thenThrow(new SQLException());
        this.snowflakeMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableException() throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "test@schema");
        this.snowflakeMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableNoColumnsException() throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "test@table");
        this.snowflakeMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String[] schema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] values = {{Types.INTEGER, 12, "testCol1", 0, 0}, {Types.VARCHAR, 25, "testCol2", 0, 0},
                {Types.TIMESTAMP, 93, "testCol3", 0, 0},  {Types.TIMESTAMP_WITH_TIMEZONE, 93, "testCol4", 0, 0}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());

        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName("TESTSCHEMA", "TESTTABLE");
        when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = this.snowflakeMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doListSchemaNames() throws Exception {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, "queryId", "testCatalog");

        String[] schema = {"TABLE_SCHEM", "TABLE_CATALOG"};
        Object[][] values = {{"TESTSCHEMA", "testCatalog"}, {"TESTSCHEMA2", "testCatalog"}};
        int[] types = {Types.VARCHAR, Types.VARCHAR};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet schemaResultSet = mockResultSet(schema, types, values, rowNumber);
        
        when(this.connection.getMetaData().getSchemas("testCatalog", null)).thenReturn(schemaResultSet);
        when(this.connection.getCatalog()).thenReturn("testCatalog");
        
        ListSchemasResponse listSchemasResponse = this.snowflakeMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        
        Assert.assertEquals(2, listSchemasResponse.getSchemas().size());
        Assert.assertTrue(listSchemasResponse.getSchemas().contains("TESTSCHEMA"));
        Assert.assertTrue(listSchemasResponse.getSchemas().contains("TESTSCHEMA2"));
    }

    @Test
    public void getPartitions() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField(BLOCK_PARTITION_COLUMN_NAME)
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add(BLOCK_PARTITION_COLUMN_NAME);
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("day", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0)), false));

        constraintsMap.put("month", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0)), false));

        constraintsMap.put("year", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 2000)), false));

        // Mock view check - empty result set means it's not a view
        ResultSet viewResultSet = mockResultSet(
            new String[]{"TABLE_SCHEM", "TABLE_NAME"},
            new int[]{Types.VARCHAR, Types.VARCHAR},
            new Object[][]{},
            new AtomicInteger(-1)
        );
        Statement mockStatement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(any())).thenReturn(viewResultSet);

        // Mock count query
        ResultSet countResultSet = mockResultSet(
            new String[]{"row_count"},
            new int[]{Types.BIGINT},
            new Object[][]{{1000L}},
            new AtomicInteger(-1)
        );
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(any())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(countResultSet);

        // Mock environment properties
        System.setProperty("aws_region", "us-east-1");
        System.setProperty("s3_export_bucket", "test-bucket");
        System.setProperty("s3_export_enabled", "false");

        // Mock metadata columns
        String[] columnSchema = {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME"};
        Object[][] columnValues = {
            {"schema1", "table1", "day", "int"},
            {"schema1", "table1", "month", "int"},
            {"schema1", "table1", "year", "int"},
            {"schema1", "table1", "preparedStmt", "varchar"},
            {"schema1", "table1", "queryId", "varchar"}
        };
        int[] columnTypes = {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        ResultSet columnResultSet = mockResultSet(columnSchema, columnTypes, columnValues, new AtomicInteger(-1));
        when(connection.getMetaData().getColumns(any(), eq("schema1"), eq("table1"), any())).thenReturn(columnResultSet);

        GetTableLayoutRequest req = new GetTableLayoutRequest(this.federatedIdentity, "queryId", "default",
                new TableName("schema1", "table1"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Map.of(), null),
                tableSchema,
                partitionCols);

        GetTableLayoutResponse res = snowflakeMetadataHandlerMocked.doGetTableLayout(allocator, req);
        Block partitions = res.getPartitions();

        assertTrue(partitions.getRowCount() > 0);
        assertNotNull(partitions.getFieldVector("preparedStmt"));
        assertNotNull(partitions.getFieldVector("queryId"));
    }

    @Test
    public void doGetSplits() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("partition")
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(schema);
        partitions.getFieldVector("preparedStmt").allocateNew();
        partitions.getFieldVector("queryId").allocateNew();
        partitions.getFieldVector("partition").allocateNew();
        partitions.getFieldVector("day").allocateNew();
        partitions.getFieldVector("month").allocateNew();
        partitions.getFieldVector("year").allocateNew();

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector("day"), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector("month"), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector("year"), i, (i % 28) + 1);
            BlockUtils.setValue(partitions.getFieldVector("preparedStmt"), i, "SELECT * FROM table");
            BlockUtils.setValue(partitions.getFieldVector("queryId"), i, String.valueOf(i));
            BlockUtils.setValue(partitions.getFieldVector("partition"), i, "partition_" + i);
        }
        partitions.setRowCount(num_partitions);

        // Mock S3 export functionality
        List<S3Object> objectList = new ArrayList<>();
        for (int i = 0; i < num_partitions; i++) {
            S3Object obj = S3Object.builder()
                .key(i + "/part_" + i + ".csv")
                .size(1000L)
                .build();
            objectList.add(obj);
        }
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder()
            .contents(objectList)
            .build();
        
        when(mockS3.listObjects(any(ListObjectsRequest.class))).thenReturn(listObjectsResponse);
        when(snowflakeMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");

        // Mock environment properties
        System.setProperty("aws_region", "us-east-1");
        System.setProperty("s3_export_bucket", "test-bucket");
        System.setProperty("s3_export_enabled", "true");

        // Mock database metadata
        ResultSet viewResultSet = mockResultSet(
            new String[]{"TABLE_SCHEM", "TABLE_NAME"},
            new int[]{Types.VARCHAR, Types.VARCHAR},
            new Object[][]{{"schema", "table_name"}},
            new AtomicInteger(-1)
        );
        when(connection.getMetaData().getTables(any(), eq("schema"), eq("table_name"), any())).thenReturn(viewResultSet);

        // Mock prepared statement execution
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(any())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.execute()).thenReturn(true);

        // Mock metadata columns
        String[] columnSchema = {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME"};
        Object[][] columnValues = {
            {"schema", "table_name", "day", "int"},
            {"schema", "table_name", "month", "int"},
            {"schema", "table_name", "year", "int"},
            {"schema", "table_name", "preparedStmt", "varchar"},
            {"schema", "table_name", "queryId", "varchar"}
        };
        int[] columnTypes = {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        ResultSet columnResultSet = mockResultSet(columnSchema, columnTypes, columnValues, new AtomicInteger(-1));
        when(connection.getMetaData().getColumns(any(), eq("schema"), eq("table_name"), any())).thenReturn(columnResultSet);

        GetSplitsRequest originalReq = new GetSplitsRequest(this.federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);
        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);

        MetadataResponse rawResponse = snowflakeMetadataHandlerMocked.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        String continuationToken = response.getContinuationToken();

        assertEquals(num_partitions, response.getSplits().size());
        
        for (Split nextSplit : response.getSplits()) {
            assertNotNull(nextSplit.getSpillLocation());
        }
    }

    @Test
    public void testGetPartitionSchema() {
        Schema schema = snowflakeMetadataHandler.getPartitionSchema("testCatalog");
        assertNotNull(schema);
        assertEquals(1, schema.getFields().size());
        assertEquals("partition", schema.getFields().get(0).getName());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), schema.getFields().get(0).getType());
    }

    @Test
    public void testListDatabaseNames() throws Exception {
        String[] schema = {"TABLE_SCHEM", "TABLE_CATALOG"};
        Object[][] values = {
            {"schema1", "testCatalog"},
            {"information_schema", "testCatalog"},
            {"schema2", "testCatalog"}
        };
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        
        when(connection.getMetaData().getSchemas(nullable(String.class), nullable(String.class))).thenReturn(resultSet);
        when(connection.getCatalog()).thenReturn("testCatalog");
        
        Set<String> databaseNames = snowflakeMetadataHandler.listDatabaseNames(connection);
        assertEquals(2, databaseNames.size());
        assertTrue(databaseNames.contains("schema1"));
        assertTrue(databaseNames.contains("schema2"));
        assertFalse(databaseNames.contains("information_schema"));
    }

    @Test
    public void testGetSchemaWithDataTypes() throws Exception {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = SchemaBuilder.newBuilder().build();
        
        String[] metadataSchema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] metadataValues = {
            {java.sql.Types.INTEGER, 10, "intCol", 0, 10},
            {java.sql.Types.VARCHAR, 255, "varcharCol", 0, 10},
            {java.sql.Types.TIMESTAMP, 0, "timestampCol", 0, 10},
            {java.sql.Types.TIMESTAMP_WITH_TIMEZONE, 0, "timestampTzCol", 0, 10}
        };
        AtomicInteger metadataRowNumber = new AtomicInteger(-1);
        ResultSet metadataResultSet = mockResultSet(metadataSchema, metadataValues, metadataRowNumber);
        
        String[] typeSchema = {"COLUMN_NAME", "DATA_TYPE"};
        Object[][] typeValues = {
            {"intCol", "INTEGER"},
            {"varcharCol", "VARCHAR"},
            {"timestampCol", "TIMESTAMP"},
            {"timestampTzCol", "TIMESTAMP_TZ"}
        };
        AtomicInteger typeRowNumber = new AtomicInteger(-1);
        ResultSet typeResultSet = mockResultSet(typeSchema, typeValues, typeRowNumber);
        
        when(connection.getMetaData().getColumns(nullable(String.class), eq(tableName.getSchemaName()), eq(tableName.getTableName()), nullable(String.class)))
            .thenReturn(metadataResultSet);
        PreparedStatement typeStmt = mock(PreparedStatement.class);
        when(connection.prepareStatement(contains("select COLUMN_NAME, DATA_TYPE"))).thenReturn(typeStmt);
        when(typeStmt.executeQuery()).thenReturn(typeResultSet);
        
        Schema schema = snowflakeMetadataHandler.getSchema(connection, tableName, partitionSchema);
        assertNotNull(schema);
        assertEquals(4, schema.getFields().size());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.INT.getType(), schema.findField("intCol").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), schema.findField("varcharCol").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType(), schema.findField("timestampCol").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType(), schema.findField("timestampTzCol").getType());
    }

    @Test(expected = RuntimeException.class)
    public void testGetSchemaNoMatchingColumns() throws Exception {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = SchemaBuilder.newBuilder().build();
        
        String[] metadataSchema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] metadataValues = {};
        AtomicInteger metadataRowNumber = new AtomicInteger(-1);
        ResultSet metadataResultSet = mockResultSet(metadataSchema, metadataValues, metadataRowNumber);
        
        when(connection.getMetaData().getColumns(nullable(String.class), eq(tableName.getSchemaName()), eq(tableName.getTableName()), nullable(String.class)))
            .thenReturn(metadataResultSet);
            
        snowflakeMetadataHandler.getSchema(connection, tableName, partitionSchema);
    }

    @Test
    public void testGetSchemaUnsupportedTypes() throws Exception {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema partitionSchema = SchemaBuilder.newBuilder().build();
        
        String[] metadataSchema = {"DATA_TYPE", "COLUMN_SIZE", "COLUMN_NAME", "DECIMAL_DIGITS", "NUM_PREC_RADIX"};
        Object[][] metadataValues = {
            {Types.ARRAY, 0, "arrayCol", 0, 10},
            {Types.INTEGER, 10, "intCol", 0, 10}
        };
        AtomicInteger metadataRowNumber = new AtomicInteger(-1);
        ResultSet metadataResultSet = mockResultSet(metadataSchema, metadataValues, metadataRowNumber);
        
        String[] typeSchema = {"COLUMN_NAME", "DATA_TYPE"};
        Object[][] typeValues = {
            {"arrayCol", "ARRAY"},
            {"intCol", "INTEGER"}
        };
        AtomicInteger typeRowNumber = new AtomicInteger(-1);
        ResultSet typeResultSet = mockResultSet(typeSchema, typeValues, typeRowNumber);
        
        when(connection.getMetaData().getColumns(nullable(String.class), eq(tableName.getSchemaName()), eq(tableName.getTableName()), nullable(String.class)))
            .thenReturn(metadataResultSet);
        PreparedStatement typeStmt = mock(PreparedStatement.class);
        when(connection.prepareStatement(contains("select COLUMN_NAME, DATA_TYPE"))).thenReturn(typeStmt);
        when(typeStmt.executeQuery()).thenReturn(typeResultSet);
        
        Schema schema = snowflakeMetadataHandler.getSchema(connection, tableName, partitionSchema);
        assertNotNull(schema);
        assertEquals(2, schema.getFields().size());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.LIST.getType(), schema.findField("arrayCol").getType());
        assertEquals(org.apache.arrow.vector.types.Types.MinorType.INT.getType(), schema.findField("intCol").getType());
    }

    @Test
    public void testGetPartitionsForView() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("col1")
                .addStringField(BLOCK_PARTITION_COLUMN_NAME)
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add(BLOCK_PARTITION_COLUMN_NAME);
        
        // Mock view check query results
        String[] viewSchema = {"TABLE_SCHEMA", "TABLE_NAME"};
        Object[][] viewValues = {{"testSchema", "testView"}};
        AtomicInteger viewRowNumber = new AtomicInteger(-1);
        ResultSet viewResultSet = mockResultSet(viewSchema, viewValues, viewRowNumber);
        
        PreparedStatement viewStmt = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(viewStmt);
        when(viewStmt.executeQuery()).thenReturn(viewResultSet);
        
        GetTableLayoutRequest req = new GetTableLayoutRequest(this.federatedIdentity, "queryId", "default",
                new TableName("testSchema", "testView"),
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema,
                partitionCols);

        BlockAllocator allocator = new BlockAllocatorImpl();
        GetTableLayoutResponse res = snowflakeMetadataHandler.doGetTableLayout(allocator, req);
        
        assertNotNull(res);
        Block partitions = res.getPartitions();
        assertEquals(1, partitions.getRowCount());
        assertEquals("*", partitions.getFieldVector(BLOCK_PARTITION_COLUMN_NAME).getObject(0).toString());
    }

    @Test
    public void testDoGetTableWithAnnotation()
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
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());

        Schema expected = expectedSchemaBuilder.build();

        String schemaName = "testSchema";
        String tableName = "testTable";
        TableName inputTableName = new TableName(schemaName, tableName + "@schemaCase=upper&tableCase=lower");

        SnowflakeMetadataHandler snowflakeMetadataHandlerAnnotation = new SnowflakeMetadataHandler(databaseConnectionConfig, this.jdbcConnectionFactory,
            com.google.common.collect.ImmutableMap.of(CaseResolver.CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name()));
        
        when(connection.getMetaData().getColumns("testCatalog", schemaName.toUpperCase(), tableName.toLowerCase(), null)).thenReturn(resultSet);
        when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = snowflakeMetadataHandlerAnnotation.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(new TableName(schemaName.toUpperCase(), tableName.toLowerCase()), getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void testDoGetTableWithNoneCasing()
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
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("partition", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());

        Schema expected = expectedSchemaBuilder.build();

        String schemaName = "TestSchema";
        String tableName = "TestTable";
        TableName inputTableName = new TableName(schemaName, tableName);

        SnowflakeMetadataHandler snowflakeMetadataHandlerNone = new SnowflakeMetadataHandler(databaseConnectionConfig, this.jdbcConnectionFactory,
            com.google.common.collect.ImmutableMap.of(CaseResolver.CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        
        when(connection.getMetaData().getColumns("testCatalog", schemaName, tableName, null)).thenReturn(resultSet);
        when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = snowflakeMetadataHandlerNone.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(new TableName(schemaName, tableName), getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }
}
