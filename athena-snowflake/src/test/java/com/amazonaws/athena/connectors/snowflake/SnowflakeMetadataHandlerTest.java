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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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

    @Test(expected = RuntimeException.class)
    public void doListSchemaNames() throws Exception {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, "queryId", "testCatalog");

        Statement statement = mock(Statement.class);
        when(this.connection.createStatement()).thenReturn(statement);
        String[][] SchemaandCatalogNames = {{"TESTSCHEMA"},{"TESTCATALOG"}};
        ResultSet schemaResultSet = mockResultSet(new String[]{"TABLE_SCHEM","TABLE_CATALOG"}, new int[]{Types.VARCHAR,Types.VARCHAR}, SchemaandCatalogNames, new AtomicInteger(-1));
        when(this.connection.getMetaData().getSchemas(any(), any())).thenReturn(schemaResultSet);
        ListSchemasResponse listSchemasResponse = this.snowflakeMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        String[] expectedResult = {"TESTSCHEMA","TESTCATALOG"};
        Assert.assertEquals(Arrays.toString(expectedResult), listSchemasResponse.getSchemas().toString());
    }

    @Test
    public void getPartitions() throws Exception
    {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("day", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0)), false));

        constraintsMap.put("month", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0)), false));

        constraintsMap.put("year", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 2000)), false));

        String testSql = "Select * from schema1.table1";
        String[] test = new String[]{"Select * from schema1.table1", "Select * from schema1.table1"};

        String[] schema = {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {{"testSchema", "testTable1", "day", "int"}, {"testSchema", "testTable1", "month", "int"},
                {"testSchema", "testTable1", "year", "int"}, {"testSchema", "testTable1", "preparedStmt", "varchar"},
                {"testSchema", "testTable1", "queryId", "varchar"}};
        int[] types = {Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR};
        List<TableName> expectedTables = new ArrayList<>();
        expectedTables.add(new TableName("testSchema", "testTable1"));

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        when(connection.getMetaData().getColumns(null, "schema1",
                "table1", null)).thenReturn(resultSet);


        when(snowflakeMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(this.federatedIdentity, "queryId", "default",
                new TableName("schema1", "table1"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                tableSchema,
                partitionCols);

             GetTableLayoutResponse res = snowflakeMetadataHandlerMocked.doGetTableLayout(allocator, req)) {
            Block partitions = res.getPartitions();

            Assert.assertNotNull(partitions.getFieldReader("preparedStmt").readText().toString());

            for (int row = 0; row < partitions.getRowCount() && row < 1; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));

            }
            assertTrue(partitions.getRowCount() > 0);
        }
    }

    @Test
    public void doGetSplits() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector("day"), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector("month"), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector("year"), i, (i % 28) + 1);
            BlockUtils.setValue(partitions.getFieldVector("preparedStmt"), i, "test");
            BlockUtils.setValue(partitions.getFieldVector("queryId"), i, "123");
        }

        List<S3Object> objectList = new ArrayList<>();
        S3Object obj = S3Object.builder().key("testKey").build();
        objectList.add(obj);
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder().contents(objectList).build();
        when(snowflakeMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");

        when(mockS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(listObjectsResponse);

        GetSplitsRequest originalReq = new GetSplitsRequest(this.federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                null);
        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);

        logger.info("doGetSplits: req[{}]", req);
        MetadataResponse rawResponse = snowflakeMetadataHandlerMocked.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - splits[{}]", continuationToken, response.getSplits());

        for (Split nextSplit : response.getSplits()) {

            assertNotNull(nextSplit.getProperty("query_id"));
            assertNotNull(nextSplit.getProperty("exportBucket"));
            assertNotNull(nextSplit.getProperty("s3ObjectKey"));
        }

        assertTrue(!response.getSplits().isEmpty());
    }
}
