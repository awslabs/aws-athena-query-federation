/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
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
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.amazonaws.athena.connectors.vertica.query.QueryFactory;
import com.amazonaws.athena.connectors.vertica.query.VerticaExportQueryBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;

@RunWith(MockitoJUnitRunner.class)

public class VerticaMetadataHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaMetadataHandlerTest.class);
    private static final String[] TABLE_TYPES = new String[]{"TABLE"};
    private QueryFactory queryFactory;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private VerticaMetadataHandler verticaMetadataHandler;
    private VerticaExportQueryBuilder verticaExportQueryBuilder;
    private VerticaSchemaUtils verticaSchemaUtils;
    private Connection connection;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private S3Client amazonS3;
    private FederatedIdentity federatedIdentity;
    private BlockAllocatorImpl allocator;
    private DatabaseMetaData databaseMetaData;
    private TableName tableName;
    private Schema schema;
    private Constraints constraints;
    private SchemaBuilder schemaBuilder;
    private BlockWriter blockWriter;
    private QueryStatusChecker queryStatusChecker;
    private VerticaMetadataHandler verticaMetadataHandlerMocked;
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", VERTICA_NAME,
            "vertica://jdbc:vertica:thin:username/password@//127.0.0.1:1521/vrt");


    @Before
    public void setUp() throws Exception
    {

        this.verticaSchemaUtils = Mockito.mock(VerticaSchemaUtils.class);
        this.queryFactory = Mockito.mock(QueryFactory.class);
        this.verticaExportQueryBuilder = Mockito.mock(VerticaExportQueryBuilder.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        this.tableName = Mockito.mock(TableName.class);
        this.schema = Mockito.mock(Schema.class);
        this.constraints = Mockito.mock(Constraints.class);
        this.schemaBuilder = Mockito.mock(SchemaBuilder.class);
        this.blockWriter = Mockito.mock(BlockWriter.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.amazonS3 = Mockito.mock(S3Client.class);

        Mockito.lenient().when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build())))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);

        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.verticaMetadataHandler = new VerticaMetadataHandler(databaseConnectionConfig, this.jdbcConnectionFactory,
                com.google.common.collect.ImmutableMap.of(), amazonS3, verticaSchemaUtils);
        this.allocator = new BlockAllocatorImpl();
        this.databaseMetaData = this.connection.getMetaData();
        verticaMetadataHandlerMocked = Mockito.spy(this.verticaMetadataHandler);
    }


    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void doGetTable() throws Exception {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("id")
                .addStringField("name")
                .build();
        Mockito.when(verticaSchemaUtils.buildTableSchema(connection, tableName)).thenReturn(tableSchema);

        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, Collections.emptyMap());
        GetTableResponse response = verticaMetadataHandler.doGetTable(allocator, request);

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(tableName, response.getTableName());
        assertEquals(tableSchema, response.getSchema());
        assertTrue(response.getPartitionColumns().isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTable_SchemaUtilsFailure_ShouldThrowException() throws Exception {
        Mockito.when(verticaSchemaUtils.buildTableSchema(connection, tableName))
                .thenThrow(new RuntimeException("Schema build failed"));

        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, Collections.emptyMap());
        verticaMetadataHandler.doGetTable(allocator, request);
    }

    @Test(expected = NullPointerException.class)
    public void doGetTable_NullRequest_ShouldThrowException() throws Exception {
        verticaMetadataHandler.doGetTable(allocator, null);
    }

    @Test
    public void doGetTable_EmptySchema() throws Exception {
        Schema emptySchema = SchemaBuilder.newBuilder().build();
        Mockito.when(verticaSchemaUtils.buildTableSchema(connection, tableName)).thenReturn(emptySchema);

        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, Collections.emptyMap());
        GetTableResponse response = verticaMetadataHandler.doGetTable(allocator, request);

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(tableName, response.getTableName());
        assertEquals(emptySchema, response.getSchema());
        assertNotNull("Response should not be null", response);
    }

    @Test
    public void doListTables() throws Exception
    {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME",};
        Object[][] values = {{"testSchema", "testTable1"}};
        List<TableName> expectedTables = new ArrayList<>();
        expectedTables.add(new TableName("testSchema", "testTable1"));

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null, tableName.getSchemaName(), null, new String[]{"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        ListTablesResponse listTablesResponse = this.verticaMetadataHandler.doListTables(this.allocator,
                new ListTablesRequest(this.federatedIdentity,
                        "testQueryId",
                        "testCatalog",
                        tableName.getSchemaName(),
                        null, UNLIMITED_PAGE_SIZE_VALUE));

        Assert.assertArrayEquals(expectedTables.toArray(), listTablesResponse.getTables().toArray());

    }

    @Test
    public void doListSchemaNames() throws Exception
    {

        String[] schema = {"TABLE_SCHEM"};
        Object[][] values = {{"testDB1"}};
        String[] expected = {"testDB1"};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null, null, null, TABLE_TYPES)).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        ListSchemasResponse listSchemasResponse = this.verticaMetadataHandler.doListSchemaNames(this.allocator,
                new ListSchemasRequest(this.federatedIdentity,
                        "testQueryId", "testCatalog"));

        Assert.assertArrayEquals(expected, listSchemasResponse.getSchemas().toArray());
    }

    @Test
    public void enhancePartitionSchema()
    {
        Set<String> partitionCols = new HashSet<>();
        SchemaBuilder schemaBuilder = new SchemaBuilder();

        this.verticaMetadataHandler.enhancePartitionSchema(schemaBuilder, new GetTableLayoutRequest(
                this.federatedIdentity,
                "queryId",
                "testCatalog",
                this.tableName,
                this.constraints,
                this.schema,
                partitionCols
        ));
        Assert.assertEquals("preparedStmt", schemaBuilder.getField("preparedStmt").getName());

    }

    @Test
    public void getPartitions() throws Exception {

        Schema tableSchema = SchemaBuilder.newBuilder()
                .addField("bit_col", new ArrowType.Bool()) // BIT
                .addField("tinyint_col", new ArrowType.Int(8, true)) // TINYINT
                .addField("smallint_col", new ArrowType.Int(16, true)) // SMALLINT
                .addIntField("int_col") // INT
                .addField("bigint_col", new ArrowType.Int(64, true)) // BIGINT
                .addField("float_col", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)) // FLOAT4
                .addField("double_col", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)) // FLOAT8
                .addField("decimal_col", new ArrowType.Decimal(10, 2, 128)) // DECIMAL
                .addStringField("varchar_col") // VARCHAR
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");
        partitionCols.add("awsRegionSql");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("bit_col", SortedRangeSet.copyOf(new ArrowType.Bool(),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Bool(), true)), false));
        constraintsMap.put("tinyint_col", SortedRangeSet.copyOf(new ArrowType.Int(8, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(8, true), (byte) 127)), false));
        constraintsMap.put("smallint_col", SortedRangeSet.copyOf(new ArrowType.Int(16, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(16, true), (short) 32767)), false));
        constraintsMap.put("int_col", SortedRangeSet.copyOf(new ArrowType.Int(32, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(32, true), 1000)), false));
        constraintsMap.put("bigint_col", SortedRangeSet.copyOf(new ArrowType.Int(64, true),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Int(64, true), 1000000L)), false));
        constraintsMap.put("float_col", SortedRangeSet.copyOf(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE),
                ImmutableList.of(Range.equal(allocator, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), 3.14f)), false));
        constraintsMap.put("double_col", SortedRangeSet.copyOf(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
                ImmutableList.of(Range.equal(allocator, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), 3.14159)), false));
        constraintsMap.put("decimal_col", SortedRangeSet.copyOf(new ArrowType.Decimal(10, 2, 128),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Decimal(10, 2, 128), new BigDecimal("123.45"))), false));
        constraintsMap.put("varchar_col", SortedRangeSet.copyOf(new ArrowType.Utf8(),
                ImmutableList.of(Range.equal(allocator, new ArrowType.Utf8(), "test")), false));

        String[] schema = {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {
                {"testSchema", "testTable1", "bit_col", "boolean"},
                {"testSchema", "testTable1", "tinyint_col", "tinyint"},
                {"testSchema", "testTable1", "smallint_col", "smallint"},
                {"testSchema", "testTable1", "int_col", "int"},
                {"testSchema", "testTable1", "bigint_col", "bigint"},
                {"testSchema", "testTable1", "float_col", "float"},
                {"testSchema", "testTable1", "double_col", "double"},
                {"testSchema", "testTable1", "decimal_col", "numeric"},
                {"testSchema", "testTable1", "varchar_col", "varchar"},
                {"testSchema", "testTable1", "preparedStmt", "varchar"},
                {"testSchema", "testTable1", "queryId", "varchar"},
                {"testSchema", "testTable1", "awsRegionSql", "varchar"}
        };
        int[] types = {
                Types.BOOLEAN, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                Types.FLOAT, Types.DOUBLE, Types.DECIMAL,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
        };

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        String queryId = "queryId" + UUID.randomUUID().toString().replace("-", "");
        String s3ExportBucket = "s3://testS3Bucket";
        String expectedExportSql = String.format(
                "EXPORT TO PARQUET(directory = 's3://s3://testS3Bucket/%s', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                        "AS SELECT bit_col,tinyint_col,smallint_col,int_col,bigint_col,float_col,double_col,decimal_col," +
                        "varchar_col,preparedStmt,queryId,awsRegionSql " +
                        "FROM \"schema1\".\"table1\" " +
                        "WHERE (\"bit_col\" = 1 ) AND (\"tinyint_col\" = 127 ) AND (\"smallint_col\" = 32767 ) AND (\"int_col\" = 1000 ) " +
                        "AND (\"bigint_col\" = 1000000 ) AND (\"float_col\" = 3.14 ) AND (\"double_col\" = 3.14159 ) " +
                        "AND (\"decimal_col\" = 123.45 ) AND (\"varchar_col\" = 'test' )",
                queryId);

        Mockito.when(connection.getMetaData().getColumns(null, "schema1", "table1", null)).thenReturn(resultSet);
        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST("templateVerticaExportQuery")));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(s3ExportBucket);

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(federatedIdentity, queryId, "default",
                new TableName("schema1", "table1"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(),null),
                tableSchema, partitionCols);
             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {
            Block partitions = res.getPartitions();

            String actualQueryID = partitions.getFieldReader("queryId").readText().toString();
            String actualExportSql = partitions.getFieldReader("preparedStmt").readText().toString();

            logger.info("Expected queryId: {}", queryId);
            logger.info("Actual queryId: {}", actualQueryID);
            logger.info("Expected preparedStmt: {}", expectedExportSql);
            logger.info("Actual preparedStmt: {}", actualExportSql);

            Assert.assertTrue("Actual query ID should start with expected query ID: " + queryId,
                    actualQueryID.startsWith(queryId));

            String normalizedActualExportSql = actualExportSql.replace(actualQueryID, queryId);
            Assert.assertEquals(expectedExportSql, normalizedActualExportSql);
            Assert.assertEquals("ALTER SESSION SET AWSRegion='us-east-1'", partitions.getFieldReader("awsRegionSql").readText().toString());

            for (int row = 0; row < partitions.getRowCount() && row < 1; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());
        }
    }

    @Test
    public void doGetSplits()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");
        partitionCols.add("awsRegionSql");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector("day"), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector("month"), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector("year"), i, (i % 28) + 1);
            BlockUtils.setValue(partitions.getFieldVector("preparedStmt"), i, "test");
            BlockUtils.setValue(partitions.getFieldVector("queryId"), i, "123");
            BlockUtils.setValue(partitions.getFieldVector("awsRegionSql"), i, "us-west-2");

        }

        List<S3Object> objectList = new ArrayList<>();
        S3Object obj = S3Object.builder().key("testKey").build();
        objectList.add(obj);
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder().contents(objectList).build();
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(listObjectsResponse);

        GetSplitsRequest originalReq = new GetSplitsRequest(this.federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT,Collections.emptyMap(),null),
                null);
        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);

        logger.info("doGetSplits: req[{}]", req);
        doGetSplitsFunctionTest(req);
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket/testWithFolderPath");
        doGetSplitsFunctionTest(req);
    }

    @Test
    public void doGetSplitsQueryPassthrough() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("id")
                .addStringField("name")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");
        partitionCols.add("awsRegionSql");

        String query = "SELECT id, name FROM testTable";
        Map<String, String> queryArgs = Map.of("QUERY", query, "schemaFunctionName", "SYSTEM.QUERY");
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryArgs,
                null
        );

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("preparedStmt"), 0, "SELECT id, name FROM testTable");
        BlockUtils.setValue(partitions.getFieldVector("queryId"), 0, "query123");
        BlockUtils.setValue(partitions.getFieldVector("awsRegionSql"), 0, "ALTER SESSION SET AWSRegion='us-west-2'");

        List<S3Object> objectList = new ArrayList<>();
        S3Object obj = S3Object.builder().key("query123/part1.parquet").build();
        objectList.add(obj);
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder().contents(objectList).build();

        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("s3://testS3Bucket");
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(listObjectsResponse);


        Mockito.when(connection.prepareStatement("EXPORT TO PARQUET(directory = 's3://testS3Bucket/query123') AS SELECT id, name FROM testTable")).thenReturn(Mockito.mock(PreparedStatement.class));
        Mockito.when(connection.prepareStatement("ALTER SESSION SET AWSRegion='us-west-2'")).thenReturn(Mockito.mock(PreparedStatement.class));

        GetSplitsRequest req = new GetSplitsRequest(federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", JdbcQueryPassthrough.SCHEMA_FUNCTION_NAME), partitions, partitionCols, queryConstraints, null);

        GetSplitsResponse rawResponse = verticaMetadataHandlerMocked.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        assertEquals(1, rawResponse.getSplits().size());
        Split split = rawResponse.getSplits().iterator().next();
        assertEquals("query123", split.getProperty("query_id"));
        assertEquals("s3:", split.getProperty("exportBucket"));
        assertEquals("query123/part1.parquet", split.getProperty("s3ObjectKey"));
    }


    @Test
    public void testBuildQueryPassthroughSql() {
        String query = "SELECT id, name FROM testTable";
        Map<String, String> queryArgs = Map.of("QUERY", query, "schemaFunctionName", "SYSTEM.QUERY");
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryArgs,
                null
        );

        String result = verticaMetadataHandlerMocked.buildQueryPassthroughSql(queryConstraints);
        assertEquals(query, result);
    }

    private void doGetSplitsFunctionTest(GetSplitsRequest req) {
        MetadataResponse rawResponse = verticaMetadataHandlerMocked.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - splits[{}]", continuationToken, response.getSplits());

        for (Split nextSplit : response.getSplits()) {

            assertNotNull(nextSplit.getProperty("query_id"));
            assertNotNull(nextSplit.getProperty("exportBucket"));
            assertNotNull(nextSplit.getProperty("s3ObjectKey"));
        }

        assertFalse(response.getSplits().isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void doGetSplits_NullRequest_ShouldThrowException() {
        verticaMetadataHandlerMocked.doGetSplits(allocator, null);
    }

    @Test(expected = RuntimeException.class)
    public void doGetSplits_InvalidPartitionData_ShouldThrowException() {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("invalidField")
                .build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("invalidField"), 0, "invalid");

        GetSplitsRequest req = new GetSplitsRequest(federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"), partitions, Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), null);

        verticaMetadataHandlerMocked.doGetSplits(allocator, req);
    }

    @Test(expected = RuntimeException.class)
    public void doGetSplits_S3AccessFailure_ShouldThrowException() {
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class)))
                .thenThrow(new RuntimeException("S3 access failed"));

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("preparedStmt"), 0, "test");
        BlockUtils.setValue(partitions.getFieldVector("queryId"), 0, "123");
        BlockUtils.setValue(partitions.getFieldVector("awsRegionSql"), 0, "us-west-2");

        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");

        GetSplitsRequest req = new GetSplitsRequest(federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"), partitions, Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), null);

        verticaMetadataHandlerMocked.doGetSplits(allocator, req);
    }
    @Test
    public void doGetQueryPassthroughSchema() throws Exception {
        String query = "SELECT id, name FROM testTable";
        Map<String, String> queryArgs = Map.of("QUERY", query, "schemaFunctionName", "SYSTEM.QUERY");
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryArgs,
                null
        );
        ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        Mockito.when(connection.prepareStatement(query)).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(2);
        Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn("id");
        Mockito.when(resultSetMetaData.getColumnLabel(1)).thenReturn("id");
        Mockito.when(resultSetMetaData.getColumnTypeName(1)).thenReturn("INTEGER");
        Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn("name");
        Mockito.when(resultSetMetaData.getColumnLabel(2)).thenReturn("name");
        Mockito.when(resultSetMetaData.getColumnTypeName(2)).thenReturn("VARCHAR");

        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, queryConstraints.getQueryPassthroughArguments());
        GetTableResponse response = verticaMetadataHandler.doGetQueryPassthroughSchema(allocator, request);

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(tableName, response.getTableName());
        assertEquals(2, response.getSchema().getFields().size());
        assertEquals("id", response.getSchema().getFields().get(0).getName());
        assertEquals("name", response.getSchema().getFields().get(1).getName());
    }

    @Test
    public void doGetQueryPassthroughSchemaInvalid() throws Exception {
        Constraints queryConstraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );
        GetTableRequest request = new GetTableRequest(federatedIdentity, "testQueryId", "testCatalog", tableName, queryConstraints.getQueryPassthroughArguments());

        try {
            verticaMetadataHandler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No Query passed through"));
        }
        }
}