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

    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable1";
    private static final String TEST_S3_BUCKET = "testS3Bucket";
    private static final String TEST_QUERY_ID = "queryId";
    private static final String QUERY_ID = "testQueryId";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String DEFAULT_CATALOG = "default";

    private static final String INT_TYPE = "int";
    private static final String VARCHAR_TYPE = "varchar";
    private static final String TEST_VALUE = "test";
    private static final String NULLABLE_FIELD = "nullable_field";
    private static final String NULLABLE_NAME = "nullable_name";
    private static final String NULLABLE_SCORE = "nullable_score";
    private static final String REQUIRED_FIELD = "required_field";

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String SCORE_FIELD = "score";
    private static final String GRADE_FIELD = "grade";
    private static final String AGE_FIELD = "age";
    private static final String DEPARTMENT_FIELD = "department";
    private static final String STATUS_FIELD = "status";
    private static final String PRICE_FIELD = "price";
    private static final String CATEGORY_FIELD = "category";
    private static final String FIELD1 = "field1";
    private static final String FIELD2 = "field2";
    private static final String PREPARED_STMT_FIELD = "preparedStmt";
    private static final String AWS_REGION_SQL_FIELD = "awsRegionSql";

    private static final String TABLE_SCHEM = "TABLE_SCHEM";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";

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
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, VERTICA_NAME,
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

        GetTableRequest request = new GetTableRequest(federatedIdentity, QUERY_ID, TEST_CATALOG, tableName, Collections.emptyMap());
        GetTableResponse response = verticaMetadataHandler.doGetTable(allocator, request);

        assertEquals(TEST_CATALOG, response.getCatalogName());
        assertEquals(tableName, response.getTableName());
        assertEquals(tableSchema, response.getSchema());
        assertTrue(response.getPartitionColumns().isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTable_SchemaUtilsFailure_ShouldThrowException() throws Exception {
        Mockito.when(verticaSchemaUtils.buildTableSchema(connection, tableName))
                .thenThrow(new RuntimeException("Schema build failed"));

        GetTableRequest request = new GetTableRequest(federatedIdentity, QUERY_ID, TEST_CATALOG, tableName, Collections.emptyMap());
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

        GetTableRequest request = new GetTableRequest(federatedIdentity, QUERY_ID, TEST_CATALOG, tableName, Collections.emptyMap());
        GetTableResponse response = verticaMetadataHandler.doGetTable(allocator, request);

        assertEquals(TEST_CATALOG, response.getCatalogName());
        assertEquals(tableName, response.getTableName());
        assertEquals(emptySchema, response.getSchema());
        assertNotNull("Response should not be null", response);
    }

    @Test
    public void doListTables() throws Exception
    {
        String[] schema = {TABLE_SCHEM, TABLE_NAME,};
        Object[][] values = {{TEST_SCHEMA, TEST_TABLE}};
        List<TableName> expectedTables = new ArrayList<>();
        expectedTables.add(new TableName(TEST_SCHEMA, TEST_TABLE));

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null, tableName.getSchemaName(), null, new String[]{"TABLE", "VIEW", "EXTERNAL TABLE", "MATERIALIZED VIEW"})).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        ListTablesResponse listTablesResponse = this.verticaMetadataHandler.doListTables(this.allocator,
                new ListTablesRequest(this.federatedIdentity,
                        QUERY_ID,
                        TEST_CATALOG,
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
                        QUERY_ID, TEST_CATALOG));

        Assert.assertArrayEquals(expected, listSchemasResponse.getSchemas().toArray());
    }

    @Test
    public void enhancePartitionSchema()
    {
        Set<String> partitionCols = new HashSet<>();
        SchemaBuilder schemaBuilder = new SchemaBuilder();

        this.verticaMetadataHandler.enhancePartitionSchema(schemaBuilder, new GetTableLayoutRequest(
                this.federatedIdentity,
                TEST_QUERY_ID,
                TEST_CATALOG,
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
                .addStringField(TEST_QUERY_ID)
                .addStringField(AWS_REGION_SQL_FIELD)
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("preparedStmt");
        partitionCols.add(TEST_QUERY_ID);
        partitionCols.add(AWS_REGION_SQL_FIELD);

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
                ImmutableList.of(Range.equal(allocator, new ArrowType.Utf8(), TEST_VALUE)), false));

        String[] schema = {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {
                {TEST_SCHEMA, TEST_TABLE, "bit_col", "boolean"},
                {TEST_SCHEMA, TEST_TABLE, "tinyint_col", "tinyint"},
                {TEST_SCHEMA, TEST_TABLE, "smallint_col", "smallint"},
                {TEST_SCHEMA, TEST_TABLE, "int_col", "int"},
                {TEST_SCHEMA, TEST_TABLE, "bigint_col", "bigint"},
                {TEST_SCHEMA, TEST_TABLE, "float_col", "float"},
                {TEST_SCHEMA, TEST_TABLE, "double_col", "double"},
                {TEST_SCHEMA, TEST_TABLE, "decimal_col", "numeric"},
                {TEST_SCHEMA, TEST_TABLE, "varchar_col", VARCHAR_TYPE},
                {TEST_SCHEMA, TEST_TABLE, "preparedStmt", VARCHAR_TYPE},
                {TEST_SCHEMA, TEST_TABLE, TEST_QUERY_ID, VARCHAR_TYPE},
                {TEST_SCHEMA, TEST_TABLE, AWS_REGION_SQL_FIELD, VARCHAR_TYPE}
        };
        int[] types = {
                Types.BOOLEAN, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                Types.FLOAT, Types.DOUBLE, Types.DECIMAL,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
        };

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        String queryId = TEST_QUERY_ID + UUID.randomUUID().toString().replace("-", "");
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

            String actualQueryID = partitions.getFieldReader(TEST_QUERY_ID).readText().toString();
            String actualExportSql = partitions.getFieldReader("preparedStmt").readText().toString();

            logger.info("Expected queryId: {}", queryId);
            logger.info("Actual queryId: {}", actualQueryID);
            logger.info("Expected preparedStmt: {}", expectedExportSql);
            logger.info("Actual preparedStmt: {}", actualExportSql);

            Assert.assertTrue("Actual query ID should start with expected query ID: " + queryId,
                    actualQueryID.startsWith(queryId));

            String normalizedActualExportSql = actualExportSql.replace(actualQueryID, queryId);
            Assert.assertEquals(expectedExportSql, normalizedActualExportSql);
            Assert.assertEquals("ALTER SESSION SET AWSRegion='us-east-1'", partitions.getFieldReader(AWS_REGION_SQL_FIELD).readText().toString());

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
                .addStringField(TEST_QUERY_ID)
                .addStringField(AWS_REGION_SQL_FIELD)
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("preparedStmt");
        partitionCols.add(TEST_QUERY_ID);
        partitionCols.add(AWS_REGION_SQL_FIELD);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector("day"), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector("month"), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector("year"), i, (i % 28) + 1);
            BlockUtils.setValue(partitions.getFieldVector("preparedStmt"), i, TEST_VALUE);
            BlockUtils.setValue(partitions.getFieldVector(TEST_QUERY_ID), i, "123");
            BlockUtils.setValue(partitions.getFieldVector(AWS_REGION_SQL_FIELD), i, "us-west-2");

        }

        List<S3Object> objectList = new ArrayList<>();
        S3Object obj = S3Object.builder().key("testKey").build();
        objectList.add(obj);
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder().contents(objectList).build();
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(TEST_S3_BUCKET);
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(listObjectsResponse);

        GetSplitsRequest originalReq = new GetSplitsRequest(this.federatedIdentity, TEST_QUERY_ID, "catalog_name",
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
                .addStringField(TEST_QUERY_ID)
                .addStringField(AWS_REGION_SQL_FIELD)
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("preparedStmt");
        partitionCols.add(TEST_QUERY_ID);
        partitionCols.add(AWS_REGION_SQL_FIELD);

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
        BlockUtils.setValue(partitions.getFieldVector(TEST_QUERY_ID), 0, "query123");
        BlockUtils.setValue(partitions.getFieldVector(AWS_REGION_SQL_FIELD), 0, "ALTER SESSION SET AWSRegion='us-west-2'");

        List<S3Object> objectList = new ArrayList<>();
        S3Object obj = S3Object.builder().key("query123/part1.parquet").build();
        objectList.add(obj);
        ListObjectsResponse listObjectsResponse = ListObjectsResponse.builder().contents(objectList).build();

        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("s3://testS3Bucket");
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(listObjectsResponse);


        Mockito.when(connection.prepareStatement("EXPORT TO PARQUET(directory = 's3://testS3Bucket/query123') AS SELECT id, name FROM testTable")).thenReturn(Mockito.mock(PreparedStatement.class));
        Mockito.when(connection.prepareStatement("ALTER SESSION SET AWSRegion='us-west-2'")).thenReturn(Mockito.mock(PreparedStatement.class));

        GetSplitsRequest req = new GetSplitsRequest(federatedIdentity, TEST_QUERY_ID, "catalog_name",
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

        GetSplitsRequest req = new GetSplitsRequest(federatedIdentity, TEST_QUERY_ID, "catalog_name",
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
                .addStringField(TEST_QUERY_ID)
                .addStringField(AWS_REGION_SQL_FIELD)
                .build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("preparedStmt"), 0, TEST_VALUE);
        BlockUtils.setValue(partitions.getFieldVector(TEST_QUERY_ID), 0, "123");
        BlockUtils.setValue(partitions.getFieldVector(AWS_REGION_SQL_FIELD), 0, "us-west-2");

        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(TEST_S3_BUCKET);

        GetSplitsRequest req = new GetSplitsRequest(federatedIdentity, TEST_QUERY_ID, "catalog_name",
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

        GetTableRequest request = new GetTableRequest(federatedIdentity, QUERY_ID, TEST_CATALOG, tableName, queryConstraints.getQueryPassthroughArguments());
        GetTableResponse response = verticaMetadataHandler.doGetQueryPassthroughSchema(allocator, request);

        assertEquals(TEST_CATALOG, response.getCatalogName());
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
        GetTableRequest request = new GetTableRequest(federatedIdentity, QUERY_ID, TEST_CATALOG, tableName, queryConstraints.getQueryPassthroughArguments());

        try {
            verticaMetadataHandler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No Query passed through"));
        }
    }

    @Test
    public void testPredicateBuilderInClauseHandling() throws Exception
    {
        Schema tableSchema = createTestSchema(ID_FIELD, INT_TYPE, NAME_FIELD, VARCHAR_TYPE);
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(ID_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 1),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 2),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 3)
                ), false));

        String actualSql = executeTableLayoutTest(tableSchema, constraintsMap,
                ID_FIELD, INT_TYPE, NAME_FIELD, VARCHAR_TYPE);

        String actualQueryID = actualSql.substring(actualSql.indexOf("s3://testS3Bucket/") + 18,
                actualSql.indexOf("', Compression"));
        String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                "AS SELECT id,name " +
                "FROM \"testSchema\".\"testTable1\" " +
                "WHERE (\"id\" IN (1,2,3))";

        Assert.assertEquals(expectedExportSql, actualSql);
    }

    @Test
    public void testPredicateBuilderNullHandling() throws Exception
    {
        Schema tableSchema = createTestSchema(NULLABLE_FIELD, VARCHAR_TYPE);
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(NULLABLE_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), TEST_VALUE)), true));

        String actualSql = executeTableLayoutTest(tableSchema, constraintsMap,
                NULLABLE_FIELD, VARCHAR_TYPE);

        String actualQueryID = actualSql.substring(actualSql.indexOf("s3://testS3Bucket/") + 18,
                actualSql.indexOf("', Compression"));
        String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                "AS SELECT nullable_field " +
                "FROM \"testSchema\".\"testTable1\" " +
                "WHERE ((nullable_field IS NULL) OR \"nullable_field\" = 'test' )";

        Assert.assertEquals(expectedExportSql, actualSql);
    }

    @Test
    public void testPredicateBuilderRangeHandling() throws Exception
    {
        Schema tableSchema = createTestSchema(NAME_FIELD, VARCHAR_TYPE);
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(NAME_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.range(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), TEST_VALUE, true, "tesu", false)), false));

        String actualSql = executeTableLayoutTest(tableSchema, constraintsMap,
                NAME_FIELD, VARCHAR_TYPE);

        String actualQueryID = actualSql.substring(actualSql.indexOf("s3://testS3Bucket/") + 18,
                actualSql.indexOf("', Compression"));
        String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                "AS SELECT name " +
                "FROM \"testSchema\".\"testTable1\" " +
                "WHERE ((\"name\" >= 'test'  AND \"name\" < 'tesu' ))";

        Assert.assertEquals(expectedExportSql, actualSql);
    }

    @Test
    public void testComplexExpressionWithRangeAndInPredicatesTest() throws Exception
    {
        Schema tableSchema = createTestSchema(ID_FIELD, INT_TYPE, STATUS_FIELD, VARCHAR_TYPE, PRICE_FIELD, INT_TYPE, CATEGORY_FIELD, VARCHAR_TYPE);
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(PRICE_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.range(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 100, true, 1000, true)), false));

        constraintsMap.put(ID_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 1),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 2),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 3),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 4),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 5)
                ), false));

        constraintsMap.put(STATUS_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.range(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), "A", false, "Z", true)), false));

        constraintsMap.put(CATEGORY_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), "electronics"),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), "books")
                ), false));

        String actualSql = executeTableLayoutTest(tableSchema, constraintsMap,
                ID_FIELD, INT_TYPE, STATUS_FIELD, VARCHAR_TYPE, PRICE_FIELD, INT_TYPE, CATEGORY_FIELD, VARCHAR_TYPE);

        // Extract query ID for expected SQL construction
        String actualQueryID = actualSql.substring(actualSql.indexOf("s3://testS3Bucket/") + 18,
                actualSql.indexOf("', Compression"));

        // Construct expected SQL with actual query ID
        String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                "AS SELECT id,status,price,category " +
                "FROM \"testSchema\".\"testTable1\" " +
                "WHERE (\"id\" IN (1,2,3,4,5)) AND ((\"status\" > 'A'  AND \"status\" <= 'Z' )) AND ((\"price\" >= 100  AND \"price\" <= 1000 )) AND (\"category\" IN ('books','electronics'))";

        Assert.assertEquals(expectedExportSql, actualSql);
    }

    @Test
    public void testComplexExpressionWithNullableComparisonsTest() throws Exception
    {
        Schema tableSchema = createTestSchema(ID_FIELD, INT_TYPE, NULLABLE_NAME, VARCHAR_TYPE, NULLABLE_SCORE, INT_TYPE, REQUIRED_FIELD, VARCHAR_TYPE);
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(NULLABLE_NAME, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), TEST_VALUE)), true));

        constraintsMap.put(NULLABLE_SCORE, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.range(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0, true, 100, false)), false));

        constraintsMap.put(REQUIRED_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.all(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())), false));

        constraintsMap.put(ID_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0)), false));

        String actualSql = executeTableLayoutTest(tableSchema, constraintsMap,
                ID_FIELD, INT_TYPE, NULLABLE_NAME, VARCHAR_TYPE, NULLABLE_SCORE, INT_TYPE, REQUIRED_FIELD, VARCHAR_TYPE);

        // Extract query ID for expected SQL construction
        String actualQueryID = actualSql.substring(actualSql.indexOf("s3://testS3Bucket/") + 18,
                actualSql.indexOf("', Compression"));

        // Construct expected SQL with actual query ID
        String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                "AS SELECT id,nullable_name,nullable_score,required_field " +
                "FROM \"testSchema\".\"testTable1\" " +
                "WHERE ((\"id\" > 0 )) AND ((nullable_name IS NULL) OR \"nullable_name\" = 'test' ) AND ((\"nullable_score\" >= 0  AND \"nullable_score\" < 100 )) AND (required_field IS NOT NULL)";

        Assert.assertEquals(expectedExportSql, actualSql);

        logger.info("Nullable comparisons test - Generated SQL: {}", actualSql);
    }

    @Test
    public void testMultipleOperatorComplexExpression() throws Exception
    {
        Schema tableSchema = createTestSchema(SCORE_FIELD, INT_TYPE, GRADE_FIELD, VARCHAR_TYPE, AGE_FIELD, INT_TYPE, DEPARTMENT_FIELD, VARCHAR_TYPE);
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(SCORE_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 85)), false));

        constraintsMap.put(GRADE_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.lessThanOrEqual(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), "B")), false));

        constraintsMap.put(AGE_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.range(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 18, true, 65, false)), false));

        constraintsMap.put(DEPARTMENT_FIELD, SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), "engineering"),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), "marketing"),
                        Range.equal(allocator, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), "sales")
                ), false));

        String actualSql = executeTableLayoutTest(tableSchema, constraintsMap,
                SCORE_FIELD, INT_TYPE, GRADE_FIELD, VARCHAR_TYPE, AGE_FIELD, INT_TYPE, DEPARTMENT_FIELD, VARCHAR_TYPE);

        // Extract query ID for expected SQL construction
        String actualQueryID = actualSql.substring(actualSql.indexOf("s3://testS3Bucket/") + 18,
                actualSql.indexOf("', Compression"));

        // Construct expected SQL with actual query ID
        String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                "AS SELECT score,grade,age,department " +
                "FROM \"testSchema\".\"testTable1\" " +
                "WHERE ((\"score\" > 85 )) AND ((\"grade\" <= 'B' )) AND ((\"age\" >= 18  AND \"age\" < 65 )) AND (\"department\" IN ('engineering','marketing','sales'))";

        Assert.assertEquals(expectedExportSql, actualSql);

        logger.info("Multiple operators test - Generated SQL: {}", actualSql);
    }

    @Test
    public void testEmptyConstraintsNegativeCase() throws Exception
    {
        Schema tableSchema = createTestSchema(FIELD1, INT_TYPE, FIELD2, VARCHAR_TYPE);
        Map<String, ValueSet> constraintsMap = new HashMap<>(); // Empty constraints

        String actualSql = executeTableLayoutTest(tableSchema, constraintsMap,
                FIELD1, INT_TYPE, FIELD2, VARCHAR_TYPE);

        // Extract query ID for expected SQL construction
        String actualQueryID = actualSql.substring(actualSql.indexOf("s3://testS3Bucket/") + 18,
                actualSql.indexOf("', Compression"));

        // Construct expected SQL with actual query ID (no WHERE clause for empty constraints)
        String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                "AS SELECT field1,field2 " +
                "FROM \"testSchema\".\"testTable1\"";

        Assert.assertEquals(expectedExportSql, actualSql);

        logger.info("Empty constraints test - Generated SQL: {}", actualSql);
    }

    private Schema createTestSchema(String... columnSpecs)
    {
        SchemaBuilder builder = SchemaBuilder.newBuilder();
        for (int i = 0; i < columnSpecs.length; i += 2) {
            String columnName = columnSpecs[i];
            String columnType = columnSpecs[i + 1];
            switch (columnType.toLowerCase()) {
                case INT_TYPE:
                case "integer":
                    builder.addIntField(columnName);
                    break;
                case "bigint":
                    builder.addBigIntField(columnName);
                    break;
                default:
                    builder.addStringField(columnName);
            }
        }
        return builder.build();
    }

    private String executeTableLayoutTest(Schema tableSchema,
                                          Map<String, ValueSet> constraintsMap, String... columnSpecs) throws Exception
    {
        Set<String> partitionCols = new HashSet<>();

        // Create mock result set for columns
        String[] schema = {TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, TYPE_NAME};
        Object[][] values = new Object[columnSpecs.length / 2][];
        int[] types = new int[columnSpecs.length / 2];

        for (int i = 0; i < columnSpecs.length; i += 2) {
            int rowIndex = i / 2;
            String columnName = columnSpecs[i];
            String columnType = columnSpecs[i + 1];
            values[rowIndex] = new Object[]{TEST_SCHEMA, TEST_TABLE, columnName, columnType};
            types[rowIndex] = columnType.equalsIgnoreCase(INT_TYPE) || columnType.equalsIgnoreCase("integer") ? Types.INTEGER : Types.VARCHAR;
        }

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);
        Mockito.when(connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, null)).thenReturn(resultSet);

        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST("templateVerticaExportQuery")));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn(TEST_S3_BUCKET);

        try (GetTableLayoutRequest req = new GetTableLayoutRequest(this.federatedIdentity, TEST_QUERY_ID, DEFAULT_CATALOG,
                new TableName(TEST_SCHEMA, TEST_TABLE),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                tableSchema,
                partitionCols);

             GetTableLayoutResponse res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req)) {
            Block partitions = res.getPartitions();

            String actualSql = partitions.getFieldReader(PREPARED_STMT_FIELD).readText().toString();

            for (int row = 0; row < partitions.getRowCount() && row < 1; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("Generated SQL: {}", actualSql);

            return actualSql;
        }
    }
}