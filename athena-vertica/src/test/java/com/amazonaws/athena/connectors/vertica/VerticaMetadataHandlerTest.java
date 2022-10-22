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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.vertica.query.QueryFactory;
import com.amazonaws.athena.connectors.vertica.query.VerticaExportQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

@RunWith(MockitoJUnitRunner.class)

public class VerticaMetadataHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaMetadataHandlerTest.class);
    private static final String[] TABLE_TYPES = {"TABLE"};
    private QueryFactory queryFactory;

    private VerticaMetadataHandler verticaMetadataHandler;
    private VerticaConnectionFactory verticaConnectionFactory;
    private VerticaExportQueryBuilder verticaExportQueryBuilder;
    private VerticaSchemaUtils verticaSchemaUtils;
    private Connection connection;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private AmazonS3 amazonS3;
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
    @Mock
    private AmazonS3 s3clientMock;
    @Mock
    private ListObjectsRequest listObjectsRequest;
    @Mock
    private ObjectListing objectListing;


    @Before
    public void setUp() throws SQLException
    {

        this.verticaConnectionFactory = Mockito.mock(VerticaConnectionFactory.class);
        this.verticaSchemaUtils = Mockito.mock(VerticaSchemaUtils.class);
        this.queryFactory = Mockito.mock(QueryFactory.class);
        this.verticaExportQueryBuilder = Mockito.mock(VerticaExportQueryBuilder.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        this.tableName = Mockito.mock(TableName.class);
        this.schema = Mockito.mock(Schema.class);
        this.constraints = Mockito.mock(Constraints.class);
        this.schemaBuilder = Mockito.mock(SchemaBuilder.class);
        this.blockWriter = Mockito.mock(BlockWriter.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.amazonS3 = Mockito.mock(AmazonS3.class);

        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        Mockito.when(this.verticaConnectionFactory.getOrCreateConn(anyString())).thenReturn(connection);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(amazonS3.getRegion()).thenReturn(Region.US_West_2);

        this.verticaMetadataHandler = new VerticaMetadataHandler(new LocalKeyFactory(),
                verticaConnectionFactory,
                secretsManager,
                athena,
                "spill-bucket",
                "spill-prefix",
                verticaSchemaUtils,
                amazonS3
        );
        this.allocator =  new BlockAllocatorImpl();
        this.databaseMetaData = this.connection.getMetaData();
        verticaMetadataHandlerMocked = Mockito.spy(this.verticaMetadataHandler);
    }


    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
    }


    @Test
    public void doListSchemaNames() throws SQLException
    {

        String[] schema = {"TABLE_SCHEM"};
        Object[][] values = {{"testDB1"}};
        String[] expected = {"testDB1"};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null,null, null, TABLE_TYPES)).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        ListSchemasResponse listSchemasResponse = this.verticaMetadataHandler.doListSchemaNames(this.allocator,
                                                                     new ListSchemasRequest(this.federatedIdentity,
                                                                    "testQueryId", "testCatalog"));
        Assert.assertArrayEquals(expected, listSchemasResponse.getSchemas().toArray());
    }

    @Test
    public void doListTables() throws SQLException {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME", };
        Object[][] values = {{"testSchema", "testTable1"}};
        List<TableName> expectedTables = new ArrayList<>();
        expectedTables.add(new TableName("testSchema", "testTable1"));

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null, tableName.getSchemaName(), null, TABLE_TYPES)).thenReturn(resultSet);
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
    public void enhancePartitionSchema()
    {
        GetTableLayoutRequest req = null;
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
    public void getPartitions() throws Exception
    {
       Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");
        partitionCols.add("awsRegionSql");
        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("day", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0)), false));

        constraintsMap.put("month", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 0)), false));

        constraintsMap.put("year", SortedRangeSet.copyOf(org.apache.arrow.vector.types.Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 2000)), false));


        GetTableLayoutRequest req = null;
        GetTableLayoutResponse res = null;


        String testSql = "Select * from schema1.table1";
        String[] test = new String[]{"Select * from schema1.table1", "Select * from schema1.table1"};

        String[] schema = {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {{"testSchema", "testTable1", "day", "int"}, {"testSchema", "testTable1", "month", "int"},
                {"testSchema", "testTable1", "year", "int"}, {"testSchema", "testTable1", "preparedStmt", "varchar"},
                {"testSchema", "testTable1", "queryId", "varchar"},  {"testSchema", "testTable1", "awsRegionSql", "varchar"}};
        int[] types = {Types.INTEGER, Types.INTEGER, Types.INTEGER,Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        List<TableName> expectedTables = new ArrayList<>();
        expectedTables.add(new TableName("testSchema", "testTable1"));

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        Mockito.when(connection.getMetaData().getColumns(null, "schema1",
                "table1", null)).thenReturn(resultSet);

        Mockito.when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST("templateVerticaExportQuery")));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");

        try {
            req = new GetTableLayoutRequest(this.federatedIdentity, "queryId", "default",
                    new TableName("schema1", "table1"),
                    new Constraints(constraintsMap),
                    tableSchema,
                    partitionCols);

            res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req);

            Block partitions = res.getPartitions();

            String actualQueryID = partitions.getFieldReader("queryId").readText().toString();
            String expectedExportSql = "EXPORT TO PARQUET(directory = 's3://testS3Bucket/" +
                    actualQueryID + "', Compression='snappy', fileSizeMB=16, rowGroupSizeMB=16) " +
                    "AS SELECT day,month,year,preparedStmt,queryId,awsRegionSql " +
                    "FROM \"schema1\".\"table1\" " +
                    "WHERE ((\"day\" > 0 )) AND ((\"month\" > 0 )) AND ((\"year\" > 2000 ))";

            Assert.assertEquals(expectedExportSql, partitions.getFieldReader("preparedStmt").readText().toString());


            for (int row = 0; row < partitions.getRowCount() && row < 1; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));

            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());

        }
        finally {
        try {
            req.close();
            res.close();
        }
        catch (Exception ex) {
            logger.error("doGetTableLayout: ", ex);
        }
    }

        logger.info("doGetTableLayout - exit");

    }


    @Test
    public void doGetSplits() throws SQLException {

        logger.info("doGetSplits: enter");

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
        List<S3ObjectSummary> s3ObjectSummariesList = new ArrayList<>();
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName("s3ExportBucket");
        s3ObjectSummary.setKey("testKey");
        s3ObjectSummariesList.add(s3ObjectSummary);
        ListObjectsRequest listObjectsRequestObj = new ListObjectsRequest();
        listObjectsRequestObj.setBucketName("s3ExportBucket");
        listObjectsRequestObj.setPrefix("queryId");


        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");
        Mockito.when(listObjectsRequest.withBucketName(anyString())).thenReturn(listObjectsRequestObj);
        Mockito.when(listObjectsRequest.withPrefix(anyString())).thenReturn(listObjectsRequestObj);
        Mockito.when(amazonS3.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
        Mockito.when(objectListing.getObjectSummaries()).thenReturn(s3ObjectSummariesList);

        GetSplitsRequest originalReq = new GetSplitsRequest(this.federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap),
                null);
        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);

        logger.info("doGetSplits: req[{}]", req);
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

        assertTrue(!response.getSplits().isEmpty());

        logger.info("doGetSplits: exit");
    }


}
