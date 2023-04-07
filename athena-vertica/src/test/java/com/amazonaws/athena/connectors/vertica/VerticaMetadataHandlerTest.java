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
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.proto.metadata.*;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

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
    private BlockAllocator allocator;
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
        this.federatedIdentity = FederatedIdentity.newBuilder().build();
        this.databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        this.tableName = TableName.newBuilder().build();
        this.schema = Mockito.mock(Schema.class);
        this.constraints = Mockito.mock(Constraints.class);
        this.schemaBuilder = Mockito.mock(SchemaBuilder.class);
        this.blockWriter = Mockito.mock(BlockWriter.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.amazonS3 = Mockito.mock(AmazonS3.class);

        Mockito.lenient().when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        Mockito.when(this.verticaConnectionFactory.getOrCreateConn(nullable(String.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(amazonS3.getRegion()).thenReturn(Region.US_West_2);

        this.verticaMetadataHandler = new VerticaMetadataHandler(new LocalKeyFactory(),
                verticaConnectionFactory,
                secretsManager,
                athena,
                "spill-bucket",
                "spill-prefix",
                verticaSchemaUtils,
                amazonS3,
                com.google.common.collect.ImmutableMap.of("catalog_name", "asdf_connection_str", "export_bucket", "asdf_export_bucket_loc"));
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
            ListSchemasRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalog").build()
        );
        Assert.assertArrayEquals(expected, listSchemasResponse.getSchemasList().toArray());
    }

    @Test
    public void doListTables() throws SQLException {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME", };
        Object[][] values = {{"testSchema", "testTable1"}};
        List<TableName> expectedTables = new ArrayList<>();
        expectedTables.add(TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable1").build());

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(databaseMetaData.getTables(null, tableName.getSchemaName(), null, TABLE_TYPES)).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        ListTablesResponse listTablesResponse = this.verticaMetadataHandler.doListTables(this.allocator,
            ListTablesRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalog").setSchemaName(tableName.getSchemaName()).setPageSize(UNLIMITED_PAGE_SIZE_VALUE).build()
        );

        Assert.assertArrayEquals(expectedTables.toArray(), listTablesResponse.getTablesList().toArray());

    }

    @Test
    public void enhancePartitionSchema()
    {
        GetTableLayoutRequest req = null;
        Set<String> partitionCols = new HashSet<>();
        SchemaBuilder schemaBuilder = new SchemaBuilder();

        this.verticaMetadataHandler.enhancePartitionSchema(allocator, schemaBuilder, GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("queryId").setCatalogName("testCatalog")
            .setTableName(this.tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(this.constraints))
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(this.schema))
            .addAllPartitionCols(partitionCols)
            .build());
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
        expectedTables.add(TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable1").build());

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        Mockito.when(connection.getMetaData().getColumns(null, "schema1",
                "table1", null)).thenReturn(resultSet);

        Mockito.lenient().when(queryFactory.createVerticaExportQueryBuilder()).thenReturn(new VerticaExportQueryBuilder(new ST("templateVerticaExportQuery")));
        Mockito.when(verticaMetadataHandlerMocked.getS3ExportBucket()).thenReturn("testS3Bucket");

        req = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("queryId").setCatalogName("default")
        .setTableName(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build()).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
        .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(tableSchema))
        .addAllPartitionCols(partitionCols)
        .build();
        res = verticaMetadataHandlerMocked.doGetTableLayout(allocator, req);

        Block partitions = ProtobufMessageConverter.fromProtoBlock(allocator, res.getPartitions());

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
        logger.info("doGetTableLayout - exit");

    }


    @Test
    public void doGetSplits() throws SQLException {

        logger.info("doGetSplits: enter");

        Schema schema = SchemaBuilder.newBuilder()
                .addField("day", new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true))
                .addField("month", new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true))
                .addField("year", new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true))
                .addField("preparedStmt", new org.apache.arrow.vector.types.pojo.ArrowType.Utf8())
                .addField("queryId", new org.apache.arrow.vector.types.pojo.ArrowType.Utf8())
                .addField("awsRegionSql", new org.apache.arrow.vector.types.pojo.ArrowType.Utf8())
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add("preparedStmt");
        partitionCols.add("queryId");
        partitionCols.add("awsRegionSql");

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
        // There was a bug in this test where we weren't setting the row count on the block, so the readers didn't think it could read anything.
        partitions.setRowCount(10);

        // Everything commented out here was me testing out and understanding how our BlockUtils works.
        // logger.error("FIRST ROW OF BLOCK IS {}", BlockUtils.rowToString(partitions, 0));
        // logger.error("LAST ROW OF BLOCK IS {}", BlockUtils.rowToString(partitions, 9));
        // Block partitionsCopy = allocator.createBlock(schema);
        // BlockUtils.copyRows(partitions, partitionsCopy, 0, 9); //inclusive copy
        // logger.error("FIRST ROW OF RAW COPY IS {}", BlockUtils.rowToString(partitionsCopy, 0));
        // Block toAndFromProtoBlock = ProtobufMessageConverter.fromProtoBlock(allocator, ProtobufMessageConverter.toProtoBlock(partitions));
        // logger.error("FIRST ROW OF PROTO COPY IS {}", BlockUtils.rowToString(toAndFromProtoBlock, 0));
        // partitions.getFieldVectors().forEach(fv -> {
        //     fv.setValueCount(10);
        //     for (int i = 0; i < fv.getValueCount(); i++) {
        //         // fv.get()
        //     }
        //     logger.error("ROW COUNT FOR FIELD VECTOR {} IS {}", fv.getName(), fv.getValueCount());
        // });
        
        List<S3ObjectSummary> s3ObjectSummariesList = new ArrayList<>();
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName("s3ExportBucket");
        s3ObjectSummary.setKey("testKey");
        s3ObjectSummariesList.add(s3ObjectSummary);
        ListObjectsRequest listObjectsRequestObj = new ListObjectsRequest();
        listObjectsRequestObj.setBucketName("s3ExportBucket");
        listObjectsRequestObj.setPrefix("queryId");

        Mockito.lenient().when(listObjectsRequest.withBucketName(nullable(String.class))).thenReturn(listObjectsRequestObj);
        Mockito.lenient().when(listObjectsRequest.withPrefix(nullable(String.class))).thenReturn(listObjectsRequestObj);
        Mockito.when(amazonS3.listObjects(nullable(ListObjectsRequest.class))).thenReturn(objectListing);
        Mockito.when(objectListing.getObjectSummaries()).thenReturn(s3ObjectSummariesList);

        logger.error("ROW COUNT IS {}", partitions.getRowCount());
        GetSplitsRequest req = GetSplitsRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("queryId").setCatalogName("catalog_name")
            .setTableName(TableName.newBuilder().setSchemaName("schema").setTableName("table_name").build())
            .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
            .addAllPartitionCols(partitionCols)
            .build();
        logger.info("doGetSplits: req[{}]", req);
        GetSplitsResponse response = verticaMetadataHandler.doGetSplits(allocator, req);
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - splits[{}]", continuationToken, response.getSplitsList());

        for (Split nextSplit : response.getSplitsList()) {

            assertNotNull(nextSplit.getPropertiesMap().get("query_id"));
            assertNotNull(nextSplit.getPropertiesMap().get("exportBucket"));
            assertNotNull(nextSplit.getPropertiesMap().get("s3ObjectKey"));
        }

        assertTrue(!response.getSplitsList().isEmpty());

        logger.info("doGetSplits: exit");
    }


}
