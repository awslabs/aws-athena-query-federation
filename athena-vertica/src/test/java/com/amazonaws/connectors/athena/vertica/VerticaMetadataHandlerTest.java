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
package com.amazonaws.connectors.athena.vertica;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class VerticaMetadataHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaMetadataHandlerTest.class);
    private static final String[] TABLE_TYPES = {"TABLE"};

    private VerticaMetadataHandler verticaMetadataHandler;
    private VerticaConnectionFactory verticaConnectionFactory;
    private VerticaJdbcSplitQueryBuilder verticaJdbcSplitQueryBuilder;
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

    @Before
    public void setUp() throws SQLException
    {

        this.verticaConnectionFactory = Mockito.mock(VerticaConnectionFactory.class);
        this.verticaJdbcSplitQueryBuilder = Mockito.mock(VerticaJdbcSplitQueryBuilder.class);
        this.verticaSchemaUtils = Mockito.mock(VerticaSchemaUtils.class);
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



        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        Mockito.when(this.verticaConnectionFactory.getOrCreateConn(anyString())).thenReturn(connection);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);

        this.verticaMetadataHandler = new VerticaMetadataHandler(new LocalKeyFactory(),
                verticaConnectionFactory,
                secretsManager,
                athena,
                "spill-bucket",
                "spill-prefix",
                verticaJdbcSplitQueryBuilder,
                verticaSchemaUtils,
                amazonS3
        );
        this.allocator =  new BlockAllocatorImpl();
        this.databaseMetaData = this.connection.getMetaData();

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
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
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
                                                                        tableName.getSchemaName()));

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
       /* Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .addStringField("preparedStmt")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("preparedStmt");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        GetTableLayoutRequest req = null;
        GetTableLayoutResponse res = null;

        String testSql = "Select * from schema1.table1";
        String[] test = new String[]{"Select * from schema1.table1", "Select * from schema1.table1"};
        ResultSet definition = connection.getMetaData().getColumns(null, "schema1", "table1", null);
        Mockito.when(verticaJdbcSplitQueryBuilder.buildSql(null, "schema1", "table1", tableSchema,
                new Constraints(constraintsMap),
                "queryId", definition)).thenReturn(testSql);


        req = new GetTableLayoutRequest(this.federatedIdentity, "queryId", "default",
                new TableName("schema1", "table1"),
                new Constraints(constraintsMap),
                tableSchema,
                partitionCols);


        verticaMetadataHandler.getPartitions(this.blockWriter, req, this.queryStatusChecker);
        res = verticaMetadataHandler.doGetTableLayout(allocator, req);

        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 1; row++) {
            logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
        }
        assertTrue(partitions.getRowCount() > 0);
        logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());*/

    }

    @Test
    public void doGetSplits()
    {
        //todo : to add more tests

    }




}
