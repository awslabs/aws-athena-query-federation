/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

public class JdbcMetadataHandlerTest
        extends TestBase
{
    private static final Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField("testPartitionCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();

    private JdbcMetadataHandler jdbcMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private FederatedIdentity federatedIdentity;
    private Connection connection;
    private BlockAllocator blockAllocator;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;

    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "fakedatabase",
                "fakedatabase://jdbc:fakedatabase://hostname/${testSecret}", "testSecret");
        this.jdbcMetadataHandler = new JdbcMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory)
        {
            @Override
            public Schema getPartitionSchema(final String catalogName)
            {
                return PARTITION_SCHEMA;
            }

            @Override
            public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
            {
            }

            @Override
            public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
            {
                return null;
            }
        };
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
    }

    @Test
    public void getJdbcConnectionFactory()
    {
        Assert.assertEquals(this.jdbcConnectionFactory, this.jdbcMetadataHandler.getJdbcConnectionFactory());
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        String[] schema = {"TABLE_SCHEM"};
        Object[][] values = {{"testDB"}, {"testdb2"}, {"information_schema"}};
        String[] expected = {"testDB", "testdb2"};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Mockito.when(connection.getMetaData().getSchemas()).thenReturn(resultSet);
        ListSchemasResponse listSchemasResponse = this.jdbcMetadataHandler.doListSchemaNames(this.blockAllocator, new ListSchemasRequest(this.federatedIdentity, "testQueryId", "testCatalog"));
        Assert.assertArrayEquals(expected, listSchemasResponse.getSchemas().toArray());
    }

    @Test
    public void doListTables()
            throws Exception
    {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"testSchema", "testTable"}, {"testSchema", "testtable2"}};
        TableName[] expected = {new TableName("testSchema", "testTable"), new TableName("testSchema", "testtable2")};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData().getTables("testCatalog", "testSchema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE"})).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        ListTablesResponse listTablesResponse = this.jdbcMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "testSchema", null, UNLIMITED_PAGE_SIZE_VALUE));
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());
    }

    @Test
    public void doListTablesEscaped()
            throws Exception
    {
        String[] schema = {"TABLE_SCHEM", "TABLE_NAME"};
        Object[][] values = {{"test_Schema", "testTable"}, {"test_Schema", "testtable2"}};
        TableName[] expected = {new TableName("test_Schema", "testTable"), new TableName("test_Schema", "testtable2")};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Mockito.when(connection.getMetaData().getTables("testCatalog", "test\\_Schema", null, new String[] {"TABLE", "VIEW", "EXTERNAL TABLE"})).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenReturn("\\");
        ListTablesResponse listTablesResponse = this.jdbcMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId",
                        "testCatalog", "test_Schema", null, UNLIMITED_PAGE_SIZE_VALUE));
        Assert.assertArrayEquals(expected, listTablesResponse.getTables().toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void doListTablesEscapedException()
            throws Exception
    {
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenReturn("_");
        this.jdbcMetadataHandler.doListTables(this.blockAllocator, new ListTablesRequest(this.federatedIdentity,
                "testQueryId", "testCatalog", "test_Schema", null, UNLIMITED_PAGE_SIZE_VALUE));
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
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        TableName inputTableName = new TableName("testSchema", "testTable");
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");

        GetTableResponse getTableResponse = this.jdbcMetadataHandler.doGetTable(
                this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName));

        Assert.assertEquals(expected, getTableResponse.getSchema());
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableNoColumns()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");

        this.jdbcMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName));
    }

    @Test(expected = SQLException.class)
    public void doGetTableSQLException()
            throws Exception
    {
        TableName inputTableName = new TableName("testSchema", "testTable");
        Mockito.when(this.connection.getMetaData().getColumns(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new SQLException());
        this.jdbcMetadataHandler.doGetTable(this.blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName));
    }

    @Test(expected = SQLException.class)
    public void doListSchemaNamesSQLException()
            throws Exception
    {
        Mockito.when(this.connection.getMetaData().getSchemas()).thenThrow(new SQLException());
        this.jdbcMetadataHandler.doListSchemaNames(this.blockAllocator, new ListSchemasRequest(this.federatedIdentity, "testQueryId", "testCatalog"));
    }

    @Test(expected = SQLException.class)
    public void doListTablesSQLException()
            throws Exception
    {
        Mockito.when(this.connection.getMetaData().getTables(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenThrow(new SQLException());
        this.jdbcMetadataHandler.doListTables(this.blockAllocator, new ListTablesRequest(this.federatedIdentity,
                "testQueryId", "testCatalog", "testSchema", null, UNLIMITED_PAGE_SIZE_VALUE));
    }
}
