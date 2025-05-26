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
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import java.sql.Types;
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
        Object[][] values = {{Types.INTEGER, 12, "testCol1", 0, 0}, {Types.VARCHAR, 25, "testCol2", 0, 0},
                {Types.TIMESTAMP, 93, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, "testCol4", 0, 0}};
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
        GetTableResponse getTableResponse = this.dataLakeGen2MetadataHandler.doGetTable(
                blockAllocator, new GetTableRequest(this.federatedIdentity, "testQueryId", "testCatalog", inputTableName, Collections.emptyMap()));

        assertEquals(expected, getTableResponse.getSchema());
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
