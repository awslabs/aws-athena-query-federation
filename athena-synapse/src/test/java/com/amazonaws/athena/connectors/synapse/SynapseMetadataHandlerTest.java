/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

public class SynapseMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SynapseMetadataHandlerTest.class);
    private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SynapseConstants.NAME,
    		  "synapse://jdbc:sqlserver://hostname;databaseName=fakedatabase");
    private SynapseMetadataHandler synapseMetadataHandler;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;
    private FederatedIdentity federatedIdentity;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;

    @Before
    public void setup()
            throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        logger.info(" this.connection.."+ this.connection);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"user\": \"testUser\", \"password\": \"testPassword\"}"));
        this.synapseMetadataHandler = new SynapseMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of("spill_bucket", "asdf_spill_bucket_loc"));
        this.federatedIdentity = FederatedIdentity.newBuilder().build();
    }

    @Test
    public void getPartitionSchema()
    {
        Assert.assertEquals(SchemaBuilder.newBuilder()
                        .addField(SynapseMetadataHandler.PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
                this.synapseMetadataHandler.getPartitionSchema("testCatalogName"));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();

        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionColumns(partitionCols).build();

        String[] columns = {"ROW_COUNT", SynapseMetadataHandler.PARTITION_NUMBER, SynapseMetadataHandler.PARTITION_COLUMN, "PARTITION_BOUNDARY_VALUE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{2, null, null, null}, {0, "1", "id", "100000" }, {0, "2", "id", "300000"}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Statement st = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(st);
        Mockito.when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()), i));
        }
        Assert.assertEquals(Arrays.asList("[PARTITION_NUMBER : 1::: :::100000:::id]","[PARTITION_NUMBER : 2:::100000:::300000:::id]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(SynapseMetadataHandler.PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());
    }

    @Test
    public void doGetTableLayoutWithNoPartitions()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionColumns(partitionCols).build();

        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(new String[] {"ROW_COUNT"}, new int[] {Types.INTEGER}, values, new AtomicInteger(-1));

        Statement st = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(st);
        Mockito.when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        Assert.assertEquals(values.length, ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getRowCount());

        List<String> actualValues = new ArrayList<>();
        for (int i = 0; i < ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getRowCount(); i++) {
            actualValues.add(BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()), i));
        }

        Assert.assertEquals(Collections.singletonList("[PARTITION_NUMBER : 0]"), actualValues);

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(SynapseMetadataHandler.PARTITION_NUMBER, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema expectedSchema = expectedSchemaBuilder.build();
        Assert.assertEquals(expectedSchema, ProtobufMessageConverter.fromProtoBlock(blockAllocator, getTableLayoutResponse.getPartitions()).getSchema());
        Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void doGetTableLayoutWithSQLException()
            throws Exception
    {
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionColumns(partitionCols).build();

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(connection);
        Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
        SynapseMetadataHandler synapseMetadataHandler = new SynapseMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of());

        synapseMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();

        String[] columns = {"ROW_COUNT", SynapseMetadataHandler.PARTITION_NUMBER, SynapseMetadataHandler.PARTITION_COLUMN, "PARTITION_BOUNDARY_VALUE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{2, null, null, null}, {0, 1, "id", "0"}, {0, 2, "id", "105"}, {0, 3, "id", "327"}, {0, 4, "id", null}};

        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Statement st = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(st);
        Mockito.when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionColumns(partitionCols).build();

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = GetSplitsRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setPartitions(getTableLayoutResponse.getPartitions()).addAllPartitionColumns(new ArrayList<>(partitionCols)).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).build();
        GetSplitsResponse getSplitsResponse = this.synapseMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        // TODO: Not sure why this is a set of maps, but I'm not going to change it
        // other than mechanically making it java 8 compatible
        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                "PARTITION_BOUNDARY_FROM", " ",
                SynapseMetadataHandler.PARTITION_NUMBER, "1",
                "PARTITION_COLUMN", "id",
                "PARTITION_BOUNDARY_TO", "0"),
            com.google.common.collect.ImmutableMap.of(
                "PARTITION_BOUNDARY_FROM", "0",
                SynapseMetadataHandler.PARTITION_NUMBER, "2",
                "PARTITION_COLUMN", "id",
                "PARTITION_BOUNDARY_TO", "105"),
            com.google.common.collect.ImmutableMap.of(
                "PARTITION_BOUNDARY_FROM", "105",
                SynapseMetadataHandler.PARTITION_NUMBER, "3",
                "PARTITION_COLUMN", "id",
                "PARTITION_BOUNDARY_TO", "327"),
            com.google.common.collect.ImmutableMap.of(
                "PARTITION_BOUNDARY_FROM", "327",
                SynapseMetadataHandler.PARTITION_NUMBER, "4",
                "PARTITION_COLUMN", "id",
                "PARTITION_BOUNDARY_TO", "null")
        );

        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplitsList().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplitsList().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsWithNoPartition()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();

        Object[][] values = {{}};
        ResultSet resultSet = mockResultSet(new String[] {"ROW_COUNT"}, new int[] {Types.INTEGER}, values, new AtomicInteger(-1));

        Statement st = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(st);
        Mockito.when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionColumns(partitionCols).build();

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = GetSplitsRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setPartitions(getTableLayoutResponse.getPartitions()).addAllPartitionColumns(new ArrayList<>(partitionCols)).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).build();
        GetSplitsResponse getSplitsResponse = this.synapseMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = new HashSet<>();
        expectedSplits.add(Collections.singletonMap(SynapseMetadataHandler.PARTITION_NUMBER, "0"));
        Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplitsList().size());
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplitsList().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetSplitsContinuation()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        Constraints constraints = Mockito.mock(Constraints.class);
        TableName tableName = TableName.newBuilder().setSchemaName("testSchema").setTableName("testTable").build();
        Schema partitionSchema = this.synapseMetadataHandler.getPartitionSchema("testCatalogName");
        Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(partitionSchema)).addAllPartitionColumns(partitionCols).build();

        String[] columns = {"ROW_COUNT", SynapseMetadataHandler.PARTITION_NUMBER, SynapseMetadataHandler.PARTITION_COLUMN, "PARTITION_BOUNDARY_VALUE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{2, null, null, null}, {0, 1, "id", "0"}, {0, 2, "id", "105"}, {0, 3, "id", "327"}, {0, 4, "id", null}};

        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        Statement st = Mockito.mock(Statement.class);
        Mockito.when(this.connection.createStatement()).thenReturn(st);
        Mockito.when(st.executeQuery(nullable(String.class))).thenReturn(resultSet);

        GetTableLayoutResponse getTableLayoutResponse = this.synapseMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

        BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
        GetSplitsRequest getSplitsRequest = GetSplitsRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalogName").setTableName(tableName).setPartitions(getTableLayoutResponse.getPartitions()).addAllPartitionColumns(new ArrayList<>(partitionCols)).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setContinuationToken("2").build();
        GetSplitsResponse getSplitsResponse = this.synapseMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

        Set<Map<String, String>> expectedSplits = com.google.common.collect.ImmutableSet.of(
            com.google.common.collect.ImmutableMap.of(
                "PARTITION_BOUNDARY_FROM", "105",
                SynapseMetadataHandler.PARTITION_NUMBER, "3",
                "PARTITION_COLUMN", "id",
                "PARTITION_BOUNDARY_TO", "327"),
            com.google.common.collect.ImmutableMap.of(
                "PARTITION_BOUNDARY_FROM", "327",
                SynapseMetadataHandler.PARTITION_NUMBER, "4",
                "PARTITION_COLUMN", "id",
                "PARTITION_BOUNDARY_TO", "null"));
        Set<Map<String, String>> actualSplits = getSplitsResponse.getSplitsList().stream().map(Split::getProperties).collect(Collectors.toSet());
        Assert.assertEquals(expectedSplits, actualSplits);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        Schema PARTITION_SCHEMA = SchemaBuilder.newBuilder().addField("PARTITION_NUMBER", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();

        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"DATA_TYPE", "COLUMN_NAME", "PRECISION", "SCALE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{Types.INTEGER, "testCol1", 0, 0}, {Types.VARCHAR, "testCol2", 0, 0},
                    {Types.TIMESTAMP, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, "testCol4", 0, 0}};
        ResultSet resultSet = mockResultSet(schema, types, values, new AtomicInteger(-1));

        String[] columns = {"DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "COLUMN_NAME"};
        int[] types2 = {Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR};
        Object[][] values2 = {{Types.INTEGER, 12, 0, "testCol1"}, {Types.VARCHAR, 25, 0, "testCol2"},
                {Types.TIMESTAMP, 93, 0, "testCol3"}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, 0, "testCol4"}};
        ResultSet resultSet2 = mockResultSet(columns, types2, values2, new AtomicInteger(-1));

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        PARTITION_SCHEMA.getFields().forEach(expectedSchemaBuilder::addField);
        Schema expected = expectedSchemaBuilder.build();

        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(nullable(String.class))).thenReturn(stmt);
        Mockito.when(stmt.executeQuery()).thenReturn(resultSet);

        Mockito.when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://hostname;databaseName=fakedatabase");

        TableName inputTableName = TableName.newBuilder().setSchemaName("TESTSCHEMA").setTableName("TESTTABLE").build();
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet2);

        GetTableResponse getTableResponse = this.synapseMetadataHandler.doGetTable(
                blockAllocator, GetTableRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalog").setTableName(inputTableName).build());
        Assert.assertEquals(expected, ProtobufMessageConverter.fromProtoSchema(blockAllocator, getTableResponse.getSchema()));
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }

    @Test
    public void doDataTypeConversion()
            throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        String[] schema = {"DATA_TYPE", "COLUMN_NAME", "PRECISION", "SCALE"};
        int[] types = {Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        Object[][] values = {{Types.INTEGER, "testCol1", 0, 0}, {Types.VARCHAR, "testCol2", 0, 0},
                {Types.TIMESTAMP, "testCol3", 0, 0}, {Types.TIMESTAMP_WITH_TIMEZONE, "testCol4", 0, 0}};
        ResultSet resultSet = mockResultSet(schema, types, values, new AtomicInteger(-1));

        String[] columns = {"DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "COLUMN_NAME"};
        int[] types2 = {Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR};
        Object[][] values2 = {{Types.INTEGER, 12, 0, "testCol1"}, {Types.VARCHAR, 25, 0, "testCol2"},
                {Types.TIMESTAMP, 93, 0, "testCol3"}, {Types.TIMESTAMP_WITH_TIMEZONE, 93, 0, "testCol4"}};
        ResultSet resultSet2 = mockResultSet(columns, types2, values2, new AtomicInteger(-1));

        PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(nullable(String.class))).thenReturn(stmt);
        Mockito.when(stmt.executeQuery()).thenReturn(resultSet);

        Mockito.when(connection.getMetaData().getURL()).thenReturn("jdbc:sqlserver://hostname-ondemand;databaseName=fakedatabase");

        TableName inputTableName = TableName.newBuilder().setSchemaName("TESTSCHEMA").setTableName("TESTTABLE").build();
        Mockito.when(connection.getCatalog()).thenReturn("testCatalog");
        Mockito.when(connection.getMetaData().getColumns("testCatalog", inputTableName.getSchemaName(), inputTableName.getTableName(), null)).thenReturn(resultSet2);

        GetTableResponse getTableResponse = this.synapseMetadataHandler.doGetTable(
                blockAllocator, GetTableRequest.newBuilder().setIdentity(this.federatedIdentity).setQueryId("testQueryId").setCatalogName("testCatalog").setTableName(inputTableName).build());
        Assert.assertEquals(inputTableName, getTableResponse.getTableName());
        Assert.assertEquals("testCatalog", getTableResponse.getCatalogName());
    }
}
