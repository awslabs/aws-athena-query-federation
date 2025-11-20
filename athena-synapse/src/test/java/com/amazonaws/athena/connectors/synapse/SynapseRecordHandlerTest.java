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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static com.amazonaws.athena.connectors.synapse.SynapseConstants.QUOTE_CHARACTER;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SynapseRecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_CATALOG_NAME = "testCatalogName";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_DB_NAME = "fakedatabase";
    private static final String TEST_HOSTNAME = "hostname";
    private static final String TEST_JDBC_URL = String.format("synapse://jdbc:sqlserver://%s;databaseName=%s", TEST_HOSTNAME, TEST_DB_NAME);
    private static final TableName TEST_TABLE_NAME = new TableName(TEST_SCHEMA, TEST_TABLE);
    
    // Test column names
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_COL3 = "testCol3";
    private static final String TEST_COL4 = "testCol4";
    private static final String TEST_ID_COL = "id";
    private static final String TEST_NAME_COL = "name";
    private static final String TEST_CREATED_AT_COL = "created_at";
    
    // Test values
    private static final String TEST_VARCHAR_VALUE = "varcharTest";
    private static final String TEST_PARTITION_FROM = "100000";
    private static final String TEST_PARTITION_TO = "300000";
    private static final int TEST_ID_1 = 123;
    private static final int TEST_ID_2 = 124;
    private static final String TEST_NAME_1 = "test1";
    private static final String TEST_NAME_2 = "test2";
    private static final String COL_ID = "id";
    private static final String COL_NAME = "name";
    private static final String COL_VALUE = "value";
    private static final String COL_INT = "intCol";
    private static final String COL_DOUBLE = "doubleCol";
    private static final String COL_STRING = "stringCol";

    private SynapseRecordHandler synapseRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private FederatedIdentity federatedIdentity;

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.federatedIdentity = mock(FederatedIdentity.class);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new SynapseQueryStringBuilder(QUOTE_CHARACTER, new SynapseFederationExpressionParser(QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, SynapseConstants.NAME,
                TEST_JDBC_URL);

        this.synapseRecordHandler = new SynapseRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.DATEDAY.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.DATEMILLI.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.VARCHAR.getType()).build())
                .build();

        Split split = Mockito.mock(Split.class);
        when(split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn(TEST_ID_COL);
        when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn(TEST_PARTITION_FROM);
        when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn(TEST_PARTITION_TO);

        ValueSet valueSet = getSingleValueSet(TEST_VARCHAR_VALUE);
        Constraints constraints = Mockito.mock(Constraints.class);
        when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put(TEST_COL4, valueSet)
                .build());

        when(constraints.getLimit()).thenReturn(5L);

        String expectedSql = "SELECT \"" + TEST_COL1 + "\", \"" + TEST_COL2 + "\", \"" + TEST_COL3 + "\", \"" + TEST_COL4 + "\" FROM \"" + TEST_SCHEMA + "\".\"" + TEST_TABLE + "\"  WHERE (\"" + TEST_COL4 + "\" = ?) AND " + TEST_ID_COL + " > " + TEST_PARTITION_FROM + " and " + TEST_ID_COL + " <= " + TEST_PARTITION_TO;
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verify(preparedStatement, Mockito.times(1)).setString(1, TEST_VARCHAR_VALUE);
    }

    @Test
    public void buildSplitSqlWithPartition()
            throws SQLException
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.DATEDAY.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.DATEMILLI.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.VARBINARY.getType()).build())
                .addField(FieldBuilder.newBuilder("partition", Types.MinorType.VARCHAR.getType()).build())
                .build();

        // Test case 1: Normal partition boundaries
        Split split = mockSplitWithPartitionProperties("0", "100000", "1");
        Constraints constraints = Mockito.mock(Constraints.class);
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        // Test case 2: Empty from boundary
        split = mockSplitWithPartitionProperties(" ", "100000", "1");
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        // Test case 3: Empty to boundary
        split = mockSplitWithPartitionProperties("300000", " ", "2");
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        // Test case 4: Both boundaries empty
        split = mockSplitWithPartitionProperties(" ", " ", "2");
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);
    }

    @Test
    public void readWithConstraint_WithValidData_ProcessesRows() throws Exception {
            Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(TEST_ID_COL, Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_NAME_COL, Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_CREATED_AT_COL, Types.MinorType.DATEMILLI.getType()).build())
                .build();

            Split split = Mockito.mock(Split.class);
            when(split.getProperties()).thenReturn(Collections.emptyMap());

            // Setup mock result set with actual test data
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            when(resultSet.next()).thenReturn(true, true, false); // Return true twice for two rows, then false
            when(resultSet.getInt(TEST_ID_COL)).thenReturn(TEST_ID_1, TEST_ID_2);
            when(resultSet.getString(TEST_NAME_COL)).thenReturn(TEST_NAME_1, TEST_NAME_2);
            when(resultSet.getTimestamp(TEST_CREATED_AT_COL)).thenReturn(new Timestamp(System.currentTimeMillis()));

            PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
            when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
            when(preparedStatement.executeQuery()).thenReturn(resultSet);

            DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
            when(connection.getMetaData()).thenReturn(metaData);
            when(metaData.getURL()).thenReturn("jdbc:sqlserver://test.sql.azuresynapse.net:1433;databaseName=testdb;");

            ReadRecordsRequest request = new ReadRecordsRequest(
                    federatedIdentity,
                    TEST_CATALOG,
                    TEST_QUERY_ID,
                    TEST_TABLE_NAME,
                    schema,
                    split,
                    new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 1000, Collections.emptyMap(),null),
                    0,
                    0
            );

            BlockSpiller spiller = Mockito.mock(BlockSpiller.class);
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            // Execute the test
            synapseRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);

            // Verify that writeRows was called twice (once for each row)
            verify(spiller, Mockito.times(2)).writeRows(Mockito.any());
            verify(resultSet, Mockito.times(3)).next(); // Called 3 times (2 true, 1 false)

    }

    @Test
    public void buildSplitSql_WithOrderBy_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createSchemaWithCommonFields();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_VALUE, Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();
        Split split = createMockSplit();

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COL_VALUE, OrderByField.Direction.DESC_NULLS_LAST));
        orderByFields.add(new OrderByField(COL_NAME, OrderByField.Direction.ASC_NULLS_LAST));

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"name\", \"value\" FROM \"testSchema\".\"testTable\"  WHERE id > 100000 and id <= 300000 ORDER BY \"value\" DESC NULLS LAST, \"name\" ASC NULLS LAST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_WithComplexExpressions_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createSchemaWithCommonFields();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DOUBLE, Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit();
        ValueSet nameValueSet = getRangeSet(Marker.Bound.EXACTLY, "test", Marker.Bound.BELOW, "tesu");
        ValueSet doubleValueSet = getRangeSet(Marker.Bound.EXACTLY, 1.0d, Marker.Bound.EXACTLY, 2.0d);

        Constraints constraints = new Constraints(
                new ImmutableMap.Builder<String, ValueSet>()
                        .put(COL_NAME, nameValueSet)
                        .put(COL_DOUBLE, doubleValueSet)
                        .build(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"name\", \"doubleCol\" FROM \"testSchema\".\"testTable\"  WHERE ((\"name\" >= ? AND \"name\" < ?)) AND ((\"doubleCol\" >= ? AND \"doubleCol\" <= ?)) AND id > 100000 and id <= 300000";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "test");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, "tesu");
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(3, 1.0d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(4, 2.0d);
    }

    @Test
    public void buildSplitSql_WithValueComparisons_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createSchemaWithCommonFields();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_INT, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();
        Split split = createMockSplit();

        ValueSet stringValueSet = getSingleValueSet("testValue");
        ValueSet intValueSet = getSingleValueSet(42);

        Map<String, ValueSet> summary = new ImmutableMap.Builder<String, ValueSet>()
                .put(COL_NAME, stringValueSet)
                .put(COL_INT, intValueSet)
                .build();

        Constraints constraints = new Constraints(
                summary,
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null);

        String expectedSql = "SELECT \"id\", \"name\", \"intCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"name\" = ?) AND (\"intCol\" = ?) AND id > 100000 and id <= 300000";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "testValue");
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 42);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_WithEmptyConstraints_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithCommonFields().build();
        Split split = createMockSplit();
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"" + COL_ID + "\", \"" + COL_NAME + "\" FROM \"testSchema\".\"testTable\"  WHERE id > 100000 and id <= 300000";
        PreparedStatement preparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement result = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(preparedStatement, result);
        verifyFetchSize(preparedStatement);
    }

    @Test
    public void buildSplitSql_WithLimitOffset_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithValueField().build();
        Split split = createMockSplit();

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                5L,
                Collections.emptyMap(),
                null
        );

        // Expected SQL should NOT contain LIMIT clause as Synapse does not support LIMIT clause
        String expectedSql = "SELECT \"id\", \"name\", \"value\" FROM \"testSchema\".\"testTable\"  WHERE id > 100000 and id <= 300000";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_WithRangeAndInPredicates_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createSchemaWithCommonFields();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_INT, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DOUBLE, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_STRING, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit();

        ValueSet intValueSet = getSingleValueSet(Arrays.asList(1, 2, 3));
        ValueSet doubleValueSet = getRangeSet(Marker.Bound.EXACTLY, 1.5d, Marker.Bound.BELOW, 5.5d);
        ValueSet stringValueSet = getSingleValueSet(Arrays.asList("value1", "value2"));

        Map<String, ValueSet> summary = new ImmutableMap.Builder<String, ValueSet>()
                .put(COL_INT, intValueSet)
                .put(COL_DOUBLE, doubleValueSet)
                .put(COL_STRING, stringValueSet)
                .build();

        Constraints constraints = new Constraints(
                summary,
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"name\", \"intCol\", \"doubleCol\", \"stringCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" IN (?,?,?)) AND ((\"doubleCol\" >= ? AND \"doubleCol\" < ?)) AND (\"stringCol\" IN (?,?)) AND id > 100000 and id <= 300000";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);

        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(3, 3);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(4, 1.5d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(5, 5.5d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(6, "value1");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(7, "value2");
    }

    @Test
    public void buildSplitSql_WithQueryPassthrough_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithCommonFields().build();

        Split split = createMockSplit();

        String passthroughQuery = "SELECT * FROM testSchema.testTable WHERE id > 100";
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(QUERY, passthroughQuery);
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                passthroughArgs,
                null);

        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(passthroughQuery);

        PreparedStatement result = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, result);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test(expected = AthenaConnectorException.class)
    public void buildSplitSql_WithInvalidQueryPassthrough_ThrowsAthenaConnectorException() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createSchemaWithCommonFields();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit();

        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(QUERY, "SELECT * FROM table");
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                passthroughArgs,
                null);

        synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
    }

    @Test
    public void buildSplitSql_WithComplexConstraintsAndOrderBy_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createSchemaWithValueField();
        schemaBuilder.addField(FieldBuilder.newBuilder("dateCol", Types.MinorType.DATEDAY.getType()).build());
        Schema schema = schemaBuilder.build();
        Split split = createMockSplit();

        ValueSet valueRangeSet = getRangeSet(Marker.Bound.ABOVE, 10.0, Marker.Bound.EXACTLY, 100.0);
        ValueSet nameValueSet = getSingleValueSet("testName");

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COL_ID, OrderByField.Direction.ASC_NULLS_LAST));
        orderByFields.add(new OrderByField(COL_VALUE, OrderByField.Direction.DESC_NULLS_LAST));

        Constraints constraints = new Constraints(
                new ImmutableMap.Builder<String, ValueSet>()
                        .put(COL_VALUE, valueRangeSet)
                        .put(COL_NAME, nameValueSet)
                        .build(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"name\", \"value\", \"dateCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"name\" = ?) AND ((\"value\" > ? AND \"value\" <= ?)) AND id > 100000 and id <= 300000 ORDER BY \"id\" ASC NULLS LAST, \"value\" DESC NULLS LAST" ;
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "testName");
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(2, 10.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(3, 100.0);
    }

    @Test
    public void buildSplitSql_WithEmptyConstraintsAndOrderBy_ReturnsCorrectSql() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithCommonFields().build();
        Split split = createMockSplit();

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COL_ID, OrderByField.Direction.ASC_NULLS_LAST));
        orderByFields.add(new OrderByField(COL_NAME, OrderByField.Direction.DESC_NULLS_LAST));

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"" + COL_ID + "\", \"" + COL_NAME + "\" FROM \"testSchema\".\"testTable\"  WHERE id > 100000 and id <= 300000 ORDER BY \"" + COL_ID + "\" ASC NULLS LAST, \"" + COL_NAME + "\" DESC NULLS LAST";
        PreparedStatement preparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement result = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(preparedStatement, result);
        verifyFetchSize(preparedStatement);
    }


    private Split mockSplitWithPartitionProperties(String from, String to, String partitionNumber)
    {
        Split split = Mockito.mock(Split.class);
        ImmutableMap<String, String> properties = ImmutableMap.of(
                SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, from,
                SynapseMetadataHandler.PARTITION_NUMBER, partitionNumber,
                SynapseMetadataHandler.PARTITION_COLUMN, TEST_COL1,
                SynapseMetadataHandler.PARTITION_BOUNDARY_TO, to
        );
        when(split.getProperties()).thenReturn(properties);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM))).thenReturn(from);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_NUMBER))).thenReturn(partitionNumber);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_COLUMN))).thenReturn(TEST_COL1);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_BOUNDARY_TO))).thenReturn(to);
        return split;
    }

    private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private SchemaBuilder createSchemaWithCommonFields() {
        return SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(COL_NAME, Types.MinorType.VARCHAR.getType()).build());
    }

    private Split createMockSplit() {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn(COL_ID);
        Mockito.when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("100000");
        Mockito.when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("300000");
        return split;
    }

    private PreparedStatement createMockPreparedStatement(String expectedSql) throws SQLException {
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        return expectedPreparedStatement;
    }

    private void verifyFetchSize(PreparedStatement preparedStatement) throws SQLException {
        Mockito.verify(preparedStatement, Mockito.atLeastOnce()).setFetchSize(1000);
    }

    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private ValueSet getSingleValueSet(List<?> values) {
        List<Range> ranges = values.stream().map(value -> {
            Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(range.isSingleValue()).thenReturn(true);
            Mockito.when(range.getLow().getValue()).thenReturn(value);
            return range;
        }).collect(Collectors.toList());

        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(ranges);
        return valueSet;
    }


    private SchemaBuilder createSchemaWithValueField() {
        return createSchemaWithCommonFields()
                .addField(FieldBuilder.newBuilder(COL_VALUE, Types.MinorType.FLOAT8.getType()).build());
    }
}
