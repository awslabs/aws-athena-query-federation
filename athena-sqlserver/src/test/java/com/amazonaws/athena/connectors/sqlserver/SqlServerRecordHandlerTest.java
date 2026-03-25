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

package com.amazonaws.athena.connectors.sqlserver;

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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.PARTITION_NUMBER;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;
import static org.mockito.ArgumentMatchers.nullable;

public class SqlServerRecordHandlerTest
{
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_CATALOG_NAME = "testCatalogName";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL4 = "testCol4";
    private static final String TEST_VARCHAR_VALUE = "varcharTest";
    private static final String SYSTEM_QUERY_FUNCTION_NAME = "system.query";
    private static final String COL_ID = "id";
    private static final String COL_NAME = "name";
    private static final String COL_SALARY = "salary";
    private static final String COL_DEPARTMENT = "department";
    private static final String COL_AGE = "age";
    private static final String COL_PRICE = "price";
    private static final String COL_QUANTITY = "quantity";
    private static final String COL_DISCOUNT = "discount";
    private static final String COL_INT = "intCol";
    private static final String COL_VARCHAR = "varcharCol";
    private static final String COL_IS_ACTIVE = "is_active";
    private static final String COL_AMOUNT = "amount";
    private static final String COL_BIGINT = "bigintCol";
    private static final String COL_FLOAT = "floatCol";
    private static final String COL_DOUBLE = "doubleCol";
    private static final String COL_DATE = "dateCol";
    private static final String COL_TIMESTAMP = "timestampCol";
    private static final String COL_BOOL = "boolCol";

    private SqlServerRecordHandler sqlServerRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
            throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER ,new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, SqlServerConstants.NAME,
                "sqlserver://jdbc:sqlserver://hostname;databaseName=fakedatabase");

        this.sqlServerRecordHandler = new SqlServerRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void buildSplitSqlNew()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)).thenReturn("pf");
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN)).thenReturn(TEST_COL1);
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("1");

        ValueSet valueSet = createSingleValueSet(TEST_VARCHAR_VALUE);
        Constraints constraints = createConstraints(
                ImmutableMap.of(TEST_COL4, valueSet)
        );

        String expectedSql = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\" FROM \"testSchema\".\"testTable\"  WHERE (\"testCol4\" = ?) AND  $PARTITION.pf(testCol1) = 1";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, TEST_VARCHAR_VALUE);
    }

    @Test
    public void buildSplitSql_ComplexMixedPredicates_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_NAME, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_AGE, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_SALARY, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DEPARTMENT, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_IS_ACTIVE, Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_AMOUNT, Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Create complex constraints with multiple predicate types
        ValueSet idSet = createMultiValueSet(1, 2, 3, 4, 5);
        ValueSet ageSet = createRangeSet(Marker.Bound.EXACTLY, 25, Marker.Bound.EXACTLY, 65);
        ValueSet salarySet = createRangeSet(Marker.Bound.ABOVE, 50000.0, Marker.Bound.BELOW, Double.MAX_VALUE);
        ValueSet deptSet = createSingleValueSet("IT");
        ValueSet activeSet = createSingleValueSet(true);
        ValueSet amountSet = createSingleValueSet(1234.56);

        Constraints constraints = createConstraints(
                ImmutableMap.of(
                        COL_ID, idSet,
                        COL_AGE, ageSet,
                        COL_SALARY, salarySet,
                        COL_DEPARTMENT, deptSet,
                        COL_IS_ACTIVE, activeSet,
                        COL_AMOUNT, amountSet
                )
        );

        String expectedSql = "SELECT \"id\", \"name\", \"age\", \"salary\", \"department\", \"is_active\", \"amount\" FROM \"testSchema\".\"testTable\"  WHERE (\"id\" IN (?,?,?,?,?)) AND ((\"age\" >= ? AND \"age\" <= ?)) AND ((\"salary\" > ? AND \"salary\" < ?)) AND (\"department\" = ?) AND (\"is_active\" = ?) AND (\"amount\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(3, 3);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(4, 4);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(5, 5);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(6, 25);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(7, 65);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(8, 50000.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(9, Double.MAX_VALUE);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(10, "IT");
        Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(11, true);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(12, 1234.56);
    }

    @Test
    public void buildSplitSql_RangePredicates_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_PRICE, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_QUANTITY, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DISCOUNT, Types.MinorType.FLOAT4.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Complex range predicates with valid bounds
        ValueSet priceSet = createRangeSet(Marker.Bound.ABOVE, 100.0, Marker.Bound.EXACTLY, 1000.0);
        ValueSet quantitySet = createRangeSet(Marker.Bound.EXACTLY, 10, Marker.Bound.BELOW, 100);
        ValueSet discountSet = createRangeSet(Marker.Bound.ABOVE, 0.05f, Marker.Bound.BELOW, 0.5f);

        Constraints constraints = createConstraints(
                ImmutableMap.of(
                        COL_PRICE, priceSet,
                        COL_QUANTITY, quantitySet,
                        COL_DISCOUNT, discountSet
                )
        );

        String expectedSql = "SELECT \"price\", \"quantity\", \"discount\" FROM \"testSchema\".\"testTable\"  WHERE ((\"price\" > ? AND \"price\" <= ?)) AND ((\"quantity\" >= ? AND \"quantity\" < ?)) AND ((\"discount\" > ? AND \"discount\" < ?))";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameter setting for range constraints
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(1, 100.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(2, 1000.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(3, 10);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(4, 100);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(5, 0.05f);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(6, 0.5f);
    }

    @Test
    public void buildSplitSql_OrderBy_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_NAME, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_SALARY, Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        OrderByField orderByField = new OrderByField(COL_SALARY, OrderByField.Direction.DESC_NULLS_LAST);
        Constraints constraints = createConstraints(
                Collections.emptyMap(),
                ImmutableList.of(orderByField),
                10
        );

        String expectedSql = "SELECT \"id\", \"name\", \"salary\" FROM \"testSchema\".\"testTable\"  ORDER BY \"salary\" DESC NULLS LAST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_MultipleOrderByFields_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DEPARTMENT, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_SALARY, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_NAME, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Multiple ORDER BY fields with different directions
        OrderByField orderByField1 = new OrderByField(COL_DEPARTMENT, OrderByField.Direction.ASC_NULLS_LAST);
        OrderByField orderByField2 = new OrderByField(COL_SALARY, OrderByField.Direction.DESC_NULLS_FIRST);
        OrderByField orderByField3 = new OrderByField(COL_NAME, OrderByField.Direction.ASC_NULLS_LAST);

        Constraints constraints = createConstraints(
                Collections.emptyMap(),
                ImmutableList.of(orderByField1, orderByField2, orderByField3),
                Constraints.DEFAULT_NO_LIMIT
        );

        String expectedSql = "SELECT \"department\", \"salary\", \"name\" FROM \"testSchema\".\"testTable\"  ORDER BY \"department\" ASC NULLS LAST, \"salary\" DESC NULLS FIRST, \"name\" ASC NULLS LAST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_EmptyConstraints_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        Constraints constraints = createConstraints(Collections.emptyMap());

        String expectedSql = "SELECT \"id\" FROM \"testSchema\".\"testTable\" ";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_SingleValueInClause_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Single value should use equality, not IN clause
        ValueSet singleValueSet = createSingleValueSet(1);
        Constraints constraints = createConstraints(ImmutableMap.of(COL_ID, singleValueSet));

        String expectedSql = "SELECT \"id\" FROM \"testSchema\".\"testTable\"  WHERE (\"id\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameter setting for single value
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
    }

    @Test
    public void buildSplitSql_LargeInClause_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Large IN clause with many values
        ValueSet largeInSet = createMultiValueSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        Constraints constraints = createConstraints(ImmutableMap.of(COL_ID, largeInSet));

        String expectedSql = "SELECT \"id\" FROM \"testSchema\".\"testTable\"  WHERE (\"id\" IN (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?))";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        for (int i = 1; i <= 15; i++) {
            Mockito.verify(preparedStatement, Mockito.times(1)).setInt(i, i);
        }
    }

    @Test(expected = SQLException.class)
    public void buildSplitSql_InvalidConnection_ThrowsSQLException() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build())
                .build();
        Split split = createTestSplit();
        Constraints constraints = createConstraints(Collections.emptyMap());
        Connection invalidConnection = Mockito.mock(Connection.class);
        Mockito.when(invalidConnection.prepareStatement(Mockito.anyString())).thenThrow(new SQLException("Connection error"));

        this.sqlServerRecordHandler.buildSplitSql(invalidConnection, TEST_CATALOG, tableName, schema, constraints, split);
    }
    
    @Test
    public void buildSplitSql_LimitConstraint_IgnoresLimit() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_NAME, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Test LIMIT constraint (should be ignored by SQL Server)
        Constraints constraints = createConstraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                100L
        );

        // SQL Server doesn't support LIMIT, so it should be ignored
        String expectedSql = "SELECT \"id\", \"name\" FROM \"testSchema\".\"testTable\" ";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_QueryPassthrough_ReturnsPassthroughQuery() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");

        String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", TEST_SCHEMA, TEST_TABLE, TEST_COL1);
        Map<String, String> queryPassthroughArgs = new com.google.common.collect.ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(QUERY, testQuery)
                .put(SCHEMA_FUNCTION_NAME, SYSTEM_QUERY_FUNCTION_NAME)
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", SqlServerConstants.NAME)
                .put("schema", TEST_SCHEMA)
                .build();

        Constraints constraints = createConstraintsWithQueryPassthrough(queryPassthroughArgs);

        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(testQuery);
        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(this.connection).prepareStatement(testQuery);
    }

    @Test
    public void buildSplitSql_PassthroughDisabled_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createTestSplit();

        ValueSet intValueSet = createSingleValueSet(123);
        ValueSet varcharValueSet = createSingleValueSet("abc");

        Constraints constraints = createConstraints(ImmutableMap.of(COL_INT, intValueSet, COL_VARCHAR, varcharValueSet)
        );

        Assert.assertFalse("Expected isQueryPassThrough to return false", constraints.isQueryPassThrough());

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" = ?) AND (\"varcharCol\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement result = this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, 123);
        Mockito.verify(result, Mockito.times(1)).setString(2, "abc");
        Mockito.verify(this.connection).prepareStatement(expectedSql);
    }

    @Test
    public void buildSplitSql_PassthroughEnabledMissingQuery_ThrowsException() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createTestSplit();

        Map<String, String> queryPassthroughArgs = new com.google.common.collect.ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(SCHEMA_FUNCTION_NAME, SYSTEM_QUERY_FUNCTION_NAME)
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", SqlServerConstants.NAME)
                .put("schema", TEST_SCHEMA)
                .build();

        Constraints constraints = createConstraintsWithQueryPassthrough(queryPassthroughArgs);

        Assert.assertTrue("Expected isQueryPassThrough to return true", constraints.isQueryPassThrough());

        try {
            this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);
            Assert.fail("Expected exception to be thrown");
        }
        catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Missing Query Passthrough Argument"));
        }
    }

    @Test
    public void buildSplitSql_PassthroughWrongSchema_ThrowsException() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createTestSplit();

        String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", "testSchema", "testTable", "testCol1");
        Map<String, String> queryPassthroughArgs = new com.google.common.collect.ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(QUERY, testQuery)
                .put(SCHEMA_FUNCTION_NAME, "wrong.function")
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", SqlServerConstants.NAME)
                .put("schema", TEST_SCHEMA)
                .build();

        Constraints constraints = createConstraintsWithQueryPassthrough(queryPassthroughArgs);

        Assert.assertTrue("Expected isQueryPassThrough to return true", constraints.isQueryPassThrough());

        try {
            this.sqlServerRecordHandler.buildSplitSql(this.connection, TEST_CATALOG_NAME, tableName, schema, constraints, split);
            Assert.fail("Expected exception to be thrown");
        }
        catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Function Signature doesn't match implementation's"));
        }
    }

    private Schema createTestSchema()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_INT, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_VARCHAR, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_BIGINT, Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_FLOAT, Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DOUBLE, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DATE, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_TIMESTAMP, Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_BOOL, Types.MinorType.BIT.getType()).build());
        return schemaBuilder.build();
    }
    
    private ValueSet createSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
    
    private ValueSet createMultiValueSet(Object... values)
    {
        java.util.List<Range> ranges = new java.util.ArrayList<>();
        for (Object value : values) {
            Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(range.isSingleValue()).thenReturn(true);
            Mockito.when(range.getLow().getValue()).thenReturn(value);
            ranges.add(range);
        }
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(ranges);
        return valueSet;
    }
    
    private ValueSet createRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
    
    private Split createTestSplit()
    {
        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(PARTITION_NUMBER, "0");
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(PARTITION_NUMBER))).thenReturn("0");
        Mockito.when(split.getProperty(Mockito.anyString())).thenReturn("0");
        return split;
    }
    
    private Constraints createConstraints(Map<String, ValueSet> summary)
    {
        return createConstraints(
                summary,
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT
        );
    }

    private Constraints createConstraints(
            Map<String, ValueSet> summary,
            java.util.List<OrderByField> orderByFields,
            long limit)
    {
        return new Constraints(
                summary != null ? summary : Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                limit,
                Collections.emptyMap(),
                null
        );
    }

    private Constraints createConstraintsWithQueryPassthrough(Map<String, String> queryPassthroughArgs)
    {
        return new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                queryPassthroughArgs,
                null
        );
    }
    
    private PreparedStatement createMockPreparedStatement(String expectedSql) throws SQLException
    {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql)))
                .thenReturn(preparedStatement);
        Mockito.when(preparedStatement.getConnection()).thenReturn(this.connection);
        return preparedStatement;
    }
}
