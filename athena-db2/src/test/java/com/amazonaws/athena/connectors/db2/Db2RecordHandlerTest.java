/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
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
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.db2.Db2Constants.PARTITION_NUMBER;
import static com.amazonaws.athena.connectors.db2.Db2Constants.QUOTE_CHARACTER;
import static org.mockito.ArgumentMatchers.nullable;

public class Db2RecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_PARTITION = "partition_name";
    private static final String TEST_PARTITION_VALUE = "partition_value";
    private static final String TEST_COL_INT = "intCol";
    private static final String TEST_COL_VARCHAR = "varcharCol";
    private static final String TEST_COL_BIGINT = "bigintCol";
    private static final String TEST_COL_FLOAT = "floatCol";
    private static final String TEST_COL_DOUBLE = "doubleCol";
    private static final String TEST_COL_DATE = "dateCol";
    private static final String TEST_COL_TIMESTAMP = "timestampCol";
    private static final String TEST_COL_BOOLEAN = "boolCol";
    private static final int TEST_INT_VALUE = 100;
    private static final String TEST_VARCHAR_VALUE = "testString";
    private static final float TEST_FLOAT_VALUE = 10.5f;
    private static final double TEST_DOUBLE_VALUE = 20.75d;
    private static final boolean TEST_BOOLEAN_VALUE = true;

    private static final String QPT_SCHEMA_FUNCTION_NAME = "schemaFunctionName";
    private static final String QPT_ENABLE_QUERY_PASSTHROUGH = "enableQueryPassthrough";
    private static final String QPT_NAME = "name";
    private static final String QPT_SCHEMA = "schema";
    private static final String QPT_SYSTEM_QUERY_SIGNATURE = "system.query";
    private static final String QPT_SYSTEM_SCHEMA = "system";
    private static final String QPT_QUERY_NAME = "query";
    private static final String QPT_ENABLE_TRUE = "true";

    private Db2RecordHandler db2RecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup() throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new Db2QueryStringBuilder(QUOTE_CHARACTER, new Db2FederationExpressionParser(QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", Db2Constants.NAME,
                "dbtwo://jdbc:db2://hostname/fakedatabase:${testsecret}");
        this.db2RecordHandler = new Db2RecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    private ValueSet getSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void buildSplitSql_whenCalledWithConstraints_returnsPreparedStatement()
            throws SQLException
    {
        final String testCol4 = "testCol4";

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(testCol4, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("0");

        ValueSet valueSet = getSingleValueSet("varcharTest");
        Constraints constraints = createConstraints(new ImmutableMap.Builder<@NotNull String, @NotNull ValueSet>()
                .put(testCol4, valueSet)
                .build());

        String expectedSql = String.format("SELECT \"%s\", \"testCol2\", \"testCol3\", \"%s\" FROM \"%s\".\"%s\"  WHERE (\"%s\" = ?)", 
                TEST_COL1, testCol4, TEST_SCHEMA, TEST_TABLE, testCol4);
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        PreparedStatement preparedStatement = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "varcharTest");
    }

    @Test
    public void buildSplitSql_withQueryPassthrough_returnsPassthroughQuery() throws SQLException {
            TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
            schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
            schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
            Schema schema = schemaBuilder.build();

            Split split = Mockito.mock(Split.class);
            Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(TEST_PARTITION, TEST_PARTITION_VALUE));
            Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

            String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", TEST_SCHEMA, TEST_TABLE, TEST_COL1);
            Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<@NotNull String, @NotNull String>()
                    .put(JdbcQueryPassthrough.QUERY, testQuery)
                    .put(QPT_SCHEMA_FUNCTION_NAME, QPT_SYSTEM_QUERY_SIGNATURE)
                    .put(QPT_ENABLE_QUERY_PASSTHROUGH, QPT_ENABLE_TRUE)
                    .put(QPT_NAME, QPT_QUERY_NAME)
                    .put(QPT_SCHEMA, QPT_SYSTEM_SCHEMA)
                    .build();

            Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                    Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

            PreparedStatement expectedPreparedStatement = createMockPreparedStatement(testQuery);

            PreparedStatement preparedStatement = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

            Assert.assertEquals(expectedPreparedStatement, preparedStatement);
            Mockito.verify(this.connection).prepareStatement(testQuery);
    }

    @Test
    public void buildSplitSql_withPassthroughDisabled_returnsPreparedStatement() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        ValueSet intValueSet = getSingleValueSet(TEST_INT_VALUE);
        ValueSet varcharValueSet = getSingleValueSet(TEST_VARCHAR_VALUE);

        // Create constraints without queryPassthroughArguments (empty map) - this makes isQueryPassThrough() return false
        Constraints constraints = new Constraints(
                ImmutableMap.of(TEST_COL_INT, intValueSet, TEST_COL_VARCHAR, varcharValueSet),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(), // Empty map means passthrough is disabled
                null);

        Assert.assertFalse("Expected isQueryPassThrough to return false", constraints.isQueryPassThrough());

        // Expected SQL should use the normal split query builder path, not passthrough
        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" = ?) AND (\"varcharCol\" = ?)";

        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, TEST_INT_VALUE);
        Mockito.verify(result, Mockito.times(1)).setString(2, TEST_VARCHAR_VALUE);

        Mockito.verify(this.connection).prepareStatement(expectedSql);
    }

    @Test
    public void buildSplitSql_withPassthroughEnabledButMissingQuery_throwsAthenaConnectorException() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        // Create passthrough arguments without the required QUERY parameter
        Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(QPT_SCHEMA_FUNCTION_NAME, QPT_SYSTEM_QUERY_SIGNATURE)
                .put(QPT_ENABLE_QUERY_PASSTHROUGH, QPT_ENABLE_TRUE)
                .put(QPT_NAME, QPT_QUERY_NAME)
                .put(QPT_SCHEMA, QPT_SYSTEM_SCHEMA)
                // Missing JdbcQueryPassthrough.QUERY parameter
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        // Verify isQueryPassThrough returns true (since we have non-empty queryPassthroughArgs)
        Assert.assertTrue("Expected isQueryPassThrough to return true", constraints.isQueryPassThrough());

        try {
            this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
            Assert.fail("Expected AthenaConnectorException to be thrown");
        } catch (AthenaConnectorException e) {
            Assert.assertTrue("Exception message should contain 'Missing Query Passthrough Argument: QUERY'",
                    e.getMessage().contains("Missing Query Passthrough Argument: QUERY"));
        }
    }

    @Test
    public void buildSplitSql_withPassthroughWithWrongSchemaFunctionName_throwsAthenaConnectorException() throws SQLException {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", TEST_SCHEMA, TEST_TABLE, TEST_COL1);
        
        // Create passthrough arguments with wrong schemaFunctionName (should be "system.query" but using "wrong.function")
        Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(JdbcQueryPassthrough.QUERY, testQuery)
                .put(QPT_SCHEMA_FUNCTION_NAME, "wrong.function") // Wrong schema function name
                .put(QPT_ENABLE_QUERY_PASSTHROUGH, QPT_ENABLE_TRUE)
                .put(QPT_NAME, QPT_QUERY_NAME)
                .put(QPT_SCHEMA, QPT_SYSTEM_SCHEMA)
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        // Verify isQueryPassThrough returns true (since we have non-empty queryPassthroughArgs)
        Assert.assertTrue("Expected isQueryPassThrough to return true", constraints.isQueryPassThrough());

        try {
            this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
            Assert.fail("Expected AthenaConnectorException to be thrown");
        } catch (AthenaConnectorException e) {
            Assert.assertTrue("Exception message should contain 'Function Signature doesn't match implementation's'",
                    e.getMessage().contains("Function Signature doesn't match implementation's"));
        }
    }

    @Test
    public void buildSplitSql_withSingleValueConstraints_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        ValueSet intValueSet = getSingleValueSet(TEST_INT_VALUE);
        Constraints constraints = createConstraints(
                ImmutableMap.of(TEST_COL_INT, intValueSet));

        String expectedSql = String.format("SELECT %s FROM \"%s\".\"%s\"  WHERE (%s = ?)",
                "\"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\"",
                TEST_SCHEMA, TEST_TABLE, "\"intCol\"");
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, TEST_INT_VALUE);
    }

    @Test
    public void buildSplitSql_withInClauseConstraints_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        ValueSet varcharValueSet = getDiscreteValueSet("value1", "value2", "value3");
        Constraints constraints = createConstraints(
                ImmutableMap.of(TEST_COL_VARCHAR, varcharValueSet));

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"varcharCol\" IN (?,?,?))";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setString(1, "value1");
        Mockito.verify(result, Mockito.times(1)).setString(2, "value2");
        Mockito.verify(result, Mockito.times(1)).setString(3, "value3");
    }

    @Test
    public void buildSplitSql_withRangeConstraints_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        ValueSet rangeValueSet = getRangeSet(10, 100);
        Constraints constraints = createConstraints(
                ImmutableMap.of(TEST_COL_INT, rangeValueSet));

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE ((\"intCol\" >= ? AND \"intCol\" < ?))";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, 10);
        Mockito.verify(result, Mockito.times(1)).setInt(2, 100);
    }

    @Test
    public void buildSplitSql_withComplexExpressions_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        ValueSet intValueSet = getSingleValueSet(TEST_INT_VALUE);
        ValueSet varcharValueSet = getSingleValueSet(TEST_VARCHAR_VALUE);
        ValueSet bigintRangeSet = getRangeSet(500L, 1000L);
        
        Constraints constraints = createConstraints(
                ImmutableMap.<String, ValueSet>builder()
                        .put(TEST_COL_INT, intValueSet)
                        .put(TEST_COL_VARCHAR, varcharValueSet)
                        .put(TEST_COL_BIGINT, bigintRangeSet)
                        .build());

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" = ?) AND (\"varcharCol\" = ?) AND ((\"bigintCol\" >= ? AND \"bigintCol\" < ?))";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, TEST_INT_VALUE);
        Mockito.verify(result, Mockito.times(1)).setString(2, TEST_VARCHAR_VALUE);
        Mockito.verify(result, Mockito.times(1)).setLong(3, 500L);
        Mockito.verify(result, Mockito.times(1)).setLong(4, 1000L);
    }

    @Test
    public void buildSplitSql_withOrderBy_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        OrderByField orderByField1 = new OrderByField(TEST_COL_INT, OrderByField.Direction.ASC_NULLS_FIRST);
        OrderByField orderByField2 = new OrderByField(TEST_COL_VARCHAR, OrderByField.Direction.DESC_NULLS_LAST);
        List<OrderByField> orderByFields = ImmutableList.of(orderByField1, orderByField2);
        
        ValueSet intValueSet = getSingleValueSet(100);
        Map<String, ValueSet> constraintMap = ImmutableMap.of(TEST_COL_INT, intValueSet);
        Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), orderByFields,
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);


        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" = ?) ORDER BY \"intCol\" ASC NULLS FIRST, \"varcharCol\" DESC NULLS LAST";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, 100);
    }

    @Test
    public void buildSplitSql_withLimitPushdown_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        long limitValue = 100L;
        ValueSet intValueSet = getSingleValueSet(50);
        Map<String, ValueSet> constraintMap = ImmutableMap.of(TEST_COL_INT, intValueSet);

        Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(),
                limitValue, Collections.emptyMap(), null);

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" = ?) LIMIT 100";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, 50);
    }

    @Test
    public void buildSplitSql_withTopNWithOrderByAndLimit_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        OrderByField orderByField = new OrderByField(TEST_COL_INT, OrderByField.Direction.DESC_NULLS_LAST);
        List<OrderByField> orderByFields = ImmutableList.of(orderByField);
        
        ValueSet varcharValueSet = getSingleValueSet(TEST_VARCHAR_VALUE);
        Map<String, ValueSet> constraintMap = ImmutableMap.of(TEST_COL_VARCHAR, varcharValueSet);
        
        Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), orderByFields,
                50L, Collections.emptyMap(), null);

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"varcharCol\" = ?) ORDER BY \"intCol\" DESC NULLS LAST LIMIT 50";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setString(1, TEST_VARCHAR_VALUE);
    }

    @Test
    public void buildSplitSql_withDifferentDataTypes_buildsCorrectSQL()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        ValueSet floatValueSet = getSingleValueSet(TEST_FLOAT_VALUE);
        ValueSet doubleValueSet = getSingleValueSet(TEST_DOUBLE_VALUE);
        ValueSet booleanValueSet = getSingleValueSet(TEST_BOOLEAN_VALUE);
        
        Constraints constraints = createConstraints(
                ImmutableMap.<String, ValueSet>builder()
                        .put(TEST_COL_FLOAT, floatValueSet)
                        .put(TEST_COL_DOUBLE, doubleValueSet)
                        .put(TEST_COL_BOOLEAN, booleanValueSet)
                        .build());

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"floatCol\" = ?) AND (\"doubleCol\" = ?) AND (\"boolCol\" = ?)";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setFloat(1, TEST_FLOAT_VALUE);
        Mockito.verify(result, Mockito.times(1)).setDouble(2, TEST_DOUBLE_VALUE);
        Mockito.verify(result, Mockito.times(1)).setBoolean(3, TEST_BOOLEAN_VALUE);
    }

    @Test
    public void buildSplitSql_withEmptyConstraints_buildsBasicQuery()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        Constraints constraints = createConstraints(Collections.emptyMap());

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\" ";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
    }

    @Test
    public void buildSplitSql_withNullValueSet_buildsBasicQuery()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        Map<String, ValueSet> constraintMap = new HashMap<>();
        constraintMap.put(TEST_COL_INT, null);
        Constraints constraints = createConstraints(constraintMap);

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\" ";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
    }

    @Test
    public void buildSplitSql_withNegativeLimit_shouldIgnoreLimit()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createTestSchema();
        Split split = createMockSplit();

        ValueSet intValueSet = getSingleValueSet(50);
        Map<String, ValueSet> constraintMap = ImmutableMap.of(TEST_COL_INT, intValueSet);

        Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(),
                -100L, Collections.emptyMap(), null);

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" = ?)";
        
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);
        
        PreparedStatement result = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, 50);
    }

    private ValueSet getDiscreteValueSet(Object... values)
    {
        List<Range> ranges = new java.util.ArrayList<>();
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

    private ValueSet getRangeSet(Object lowerValue,
                                 Object upperValue)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(Marker.Bound.EXACTLY);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(Marker.Bound.BELOW);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private Schema createTestSchema()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_INT, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_VARCHAR, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_BIGINT, Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_FLOAT, Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_DOUBLE, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_DATE, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_TIMESTAMP, Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_BOOLEAN, Types.MinorType.BIT.getType()).build());
        return schemaBuilder.build();
    }
    
    private Split createMockSplit()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("0");
        return split;
    }

    private Constraints createConstraints(Map<String, ValueSet> summary)
    {
        return new Constraints(summary, Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }

    private PreparedStatement createMockPreparedStatement(String expectedSql) throws SQLException {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql)))
                .thenReturn(preparedStatement);
        return preparedStatement;
    }
}
