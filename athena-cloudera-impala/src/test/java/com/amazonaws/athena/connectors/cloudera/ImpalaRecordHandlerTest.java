/*-
 * #%L
 * athena-cloudera-impala
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

package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
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
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.SCHEMA_NAME;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;

public class ImpalaRecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_PARTITION = "partition";
    private static final String TEST_PARTITION_VALUE = "p0";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_COL3 = "testCol3";
    private static final String TEST_COL4 = "testCol4";
    private static final String QPT_TEST_QUERY = "SELECT * FROM testSchema.testTable WHERE testCol1 = 1";
    private static final String QPT_SCHEMA_FUNCTION_NAME_VALUE = "system.query";
    private static final String QPT_NAME_PROPERTY = "name";
    private static final String QPT_SCHEMA_PROPERTY = "schema";

    private ImpalaRecordHandler impalaRecordHandler;
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
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(TEST_SECRET).build())))
                .thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new ImpalaQueryStringBuilder(IMPALA_QUOTE_CHARACTER, new ImpalaFederationExpressionParser(IMPALA_QUOTE_CHARACTER));

        this.impalaRecordHandler = new ImpalaRecordHandler(
            new DatabaseConnectionConfig(TEST_CATALOG, ImpalaConstants.IMPALA_NAME,
                "impala://jdbc:impala://localhost:10000/athena;{" + TEST_SECRET + "}", TEST_SECRET),
            amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    private ValueSet getSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        Mockito.when(valueSet.isNullAllowed()).thenReturn(false);
        return valueSet;
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.VARBINARY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(TEST_PARTITION, TEST_PARTITION_VALUE));
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.isSingleValue()).thenReturn(true);
        Mockito.when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1b.isSingleValue()).thenReturn(true);
        Mockito.when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));
        final long dateDays = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2020-01-05").getTime());
        ValueSet valueSet2 = getSingleValueSet(dateDays);
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put(TEST_COL2, valueSet2)
                .build());
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.impalaRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Date expectedDate = new Date(120, 0, 5);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1))
                .setDate(1, expectedDate);
    }

    @Test
    public void testBuildSplitSql_withQueryPassthrough()
            throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createBaseSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createBaseSplit();

        // Valid query passthrough args
        Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<String, String>()
                .put(QUERY, QPT_TEST_QUERY)
                .put(SCHEMA_FUNCTION_NAME, QPT_SCHEMA_FUNCTION_NAME_VALUE)
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put(QPT_NAME_PROPERTY, NAME)
                .put(QPT_SCHEMA_PROPERTY, SCHEMA_NAME)
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.impalaRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        // Verify passthrough query was used
        Mockito.verify(this.connection).prepareStatement(QPT_TEST_QUERY);
        Mockito.verifyNoMoreInteractions(this.connection);
        assertSame(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void testBuildSplitSql_withoutQueryPassthrough()
            throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createBaseSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createBaseSplit();

        // query passthrough is disabled (empty passthrough args)
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.impalaRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        // Verify that a non-passthrough SQL query was used
        Mockito.verify(this.connection).prepareStatement(Mockito.argThat(sql -> !sql.equals(QPT_TEST_QUERY)));
        Mockito.verifyNoMoreInteractions(this.connection);
        assertSame(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void testBuildSplitSql_withMissingQueryArg()
            throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createBaseSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createBaseSplit();

        // Required QUERY parameter is missing
        Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<String, String>()
                .put(SCHEMA_FUNCTION_NAME, QPT_SCHEMA_FUNCTION_NAME_VALUE)
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put(QPT_NAME_PROPERTY, NAME)
                .put(QPT_SCHEMA_PROPERTY, SCHEMA_NAME)
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);

        try {
            this.impalaRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
            fail("Expected exception was not thrown");
        }
        catch (RuntimeException e) {
            Mockito.verifyNoInteractions(this.connection);
            Assert.assertTrue(e.getMessage().contains("Missing Query Passthrough Argument"));
        }
    }

    @Test
    public void testBuildSplitSql_withWrongSchemaFunctionName()
            throws Exception
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createBaseSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createBaseSplit();

        // Schema function name is incorrect
        Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<String, String>()
                .put(QUERY, QPT_TEST_QUERY)
                .put(SCHEMA_FUNCTION_NAME, "wrong.function")  // Wrong schema function name
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put(QPT_NAME_PROPERTY, NAME)
                .put(QPT_SCHEMA_PROPERTY, SCHEMA_NAME)
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);

        try {
            this.impalaRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
            fail("Expected exception was not thrown");
        }
        catch (RuntimeException e) {
            Mockito.verifyNoInteractions(this.connection);
            Assert.assertTrue(e.getMessage().contains("Function Signature doesn't match implementation's"));
        }
    }


    @Test
    public void testBuildSplitSqlWithComplexConstraints()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("age", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("department", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("is_active", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("created_date", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("updated_timestamp", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("amount", new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(10, 2)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TEST_PARTITION, TEST_PARTITION_VALUE);
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

        // Create complex constraints with multiple predicates
        ValueSet idSet = createMultipleValueSet(1, 2, 3, 4, 5);
        ValueSet ageSet = createRangeSet(Marker.Bound.EXACTLY, 25, Marker.Bound.EXACTLY, 65);
        ValueSet salarySet = createRangeSet(Marker.Bound.ABOVE, 50000.0, Marker.Bound.ABOVE, Double.MAX_VALUE);
        ValueSet deptSet = getSingleValueSet("IT");
        ValueSet activeSet = getSingleValueSet(true);
        ValueSet amountSet = getSingleValueSet(BigDecimal.valueOf(1234.56));

        Constraints constraints = new Constraints(
                ImmutableMap.of(
                        "id", idSet,
                        "age", ageSet,
                        "salary", salarySet,
                        "department", deptSet,
                        "is_active", activeSet,
                        "amount", amountSet
                ),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT id, name, age, salary, department, is_active, created_date, updated_timestamp, amount FROM testSchema.testTable  WHERE (id IN (?,?,?,?,?)) AND ((age >= ? AND age <= ?)) AND ((salary > ?)) AND (department = ?) AND (is_active = ?) AND (amount = ?) AND p0";

        PreparedStatement expectedPreparedStatement = executeAndVerifySqlGeneration(tableName, schema, split, constraints, expectedSql);

        // Verify parameter setting for all constraints
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setInt(3, 3);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setInt(4, 4);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setInt(5, 5);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setInt(6, 25);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setInt(7, 65);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setDouble(8, 50000.0);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setString(9, "IT");
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setBoolean(10, true);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setBigDecimal(11, BigDecimal.valueOf(1234.56));
    }

    @Test
    public void testBuildSplitSqlWithOrderByAndLimit()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TEST_PARTITION, TEST_PARTITION_VALUE);
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

        // Create constraints with ORDER BY and LIMIT
        OrderByField orderByField = new OrderByField("salary", OrderByField.Direction.DESC_NULLS_LAST);
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                ImmutableList.of(orderByField),
                10, // limit
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT id, name, salary FROM testSchema.testTable  WHERE p0 ORDER BY salary DESC NULLS LAST LIMIT 10";

        executeAndVerifySqlGeneration(tableName, schema, split, constraints, expectedSql);
    }

    @Test
    public void testBuildSplitSqlWithTopNQuery()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("department", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TEST_PARTITION, TEST_PARTITION_VALUE);
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

        // Create Top N query with predicates, ORDER BY, and LIMIT
        ValueSet deptSet = getSingleValueSet("IT");
        ValueSet salarySet = createRangeSet(Marker.Bound.ABOVE, 50000.0, Marker.Bound.ABOVE, Double.MAX_VALUE);

        OrderByField orderByField = new OrderByField("salary", OrderByField.Direction.DESC_NULLS_LAST);
        Constraints constraints = new Constraints(
                ImmutableMap.of("department", deptSet, "salary", salarySet),
                Collections.emptyList(),
                ImmutableList.of(orderByField),
                5, // limit for top 5
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT id, name, salary, department FROM testSchema.testTable  WHERE ((salary > ?)) AND (department = ?) AND p0 ORDER BY salary DESC NULLS LAST LIMIT 5";

        PreparedStatement expectedPreparedStatement = executeAndVerifySqlGeneration(tableName, schema, split, constraints, expectedSql);

        // Verify parameter setting
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setDouble(1, 50000.0);
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setString(2, "IT");
    }

    @Test
    public void testBuildSplitSqlWithDateConstraints()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("created_date", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("updated_timestamp", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TEST_PARTITION, TEST_PARTITION_VALUE);
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

        // Test date constraints
        final long dateDays = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2023-01-15").getTime());
        ValueSet dateSet = getSingleValueSet(dateDays);

        // Test timestamp constraints
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime timestamp = LocalDateTime.parse("2023-01-15 14:30:45", formatter);
        ValueSet timestampSet = getSingleValueSet(timestamp);

        Constraints constraints = new Constraints(
                ImmutableMap.of("created_date", dateSet, "updated_timestamp", timestampSet),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT created_date, updated_timestamp FROM testSchema.testTable  WHERE (created_date = ?) AND (updated_timestamp = ?) AND p0";

        PreparedStatement expectedPreparedStatement = executeAndVerifySqlGeneration(tableName, schema, split, constraints, expectedSql);

        // Verify parameter setting
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setDate(Mockito.anyInt(), Mockito.any(Date.class));
        Mockito.verify(expectedPreparedStatement, Mockito.times(1)).setTimestamp(Mockito.anyInt(), Mockito.any(Timestamp.class));
    }

    @Test
    public void testBuildSplitSqlWithNullConstraints()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TEST_PARTITION, TEST_PARTITION_VALUE);
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

        // Test NULL constraints
        ValueSet nullSet = createNullValueSet();
        Constraints constraints = new Constraints(
                ImmutableMap.of("name", nullSet),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT name FROM testSchema.testTable  WHERE (name IS NULL) AND p0";

        executeAndVerifySqlGeneration(tableName, schema, split, constraints, expectedSql);
    }

    @Test
    public void testBuildSplitSqlWithEmptyConstraints()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createBaseSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createBaseSplit();

        // Test with empty constraints
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT id FROM testSchema.testTable  WHERE p0";

        executeAndVerifySqlGeneration(tableName, schema, split, constraints, expectedSql);
    }

    @Test
    public void testBuildSplitSqlWithMultipleOrderByFields()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("department", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TEST_PARTITION, TEST_PARTITION_VALUE);
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

        // Test multiple ORDER BY fields
        OrderByField orderByField1 = new OrderByField("department", OrderByField.Direction.ASC_NULLS_LAST);
        OrderByField orderByField2 = new OrderByField("salary", OrderByField.Direction.DESC_NULLS_FIRST);
        OrderByField orderByField3 = new OrderByField("name", OrderByField.Direction.ASC_NULLS_LAST);

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                ImmutableList.of(orderByField1, orderByField2, orderByField3),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT department, salary, name FROM testSchema.testTable  WHERE p0 ORDER BY department ASC NULLS LAST, salary DESC NULLS FIRST, name ASC NULLS LAST";

        executeAndVerifySqlGeneration(tableName, schema, split, constraints, expectedSql);
    }

    // Helper methods for creating test data

    /**
     * Creates a basic schema builder with common fields that most tests need.
     * Callers can add additional fields as needed.
     */
    private SchemaBuilder createBaseSchemaBuilder()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
        return schemaBuilder;
    }

    /**
     * Creates a split with common properties that most tests need.
     */
    private Split createBaseSplit()
    {
        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TEST_PARTITION, TEST_PARTITION_VALUE);
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);
        return split;
    }


    private ValueSet createMultipleValueSet(Object... values)
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
        Mockito.when(valueSet.isNullAllowed()).thenReturn(false);
        return valueSet;
    }

    private ValueSet createRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getLow().isLowerUnbounded()).thenReturn(lowerBound == Marker.Bound.BELOW);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        Mockito.when(range.getHigh().isUpperUnbounded()).thenReturn(upperBound == Marker.Bound.ABOVE);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        Mockito.when(valueSet.isNullAllowed()).thenReturn(false);
        return valueSet;
    }

    private ValueSet createNullValueSet()
    {
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.emptyList());
        Mockito.when(valueSet.isNullAllowed()).thenReturn(true);
        Mockito.when(valueSet.isNone()).thenReturn(true);
        return valueSet;
    }

    private PreparedStatement executeAndVerifySqlGeneration(TableName tableName, Schema schema, Split split, Constraints constraints, String expectedSql) throws SQLException
    {
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.impalaRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        assertSame(expectedPreparedStatement, preparedStatement);
        Mockito.verify(this.connection).prepareStatement(Mockito.eq(expectedSql));
        return expectedPreparedStatement;
    }
}
