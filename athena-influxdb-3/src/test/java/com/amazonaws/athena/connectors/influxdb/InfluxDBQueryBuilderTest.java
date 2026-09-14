/*-
 * #%L
 * athena-influxdb
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.influxdb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfluxDBQueryBuilderTest
{
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account",
            Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());

    private BlockAllocator allocator;
    private Schema schema;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        schema = new SchemaBuilder()
                .addField("time", Types.MinorType.DATEMILLI.getType())
                .addField("host", Types.MinorType.VARCHAR.getType())
                .addField("usage_idle", Types.MinorType.FLOAT8.getType())
                .build();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testConstraintLiteralUnpacksTrinoPackedTimestamp()
    {
        // Athena/Trino packs TIMESTAMP WITH TIME ZONE as (millisUtc << 12 | tzKey); UTC key is 0.
        final long millisUtc = 1764764130000L; // 2025-12-03T10:15:30Z
        final long packed = millisUtc << 12;
        final ArrowType.Timestamp tsType =
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC");

        // The shared helper unpacks back to the original epoch millis.
        assertEquals(millisUtc, InfluxDBQueryBuilder.constraintEpochMillis(packed, tsType));

        // constraintLiteral decodes the packed value (not the corrupted far-future year 233142).
        final String expected = "TIMESTAMP '"
                + java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(millisUtc))
                + "'";
        assertEquals(expected, InfluxDBQueryBuilder.constraintLiteral(packed, tsType));

        // toLiteral is the plain formatter: it treats the value as epoch millis directly.
        assertEquals("TIMESTAMP '"
                        + java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(millisUtc))
                        + "'",
                InfluxDBQueryBuilder.toLiteral(millisUtc, tsType));
    }

    @Test
    public void testBuildSqlWithSplitTimeBounds()
    {
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        // 1-hour window (epoch millis). The per-split bound is a half-open
        // [lower, upper) predicate on "time" expressed with TIMESTAMP literals.
        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints,
                "1764764130000", "1764767730000");

        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("\"time\" >= TIMESTAMP '"));
        assertTrue(sql.contains("\"time\" < TIMESTAMP '"));
    }

    @Test
    public void testBuildSqlWithNullSplitBoundsAddsNoTimeFilter()
    {
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        // Single-partition fallback: null bounds must not add a WHERE clause.
        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints, null, null);

        assertEquals("SELECT \"time\", \"host\", \"usage_idle\" FROM \"cpu\"", sql);
    }

    @Test
    public void testBuildSqlNoConstraints()
    {
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertEquals("SELECT \"time\", \"host\", \"usage_idle\" FROM \"cpu\"", sql);
    }

    @Test
    public void testBuildSqlWithLimit()
    {
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), 10, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.endsWith("LIMIT 10"));
    }

    @Test
    public void testBuildSqlWithOrderByAndLimit()
    {
        final List<OrderByField> orderBy = Arrays.asList(
                new OrderByField("usage_idle", OrderByField.Direction.DESC_NULLS_LAST));
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                orderBy, 5, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("ORDER BY \"usage_idle\" DESC NULLS LAST"));
        assertTrue(sql.endsWith("LIMIT 5"));
    }

    @Test
    public void testBuildSqlWithEqualityFilter()
    {
        final Map<String, ValueSet> summary = new HashMap<>();
        summary.put("host", SortedRangeSet.of(
                Range.equal(allocator, new ArrowType.Utf8(), "server1")));

        final Constraints constraints = new Constraints(summary, Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("\"host\" = 'server1'"));
    }

    @Test
    public void testBuildSqlWithRangeFilter()
    {
        final Map<String, ValueSet> summary = new HashMap<>();
        summary.put("usage_idle", SortedRangeSet.of(
                Range.greaterThan(allocator, new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), 50.0)));

        final Constraints constraints = new Constraints(summary, Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("\"usage_idle\" > 50.0"));
    }

    @Test
    public void testBuildSqlWithInList()
    {
        final Map<String, ValueSet> summary = new HashMap<>();
        summary.put("host", SortedRangeSet.of(
                Range.equal(allocator, new ArrowType.Utf8(), "server1"),
                Range.equal(allocator, new ArrowType.Utf8(), "server2"),
                Range.equal(allocator, new ArrowType.Utf8(), "server3")));

        final Constraints constraints = new Constraints(summary, Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("\"host\" IN ('server1', 'server2', 'server3')"));
    }

    @Test
    public void testBuildSqlWithNullCheck()
    {
        final Map<String, ValueSet> summary = new HashMap<>();
        summary.put("host", SortedRangeSet.onlyNull(new ArrowType.Utf8()));

        final Constraints constraints = new Constraints(summary, Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("\"host\" IS NULL"));
    }

    @Test
    public void testBuildSqlWithBetweenRange()
    {
        final Map<String, ValueSet> summary = new HashMap<>();
        final ArrowType float8 = new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        summary.put("usage_idle", SortedRangeSet.of(
                Range.range(allocator, float8, 10.0, true, 90.0, true)));

        final Constraints constraints = new Constraints(summary, Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("\"usage_idle\" >= 10.0"));
        assertTrue(sql.contains("\"usage_idle\" <= 90.0"));
    }

    @Test
    public void testBuildSqlCombinedFilterAndLimit()
    {
        final Map<String, ValueSet> summary = new HashMap<>();
        summary.put("host", SortedRangeSet.of(
                Range.equal(allocator, new ArrowType.Utf8(), "server1")));

        final List<OrderByField> orderBy = Arrays.asList(
                new OrderByField("usage_idle", OrderByField.Direction.DESC_NULLS_LAST));

        final Constraints constraints = new Constraints(summary, Collections.emptyList(),
                orderBy, 10, null, null);

        final String sql = InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("\"host\" = 'server1'"));
        assertTrue(sql.contains("ORDER BY \"usage_idle\" DESC NULLS LAST"));
        assertTrue(sql.endsWith("LIMIT 10"));
    }

    @Test
    public void testToLiteralTypes()
    {
        assertEquals("'hello'", InfluxDBQueryBuilder.toLiteral("hello", new ArrowType.Utf8()));
        assertEquals("'it''s'", InfluxDBQueryBuilder.toLiteral("it's", new ArrowType.Utf8()));
        assertEquals("42", InfluxDBQueryBuilder.toLiteral(42L, new ArrowType.Int(64, true)));
        assertEquals("3.14", InfluxDBQueryBuilder.toLiteral(3.14, new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)));
        assertEquals("true", InfluxDBQueryBuilder.toLiteral(true, new ArrowType.Bool()));
        assertEquals("NULL", InfluxDBQueryBuilder.toLiteral(null, new ArrowType.Utf8()));
        assertEquals("TIMESTAMP '2026-06-23T23:51:50Z'", InfluxDBQueryBuilder.toLiteral(1782258710000l,
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")));
    }

    /**
     * A caller can declare a numeric or date Arrow type over a Utf8 value block; the SDK never reconciles the
     * declared type with the runtime value class. toLiteral must not trust the declared type — a non-Number value
     * under a numeric type, or a non-temporal value under DATEMILLI, must be quoted and single-quote-escaped so it
     * cannot break out of the literal and inject SQL.
     */
    @Test
    public void testToLiteralNeutralizesTypeConfusionInjection()
    {
        // Numeric types declared over a String value: must NOT splice raw (would be unquoted injection).
        final String bareInjection = "0 OR 1=1 --";
        assertEquals("'0 OR 1=1 --'", InfluxDBQueryBuilder.toLiteral(bareInjection, new ArrowType.Int(64, true)));
        assertEquals("'0 OR 1=1 --'", InfluxDBQueryBuilder.toLiteral(bareInjection, new ArrowType.Int(32, true)));
        assertEquals("'0 OR 1=1 --'", InfluxDBQueryBuilder.toLiteral(bareInjection, new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)));

        // Quote-breakout payload under a numeric type: embedded quotes must be doubled.
        assertEquals("'x'' OR ''1''=''1'",
                InfluxDBQueryBuilder.toLiteral("x' OR '1'='1", new ArrowType.Int(64, true)));

        // DATEMILLI declared over a String value: must be quoted AND escaped, not spliced quoted-but-unescaped.
        assertEquals("'x'' OR ''1''=''1'",
                InfluxDBQueryBuilder.toLiteral("x' OR '1'='1", Types.MinorType.DATEMILLI.getType()));

        // Legitimate numeric values still render bare (no behavior change).
        assertEquals("42", InfluxDBQueryBuilder.toLiteral(42L, new ArrowType.Int(64, true)));
    }

    @Test
    public void testQuoteEscaping()
    {
        assertEquals("\"normal\"", InfluxDBQueryBuilder.quote("normal"));
        assertEquals("\"has\"\"quote\"", InfluxDBQueryBuilder.quote("has\"quote"));
    }

    @Test
    public void testDoGetDataSourceCapabilities() throws Exception
    {
        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        config.put("influxdb_database", "testdb");

        final com.influxdb.v3.client.InfluxDBClient mockClient = mock(
                com.influxdb.v3.client.InfluxDBClient.class);
        final InfluxDBConnectionFactory mockFactory = mock(InfluxDBConnectionFactory.class);
        when(mockFactory.getClient(anyString())).thenReturn(mockClient);

        final InfluxDBMetadataHandler handler = new InfluxDBMetadataHandler(
                mockFactory,
                new com.amazonaws.athena.connector.lambda.security.LocalKeyFactory(),
                mock(software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.class),
                mock(software.amazon.awssdk.services.athena.AthenaClient.class),
                "test-bucket",
                "test-prefix",
                config);

        final GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(
                allocator,
                new GetDataSourceCapabilitiesRequest(IDENTITY, "queryId", "catalog"));

        final Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        assertTrue(capabilities.containsKey("supports_filter_pushdown"));
        assertTrue(capabilities.containsKey("supports_limit_pushdown"));
        assertTrue(capabilities.containsKey("supports_top_n_pushdown"));
        assertTrue(capabilities.containsKey("supports_complex_expression_pushdown"));
    }

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType FLOAT8 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    private static final ArrowType BOOL = new ArrowType.Bool();

    private ConstantExpression constant(final Object value, final ArrowType type)
    {
        final Block block = allocator.createBlock(new SchemaBuilder().addField("col1", type).build());
        block.constrain(ConstraintEvaluator.emptyEvaluator());
        block.setValue("col1", 0, value);
        block.setRowCount(1);
        return new ConstantExpression(block, type);
    }

    private FunctionCallExpression fce(final StandardFunctions func,
            final ArrowType returnType,
            final FederationExpression... args)
    {
        return new com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression(
                returnType, func.getFunctionName(), Arrays.asList(args));
    }

    private VariableExpression var(final String col, final ArrowType type)
    {
        return new VariableExpression(col, type);
    }

    private StandardFunctions sf(final String name)
    {
        return StandardFunctions.valueOf(name);
    }

    private String sqlForExpression(final FederationExpression expr)
    {
        final Constraints constraints = new Constraints(new HashMap<>(), Arrays.asList(expr),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);
        return InfluxDBQueryBuilder.buildSql(schema, "cpu", constraints);
    }

    @Test
    public void testExpressionPushdownBooleanAndComparisons()
    {
        final String sql = sqlForExpression(fce(sf("AND_FUNCTION_NAME"), BOOL,
                fce(sf("EQUAL_OPERATOR_FUNCTION_NAME"), BOOL, var("host", UTF8), constant("srv", UTF8)),
                fce(sf("GREATER_THAN_OPERATOR_FUNCTION_NAME"), BOOL, var("usage_idle", FLOAT8),
                        constant(50.0, FLOAT8))));
        assertTrue(sql.contains("\"host\" = 'srv'"));
        assertTrue(sql.contains("\"usage_idle\" > 50.0"));
        assertTrue(sql.contains(" AND "));
    }

    @Test
    public void testExpressionPushdownOperatorsCoverage()
    {
        assertTrue(sqlForExpression(fce(sf("LIKE_PATTERN_FUNCTION_NAME"), BOOL,
                var("host", UTF8), constant("srv%", UTF8))).contains("LIKE"));

        final String notNull = sqlForExpression(fce(sf("NOT_FUNCTION_NAME"), BOOL,
                fce(sf("IS_NULL_FUNCTION_NAME"), BOOL, var("host", UTF8))));
        assertTrue(notNull.contains("NOT") && notNull.contains("IS NULL"));

        assertTrue(sqlForExpression(fce(sf("OR_FUNCTION_NAME"), BOOL,
                fce(sf("LESS_THAN_OPERATOR_FUNCTION_NAME"), BOOL, var("usage_idle", FLOAT8), constant(10.0, FLOAT8)),
                fce(sf("GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME"), BOOL, var("usage_idle", FLOAT8),
                        constant(90.0, FLOAT8)))).contains(" OR "));

        assertTrue(sqlForExpression(fce(sf("ADD_FUNCTION_NAME"), FLOAT8,
                var("usage_idle", FLOAT8), constant(1.0, FLOAT8))).contains(" + "));

        // Timestamp constant exercises constraintLiteral -> constraintEpochMillis -> timestampLiteral.
        final ArrowType ts = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
        assertTrue(sqlForExpression(fce(sf("GREATER_THAN_OPERATOR_FUNCTION_NAME"), BOOL,
                var("time", ts), constant(1782258710000L, ts))).contains("TIMESTAMP"));
    }
}
