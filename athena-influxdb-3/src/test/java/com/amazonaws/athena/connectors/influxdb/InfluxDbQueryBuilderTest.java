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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
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

public class InfluxDbQueryBuilderTest
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
    public void testBuildSqlNoConstraints()
    {
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

        assertEquals("SELECT \"time\", \"host\", \"usage_idle\" FROM \"cpu\"", sql);
    }

    @Test
    public void testBuildSqlWithLimit()
    {
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), 10, null, null);

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.endsWith("LIMIT 10"));
    }

    @Test
    public void testBuildSqlWithOrderByAndLimit()
    {
        final List<OrderByField> orderBy = Arrays.asList(
                new OrderByField("usage_idle", OrderByField.Direction.DESC_NULLS_LAST));
        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                orderBy, 5, null, null);

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

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

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

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

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

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

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("\"host\" IN ('server1', 'server2', 'server3')"));
    }

    @Test
    public void testBuildSqlWithNullCheck()
    {
        final Map<String, ValueSet> summary = new HashMap<>();
        summary.put("host", SortedRangeSet.onlyNull(new ArrowType.Utf8()));

        final Constraints constraints = new Constraints(summary, Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

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

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

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

        final String sql = InfluxDbQueryBuilder.buildSql(schema, "cpu", constraints);

        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("\"host\" = 'server1'"));
        assertTrue(sql.contains("ORDER BY \"usage_idle\" DESC NULLS LAST"));
        assertTrue(sql.endsWith("LIMIT 10"));
    }

    @Test
    public void testToLiteralTypes()
    {
        assertEquals("'hello'", InfluxDbQueryBuilder.toLiteral("hello", new ArrowType.Utf8()));
        assertEquals("'it''s'", InfluxDbQueryBuilder.toLiteral("it's", new ArrowType.Utf8()));
        assertEquals("42", InfluxDbQueryBuilder.toLiteral(42L, new ArrowType.Int(64, true)));
        assertEquals("3.14", InfluxDbQueryBuilder.toLiteral(3.14, new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)));
        assertEquals("true", InfluxDbQueryBuilder.toLiteral(true, new ArrowType.Bool()));
        assertEquals("NULL", InfluxDbQueryBuilder.toLiteral(null, new ArrowType.Utf8()));
        assertEquals("TIMESTAMP '2026-06-23T23:51:50Z'", InfluxDbQueryBuilder.toLiteral(1782258710000l,
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")));
    }

    @Test
    public void testQuoteEscaping()
    {
        assertEquals("\"normal\"", InfluxDbQueryBuilder.quote("normal"));
        assertEquals("\"has\"\"quote\"", InfluxDbQueryBuilder.quote("has\"quote"));
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
        final InfluxDbConnectionFactory mockFactory = mock(InfluxDbConnectionFactory.class);
        when(mockFactory.getClient(anyString())).thenReturn(mockClient);

        final InfluxDbMetadataHandler handler = new InfluxDbMetadataHandler(
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
}
