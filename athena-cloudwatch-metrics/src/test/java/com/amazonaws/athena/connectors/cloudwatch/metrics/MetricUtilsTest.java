/*-
 * #%L
 * athena-cloudwatch-metrics
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
package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.DimensionFilter;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.TestUtils.makeStringEquals;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_VALUE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.PERIOD_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.TIMESTAMP_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricUtilsTest
{
    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private String catalog = "default";
    private BlockAllocator allocator;

    @BeforeEach
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @AfterEach
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void applyMetricConstraints()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField(NAMESPACE_FIELD)
                .addStringField(METRIC_NAME_FIELD)
                .addStringField(STATISTIC_FIELD)
                .addStringField(DIMENSION_NAME_FIELD)
                .addStringField(DIMENSION_VALUE_FIELD)
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(NAMESPACE_FIELD, makeStringEquals(allocator, "match1"));
        constraintsMap.put(METRIC_NAME_FIELD, makeStringEquals(allocator, "match2"));
        constraintsMap.put(STATISTIC_FIELD, makeStringEquals(allocator, "match3"));
        constraintsMap.put(DIMENSION_NAME_FIELD, makeStringEquals(allocator, "match4"));
        constraintsMap.put(DIMENSION_VALUE_FIELD, makeStringEquals(allocator, "match5"));

        ConstraintEvaluator constraintEvaluator = new ConstraintEvaluator(allocator, schema, new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));

        Metric metric = Metric.builder()
                .namespace("match1")
                .metricName("match2")
                .dimensions(Dimension.builder().name("match4").value("match5").build())
                .build();
        String statistic = "match3";
        assertTrue(MetricUtils.applyMetricConstraints(constraintEvaluator, metric, statistic));

        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator, metric.toBuilder().namespace("no_match").build(), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator, metric.toBuilder().metricName("no_match").build(), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator,
                metric.toBuilder().dimensions(Collections.singletonList(Dimension.builder().name("no_match").value("match5").build())).build(), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator,
                metric.toBuilder().dimensions(Collections.singletonList(Dimension.builder().name("match4").value("no_match").build())).build(), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator, metric, "no_match"));
    }

    @Test
    public void pushDownPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(NAMESPACE_FIELD, makeStringEquals(allocator, "match1"));
        constraintsMap.put(METRIC_NAME_FIELD, makeStringEquals(allocator, "match2"));
        constraintsMap.put(STATISTIC_FIELD, makeStringEquals(allocator, "match3"));
        constraintsMap.put(DIMENSION_NAME_FIELD, makeStringEquals(allocator, "match4"));
        constraintsMap.put(DIMENSION_VALUE_FIELD, makeStringEquals(allocator, "match5"));

        ListMetricsRequest.Builder requestBuilder = ListMetricsRequest.builder();
        MetricUtils.pushDownPredicate(new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), requestBuilder);
        ListMetricsRequest request = requestBuilder.build();

        assertEquals("match1", request.namespace());
        assertEquals("match2", request.metricName());
        assertEquals(1, request.dimensions().size());
        assertEquals(DimensionFilter.builder().name("match4").value("match5").build(), request.dimensions().get(0));
    }

    @Test
    public void pushDownPredicateWithLinkedAccountsTrue()
    {
        try (MockedStatic<MetricUtils> mockedMetricUtils = Mockito.mockStatic(MetricUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedMetricUtils.when(MetricUtils::getEnv).thenReturn("true");

            ListMetricsRequest.Builder requestBuilder = ListMetricsRequest.builder();
            MetricUtils.pushDownPredicate(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), requestBuilder);
            ListMetricsRequest request = requestBuilder.build();

            assertTrue(request.includeLinkedAccounts());
        }
    }

    @Test
    public void pushDownPredicateWithLinkedAccountsFalse()
    {
        try (MockedStatic<MetricUtils> mockedMetricUtils = Mockito.mockStatic(MetricUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedMetricUtils.when(MetricUtils::getEnv).thenReturn("false");

            ListMetricsRequest.Builder requestBuilder = ListMetricsRequest.builder();
            MetricUtils.pushDownPredicate(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), requestBuilder);
            ListMetricsRequest request = requestBuilder.build();

            assertFalse(request.includeLinkedAccounts());
        }
    }

    @Test
    public void makeGetMetricDataRequest()
    {
        String schema = "schema";
        String table = "table";
        Integer period = 60;
        String statistic = "p90";
        String metricName = "metricName";
        String namespace = "namespace";

        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(Dimension.builder().name("dim_name1").value("dim_value1").build());
        dimensions.add(Dimension.builder().name("dim_name2").value("dim_value2").build());

        List<MetricDataQuery> metricDataQueries = new ArrayList<>();
        metricDataQueries.add(MetricDataQuery.builder()
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(namespace)
                                .metricName(metricName)
                                .dimensions(dimensions)
                                .build())
                        .period(60)
                        .stat(statistic)
                        .build())
                .id("m1")
                .build());

        Split split = Split.newBuilder(null, null)
                .add(NAMESPACE_FIELD, namespace)
                .add(METRIC_NAME_FIELD, metricName)
                .add(PERIOD_FIELD, String.valueOf(period))
                .add(STATISTIC_FIELD, statistic)
                .add(MetricDataQuerySerDe.SERIALIZED_METRIC_DATA_QUERIES_FIELD_NAME, MetricDataQuerySerDe.serialize(metricDataQueries))
                .build();

        Schema schemaForRead = SchemaBuilder.newBuilder().addStringField(METRIC_NAME_FIELD).build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(TIMESTAMP_FIELD, SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.BIGINT.getType(), 1L)), false));

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                catalog,
                "queryId-" + System.currentTimeMillis(),
                new TableName(schema, table),
                schemaForRead,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        GetMetricDataRequest actual = MetricUtils.makeGetMetricDataRequest(request);
        assertEquals(1, actual.metricDataQueries().size());
        assertNotNull(actual.metricDataQueries().get(0).id());
        MetricStat metricStat = actual.metricDataQueries().get(0).metricStat();
        assertNotNull(metricStat);
        assertEquals(metricName, metricStat.metric().metricName());
        assertEquals(namespace, metricStat.metric().namespace());
        assertEquals(statistic, metricStat.stat());
        assertEquals(period, metricStat.period());
        assertEquals(2, metricStat.metric().dimensions().size());
        assertEquals(1000L, actual.startTime().toEpochMilli());
        assertTrue(actual.startTime().toEpochMilli() <= System.currentTimeMillis() + 1_000);
    }

    // Managed-connector (Athena federation) Substrait plans over the metric_samples table schema
    // [namespace, metric_name, dim_name, dim_value, period, timestamp(BIGINT), value, statistic].
    // Produced by the same Isthmus SqlToSubstrait path the DNA test runner uses; embedded as constants
    // because the connector does not depend on Isthmus/Calcite. Regenerate from the SQL on each constant
    // if the schema or Substrait version changes. The timestamp column is epoch seconds.
    // SELECT * FROM metric_samples WHERE "timestamp" > 1704067200
    private static final String PLAN_TS_GT =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAEQARoKZ3Q6YW55X2FueRqpAxKmAwrRAjrOAgoMEgoKCAgJCgsMDQ4PEt8BEtwBCgIKABKvAQqsAQoCCgAShgEKCW5hbWVzcGFjZQoLbWV0cmljX25hbWUKCGRpbV9uYW1lCglkaW1fdmFsdWUKBnBlcmlvZAoJdGltZXN0YW1wCgV2YWx1ZQoJc3RhdGlzdGljEjIKBGICEAIKBGICEAIKBGICEAIKBGICEAIKBCoCEAIKBDoCEAIKBFoCEAIKBGICEAIYAjodCgt0ZXN0X3NjaGVtYQoObWV0cmljX3NhbXBsZXMaJBoiCAEaBAoCEAIiDBoKEggKBBICCAUiACIKGggKBjiAgcisBhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgASCW5hbWVzcGFjZRILbWV0cmljX25hbWUSCGRpbV9uYW1lEglkaW1fdmFsdWUSBnBlcmlvZBIJdGltZXN0YW1wEgV2YWx1ZRIJc3RhdGlzdGljMgsQSioHaXN0aG11cw==";
    // SELECT * FROM metric_samples WHERE "timestamp" >= 1704067200 AND "timestamp" <= 1704070800
    private static final String PLAN_TS_BETWEEN =
            "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBITGhEIAhACGgtndGU6YW55X2FueRITGhEIAhADGgtsdGU6YW55X2FueRrfAxLcAwqHAzqEAwoMEgoKCAgJCgsMDQ4PEpUCEpICCgIKABKvAQqsAQoCCgAShgEKCW5hbWVzcGFjZQoLbWV0cmljX25hbWUKCGRpbV9uYW1lCglkaW1fdmFsdWUKBnBlcmlvZAoJdGltZXN0YW1wCgV2YWx1ZQoJc3RhdGlzdGljEjIKBGICEAIKBGICEAIKBGICEAIKBGICEAIKBCoCEAIKBDoCEAIKBFoCEAIKBGICEAIYAjodCgt0ZXN0X3NjaGVtYQoObWV0cmljX3NhbXBsZXMaWhpYCAEaBAoCEAIiJhokGiIIAhoECgIQAiIMGgoSCAoEEgIIBSIAIgoaCAoGOICByKwGIiYaJBoiCAMaBAoCEAIiDBoKEggKBBICCAUiACIKGggKBjiQncisBhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgASCW5hbWVzcGFjZRILbWV0cmljX25hbWUSCGRpbV9uYW1lEglkaW1fdmFsdWUSBnBlcmlvZBIJdGltZXN0YW1wEgV2YWx1ZRIJc3RhdGlzdGljMgsQSioHaXN0aG11cw==";
    // SELECT * FROM metric_samples WHERE "timestamp" = 1704067200
    private static final String PLAN_TS_EQUAL =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRqpAxKmAwrRAjrOAgoMEgoKCAgJCgsMDQ4PEt8BEtwBCgIKABKvAQqsAQoCCgAShgEKCW5hbWVzcGFjZQoLbWV0cmljX25hbWUKCGRpbV9uYW1lCglkaW1fdmFsdWUKBnBlcmlvZAoJdGltZXN0YW1wCgV2YWx1ZQoJc3RhdGlzdGljEjIKBGICEAIKBGICEAIKBGICEAIKBGICEAIKBCoCEAIKBDoCEAIKBFoCEAIKBGICEAIYAjodCgt0ZXN0X3NjaGVtYQoObWV0cmljX3NhbXBsZXMaJBoiCAEaBAoCEAIiDBoKEggKBBICCAUiACIKGggKBjiAgcisBhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgASCW5hbWVzcGFjZRILbWV0cmljX25hbWUSCGRpbV9uYW1lEglkaW1fdmFsdWUSBnBlcmlvZBIJdGltZXN0YW1wEgV2YWx1ZRIJc3RhdGlzdGljMgsQSioHaXN0aG11cw==";
    // SELECT * FROM metric_samples WHERE namespace = 'AWS/Lambda' (non-time predicate; not pushable)
    private static final String PLAN_NS_EQUAL =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRqtAxKqAwrVAjrSAgoMEgoKCAgJCgsMDQ4PEuMBEuABCgIKABKvAQqsAQoCCgAShgEKCW5hbWVzcGFjZQoLbWV0cmljX25hbWUKCGRpbV9uYW1lCglkaW1fdmFsdWUKBnBlcmlvZAoJdGltZXN0YW1wCgV2YWx1ZQoJc3RhdGlzdGljEjIKBGICEAIKBGICEAIKBGICEAIKBGICEAIKBCoCEAIKBDoCEAIKBFoCEAIKBGICEAIYAjodCgt0ZXN0X3NjaGVtYQoObWV0cmljX3NhbXBsZXMaKBomCAEaBAoCEAIiChoIEgYKAhIAIgAiEBoOCgxiCkFXUy9MYW1iZGEaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAEgluYW1lc3BhY2USC21ldHJpY19uYW1lEghkaW1fbmFtZRIJZGltX3ZhbHVlEgZwZXJpb2QSCXRpbWVzdGFtcBIFdmFsdWUSCXN0YXRpc3RpYzILEEoqB2lzdGhtdXM=";
    // Valid base64 that does not decode to a Substrait Plan protobuf.
    private static final String PLAN_MALFORMED = "Zm9vYmFy";
    private static final long TS_LOWER = 1_704_067_200L;
    private static final long TS_UPPER = 1_704_070_800L;

    private ReadRecordsRequest substraitReadRecordsRequest(String planBase64)
    {
        List<MetricDataQuery> metricDataQueries = new ArrayList<>();
        metricDataQueries.add(MetricDataQuery.builder()
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder().namespace("ns").metricName("m").build())
                        .period(60)
                        .stat("p90")
                        .build())
                .id("m1")
                .build());
        Split split = Split.newBuilder(null, null)
                .add(MetricDataQuerySerDe.SERIALIZED_METRIC_DATA_QUERIES_FIELD_NAME, MetricDataQuerySerDe.serialize(metricDataQueries))
                .build();

        return new ReadRecordsRequest(identity,
                catalog,
                "queryId-" + System.currentTimeMillis(),
                new TableName("default", "metric_samples"),
                SchemaBuilder.newBuilder().addBigIntField(TIMESTAMP_FIELD).build(),
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                        DEFAULT_NO_LIMIT, Collections.emptyMap(), new QueryPlan("", planBase64)),
                100_000_000_000L,
                100_000_000_000L);
    }

    @Test
    public void makeGetMetricDataRequestSubstraitTimeGreaterThanPushesStartTime()
    {
        GetMetricDataRequest actual = MetricUtils.makeGetMetricDataRequest(substraitReadRecordsRequest(PLAN_TS_GT));
        assertEquals(1, actual.metricDataQueries().size());
        assertEquals(TS_LOWER, actual.startTime().getEpochSecond());
        // No upper bound in the plan -> endTime defaults to "now".
        assertTrue(actual.endTime().toEpochMilli() >= System.currentTimeMillis() - 60_000);
    }

    @Test
    public void makeGetMetricDataRequestSubstraitTimeRangePushesStartAndEndTime()
    {
        GetMetricDataRequest actual = MetricUtils.makeGetMetricDataRequest(substraitReadRecordsRequest(PLAN_TS_BETWEEN));
        assertEquals(TS_LOWER, actual.startTime().getEpochSecond());
        assertEquals(TS_UPPER, actual.endTime().getEpochSecond());
    }

    @Test
    public void makeGetMetricDataRequestSubstraitTimeEqualPushesBothBounds()
    {
        GetMetricDataRequest actual = MetricUtils.makeGetMetricDataRequest(substraitReadRecordsRequest(PLAN_TS_EQUAL));
        assertEquals(TS_LOWER, actual.startTime().getEpochSecond());
        assertEquals(TS_LOWER, actual.endTime().getEpochSecond());
    }

    @Test
    public void makeGetMetricDataRequestSubstraitNonTimePredicateIsNotPushed()
    {
        // A predicate on a non-timestamp column leaves the full window (startTime = epoch, endTime = now).
        GetMetricDataRequest actual = MetricUtils.makeGetMetricDataRequest(substraitReadRecordsRequest(PLAN_NS_EQUAL));
        assertEquals(0L, actual.startTime().toEpochMilli());
        assertTrue(actual.endTime().toEpochMilli() >= System.currentTimeMillis() - 60_000);
    }

    @Test
    public void makeGetMetricDataRequestMalformedSubstraitPlanDoesNotThrowAndSkipsPushdown()
    {
        // Best-effort: an unparseable plan must not fail the query; the full window is used.
        GetMetricDataRequest actual = MetricUtils.makeGetMetricDataRequest(substraitReadRecordsRequest(PLAN_MALFORMED));
        assertEquals(1, actual.metricDataQueries().size());
        assertEquals(0L, actual.startTime().toEpochMilli());
        assertTrue(actual.endTime().toEpochMilli() >= System.currentTimeMillis() - 60_000);
    }
}
