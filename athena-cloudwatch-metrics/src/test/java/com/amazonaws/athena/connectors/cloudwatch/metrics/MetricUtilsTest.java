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
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import org.apache.arrow.vector.types.pojo.Schema;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
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

import static com.amazonaws.athena.connectors.cloudwatch.metrics.TestUtils.makeStringEquals;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_VALUE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.PERIOD_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.TIMESTAMP_FIELD;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.jupiter.api.Assertions.*;

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
    @SetEnvironmentVariable(key = "include_linked_accounts", value = "true")
    public void pushDownPredicateWithLinkedAccountsTrue() throws Exception
    {
        ListMetricsRequest.Builder requestBuilder = ListMetricsRequest.builder();
        MetricUtils.pushDownPredicate(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), requestBuilder);
        ListMetricsRequest request = requestBuilder.build();

        assertTrue(request.includeLinkedAccounts());
    }

    @Test
    @SetEnvironmentVariable(key = "include_linked_accounts", value = "false")
    public void pushDownPredicateWithLinkedAccountsFalse() throws Exception
    {
        ListMetricsRequest.Builder requestBuilder = ListMetricsRequest.builder();
        MetricUtils.pushDownPredicate(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null), requestBuilder);
        ListMetricsRequest request = requestBuilder.build();

        assertFalse(request.includeLinkedAccounts());
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
}
