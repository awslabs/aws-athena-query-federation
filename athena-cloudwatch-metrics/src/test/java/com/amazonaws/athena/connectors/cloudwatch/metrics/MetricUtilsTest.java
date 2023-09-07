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
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.DimensionFilter;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import org.apache.arrow.vector.types.pojo.Schema;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricStatSerDe.SERIALIZED_METRIC_STATS_FIELD_NAME;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.TestUtils.makeStringEquals;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_VALUE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.PERIOD_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.TIMESTAMP_FIELD;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.*;

public class MetricUtilsTest
{
    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private String catalog = "default";
    private BlockAllocator allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
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

        ConstraintEvaluator constraintEvaluator = new ConstraintEvaluator(allocator, schema, new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT));

        Metric metric = new Metric()
                .withNamespace("match1")
                .withMetricName("match2")
                .withDimensions(new Dimension().withName("match4").withValue("match5"));
        String statistic = "match3";
        assertTrue(MetricUtils.applyMetricConstraints(constraintEvaluator, metric, statistic));

        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator, copyMetric(metric).withNamespace("no_match"), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator, copyMetric(metric).withMetricName("no_match"), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator,
                copyMetric(metric).withDimensions(Collections.singletonList(new Dimension().withName("no_match").withValue("match5"))), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator,
                copyMetric(metric).withDimensions(Collections.singletonList(new Dimension().withName("match4").withValue("no_match"))), statistic));
        assertFalse(MetricUtils.applyMetricConstraints(constraintEvaluator, copyMetric(metric), "no_match"));
    }

    private Metric copyMetric(Metric metric)
    {
        Metric newMetric = new Metric()
                .withNamespace(metric.getNamespace())
                .withMetricName(metric.getMetricName());

        List<Dimension> dims = new ArrayList<>();
        for (Dimension next : metric.getDimensions()) {
            dims.add(new Dimension().withName(next.getName()).withValue(next.getValue()));
        }
        return newMetric.withDimensions(dims);
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

        ListMetricsRequest request = new ListMetricsRequest();
        MetricUtils.pushDownPredicate(new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT), request);

        assertEquals("match1", request.getNamespace());
        assertEquals("match2", request.getMetricName());
        assertEquals(1, request.getDimensions().size());
        assertEquals(new DimensionFilter().withName("match4").withValue("match5"), request.getDimensions().get(0));
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
        dimensions.add(new Dimension().withName("dim_name1").withValue("dim_value1"));
        dimensions.add(new Dimension().withName("dim_name2").withValue("dim_value2"));

        List<MetricStat> metricStats = new ArrayList<>();
        metricStats.add(new MetricStat()
                .withMetric(new Metric()
                        .withNamespace(namespace)
                        .withMetricName(metricName)
                        .withDimensions(dimensions))
                .withPeriod(60)
                .withStat(statistic));

        Split split = Split.newBuilder(null, null)
                .add(NAMESPACE_FIELD, namespace)
                .add(METRIC_NAME_FIELD, metricName)
                .add(PERIOD_FIELD, String.valueOf(period))
                .add(STATISTIC_FIELD, statistic)
                .add(SERIALIZED_METRIC_STATS_FIELD_NAME, MetricStatSerDe.serialize(metricStats))
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
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        GetMetricDataRequest actual = MetricUtils.makeGetMetricDataRequest(request);
        assertEquals(1, actual.getMetricDataQueries().size());
        assertNotNull(actual.getMetricDataQueries().get(0).getId());
        MetricStat metricStat = actual.getMetricDataQueries().get(0).getMetricStat();
        assertNotNull(metricStat);
        assertEquals(metricName, metricStat.getMetric().getMetricName());
        assertEquals(namespace, metricStat.getMetric().getNamespace());
        assertEquals(statistic, metricStat.getStat());
        assertEquals(period, metricStat.getPeriod());
        assertEquals(2, metricStat.getMetric().getDimensions().size());
        assertEquals(1000L, actual.getStartTime().getTime());
        assertTrue(actual.getStartTime().getTime() <= System.currentTimeMillis() + 1_000);
    }
}
