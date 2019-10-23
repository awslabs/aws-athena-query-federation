package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.DimensionFilter;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricDataQuery;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.DIMENSION_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.DIMENSION_VALUE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.PERIOD_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.STATISTIC_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.TIMESTAMP_FIELD;

public class MetricUtils
{
    private static final Logger logger = LoggerFactory.getLogger(MetricUtils.class);

    //this is a format required by Cloudwatch Metrics
    private static final String METRIC_ID = "m1";

    private MetricUtils() {}

    /**
     * Filters metrics who have at least 1 metric dimension that matches DIMENSION_NAME_FIELD and DIMENSION_VALUE_FIELD filters.
     * This is just an optimization and isn't fully correct. We depend on the calling engine to apply full constraints. Also
     * filters metric name and namespace.
     *
     * @return True if the supplied metric contains at least 1 Dimension matching the evaluator.
     */
    protected static boolean applyMetricConstraints(ConstraintEvaluator evaluator, Metric metric, String statistic)
    {

        if (!evaluator.apply(NAMESPACE_FIELD, metric.getNamespace())) {
            return false;
        }

        if (!evaluator.apply(METRIC_NAME_FIELD, metric.getMetricName())) {
            return false;
        }

        if (!evaluator.apply(STATISTIC_FIELD, statistic)) {
            return false;
        }

        for (Dimension next : metric.getDimensions()) {
            if (evaluator.apply(DIMENSION_NAME_FIELD, next.getName()) && evaluator.apply(DIMENSION_VALUE_FIELD, next.getValue())) {
                return true;
            }
        }

        if (metric.getDimensions().isEmpty() &&
                evaluator.apply(DIMENSION_NAME_FIELD, null) &&
                evaluator.apply(DIMENSION_VALUE_FIELD, null)) {
            return true;
        }

        return false;
    }

    protected static void pushDownConstraint(Constraints constraints, ListMetricsRequest listMetricsRequest)
    {
        Map<String, ValueSet> summary = constraints.getSummary();

        ValueSet namespaceConstraint = summary.get(NAMESPACE_FIELD);
        if (namespaceConstraint != null && namespaceConstraint.isSingleValue()) {
            listMetricsRequest.setNamespace(namespaceConstraint.getSingleValue().toString());
        }

        ValueSet metricConstraint = summary.get(METRIC_NAME_FIELD);
        if (metricConstraint != null && metricConstraint.isSingleValue()) {
            listMetricsRequest.setMetricName(metricConstraint.getSingleValue().toString());
        }

        ValueSet dimensionNameConstraint = summary.get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = summary.get(DIMENSION_VALUE_FIELD);
        if (dimensionNameConstraint != null && dimensionNameConstraint.isSingleValue() &&
                dimensionValueConstraint != null && dimensionValueConstraint.isSingleValue()) {
            DimensionFilter filter = new DimensionFilter()
                    .withName(dimensionNameConstraint.getSingleValue().toString())
                    .withValue(dimensionValueConstraint.getSingleValue().toString());
            listMetricsRequest.setDimensions(Collections.singletonList(filter));
        }
    }

    protected static GetMetricDataRequest makeGetMetricsRequest(Split split, List<Dimension> dimensions, ReadRecordsRequest readRecordsRequest)
    {
        GetMetricDataRequest dataRequest = new GetMetricDataRequest();
        com.amazonaws.services.cloudwatch.model.Metric metric = new com.amazonaws.services.cloudwatch.model.Metric();
        metric.setNamespace(split.getProperty(NAMESPACE_FIELD));
        metric.setMetricName(split.getProperty(METRIC_NAME_FIELD));

        List<Dimension> dList = new ArrayList<>();
        for (Dimension nextDim : dimensions) {
            dList.add(new Dimension().withName(nextDim.getName()).withValue(nextDim.getValue()));
        }
        metric.setDimensions(dList);

        MetricDataQuery mds = new MetricDataQuery()
                .withMetricStat(new MetricStat()
                        .withMetric(metric)
                        .withPeriod(Integer.valueOf(split.getProperty(PERIOD_FIELD)))
                        .withStat(split.getProperty(STATISTIC_FIELD)))
                .withId(METRIC_ID);

        dataRequest.withMetricDataQueries(Collections.singletonList(mds));

        ValueSet timeConstraint = readRecordsRequest.getConstraints().getSummary().get(TIMESTAMP_FIELD);
        if (timeConstraint instanceof SortedRangeSet) {
            //SortedRangeSet is how >, <, between is represented which are easiest and most common when
            //searching logs so we attempt to push that down here as an optimization. SQL can represent complex
            //overlapping ranges which Cloudwatch can not support so this is not a replacement for applying
            //constraints using the ConstraintEvaluator.

            Range basicPredicate = ((SortedRangeSet) timeConstraint).getSpan();

            if (!basicPredicate.getLow().isNullValue()) {
                Long lowerBound = (Long) basicPredicate.getLow().getValue();
                //TODO: confirm timezone handling
                logger.info("makeGetMetricsRequest: with startTime " + (lowerBound * 1000) + " " + new Date(lowerBound * 1000));
                dataRequest.withStartTime(new Date(lowerBound * 1000));
            }
            else {
                //TODO: confirm timezone handling
                dataRequest.withStartTime(new Date(0));
            }

            if (!basicPredicate.getHigh().isNullValue()) {
                Long upperBound = (Long) basicPredicate.getHigh().getValue();
                //TODO: confirm timezone handling
                logger.info("makeGetMetricsRequest: with endTime " + (upperBound * 1000) + " " + new Date(upperBound * 1000));
                dataRequest.withEndTime(new Date(upperBound * 1000));
            }
            else {
                //TODO: confirm timezone handling
                dataRequest.withEndTime(new Date(System.currentTimeMillis()));
            }
        }
        else {
            //TODO: confirm timezone handling
            dataRequest.withStartTime(new Date(0));
            dataRequest.withEndTime(new Date(System.currentTimeMillis()));
        }

        return dataRequest;
    }
}
