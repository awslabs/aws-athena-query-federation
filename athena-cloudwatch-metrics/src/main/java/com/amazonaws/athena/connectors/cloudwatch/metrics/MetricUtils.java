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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import com.google.common.annotations.VisibleForTesting;
import io.substrait.proto.Plan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.DimensionFilter;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_VALUE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.TIMESTAMP_FIELD;

/**
 * Helper which prepares and filters Cloudwatch Metrics requests.
 */
public class MetricUtils
{
    private static final Logger logger = LoggerFactory.getLogger(MetricUtils.class);

    //this is a format required by Cloudwatch Metrics
    private static final String METRIC_ID = "m1";

    // Environment variable to control cross-account metrics inclusion
    private static final String INCLUDE_LINKED_ACCOUNTS_ENV_VAR = "include_linked_accounts";

    private MetricUtils() {}

    /**
     * Checks if cross-account metrics should be included based on environment variable
     *
     * @return True if cross-account metrics should be included, false otherwise
     */
    private static boolean shouldIncludeLinkedAccounts()
    {
        String includeLinkedAccounts = getEnv();
        return Boolean.parseBoolean(includeLinkedAccounts);
    }

    /**
     * Extracted method for environment variables to allow mocking in tests.
     */
    @VisibleForTesting
    protected static String getEnv()
    {
        return System.getenv(INCLUDE_LINKED_ACCOUNTS_ENV_VAR);
    }

    /**
     * Filters metrics who have at least 1 metric dimension that matches DIMENSION_NAME_FIELD and DIMENSION_VALUE_FIELD filters.
     * This is just an optimization and isn't fully correct. We depend on the calling engine to apply full constraints. Also
     * filters metric name and namespace.
     *
     * @return True if the supplied metric contains at least 1 Dimension matching the evaluator.
     */
    protected static boolean applyMetricConstraints(ConstraintEvaluator evaluator, Metric metric, String statistic)
    {
        if (!evaluator.apply(NAMESPACE_FIELD, metric.namespace())) {
            return false;
        }

        if (!evaluator.apply(METRIC_NAME_FIELD, metric.metricName())) {
            return false;
        }

        if (statistic != null && !evaluator.apply(STATISTIC_FIELD, statistic)) {
            return false;
        }

        for (Dimension next : metric.dimensions()) {
            if (evaluator.apply(DIMENSION_NAME_FIELD, next.name()) && evaluator.apply(DIMENSION_VALUE_FIELD, next.value())) {
                return true;
            }
        }

        if (metric.dimensions().isEmpty() &&
                evaluator.apply(DIMENSION_NAME_FIELD, null) &&
                evaluator.apply(DIMENSION_VALUE_FIELD, null)) {
            return true;
        }

        return false;
    }

    /**
     * Attempts to push the supplied predicate constraints onto the Cloudwatch Metrics request.
     */
    protected static void pushDownPredicate(Constraints constraints, ListMetricsRequest.Builder listMetricsRequest)
    {
        Map<String, ValueSet> summary = constraints.getSummary();

        listMetricsRequest.includeLinkedAccounts(shouldIncludeLinkedAccounts());

        ValueSet namespaceConstraint = summary.get(NAMESPACE_FIELD);
        if (namespaceConstraint != null && namespaceConstraint.isSingleValue()) {
            listMetricsRequest.namespace(namespaceConstraint.getSingleValue().toString());
        }

        ValueSet metricConstraint = summary.get(METRIC_NAME_FIELD);
        if (metricConstraint != null && metricConstraint.isSingleValue()) {
            listMetricsRequest.metricName(metricConstraint.getSingleValue().toString());
        }

        ValueSet dimensionNameConstraint = summary.get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = summary.get(DIMENSION_VALUE_FIELD);
        if (dimensionNameConstraint != null && dimensionNameConstraint.isSingleValue() &&
                dimensionValueConstraint != null && dimensionValueConstraint.isSingleValue()) {
            DimensionFilter filter = DimensionFilter.builder()
                    .name(dimensionNameConstraint.getSingleValue().toString())
                    .value(dimensionValueConstraint.getSingleValue().toString())
                    .build();
            listMetricsRequest.dimensions(Collections.singletonList(filter));
        }
    }

    /**
     * Creates a Cloudwatch Metrics sample data request from the provided inputs
     *
     * @param readRecordsRequest The RecordReadRequest to make into a Cloudwatch Metrics Data request.
     * @return The Cloudwatch Metrics Data request that matches the requested read operation.
     */
    protected static GetMetricDataRequest makeGetMetricDataRequest(ReadRecordsRequest readRecordsRequest)
    {
        Split split = readRecordsRequest.getSplit();
        String serializedMetricDataQueries = split.getProperty(MetricDataQuerySerDe.SERIALIZED_METRIC_DATA_QUERIES_FIELD_NAME);
        List<MetricDataQuery> metricDataQueries = MetricDataQuerySerDe.deserialize(serializedMetricDataQueries);
        GetMetricDataRequest.Builder dataRequestBuilder = GetMetricDataRequest.builder();

        dataRequestBuilder.metricDataQueries(metricDataQueries);

        // Managed-connector (Athena federation) path: predicates arrive as a Substrait query plan rather than in
        // the Constraints summary. Push only the timestamp range down to Cloudwatch as startTime/endTime; any
        // other predicate is left for the engine to apply.
        QueryPlan queryPlan = readRecordsRequest.getConstraints().getQueryPlan();
        if (queryPlan != null && queryPlan.getSubstraitPlan() != null && !queryPlan.getSubstraitPlan().isEmpty()) {
            applySubstraitTimeRange(readRecordsRequest.getConstraints(), dataRequestBuilder);
            return dataRequestBuilder.build();
        }

        ValueSet timeConstraint = readRecordsRequest.getConstraints().getSummary().get(TIMESTAMP_FIELD);
        if (timeConstraint instanceof SortedRangeSet && !timeConstraint.isNullAllowed()) {
            //SortedRangeSet is how >, <, between is represented which are easiest and most common when
            //searching logs so we attempt to push that down here as an optimization. SQL can represent complex
            //overlapping ranges which Cloudwatch can not support so this is not a replacement for applying
            //constraints using the ConstraintEvaluator.

            Range basicPredicate = ((SortedRangeSet) timeConstraint).getSpan();

            if (!basicPredicate.getLow().isNullValue()) {
                Long lowerBound = (Long) basicPredicate.getLow().getValue();
                //TODO: confirm timezone handling
                logger.info("makeGetMetricsRequest: with startTime " + (lowerBound * 1000) + " " + new Date(lowerBound * 1000));
                dataRequestBuilder.startTime(new Date(lowerBound * 1000).toInstant());
            }
            else {
                //TODO: confirm timezone handling
                dataRequestBuilder.startTime(new Date(0).toInstant());
            }

            if (!basicPredicate.getHigh().isNullValue()) {
                Long upperBound = (Long) basicPredicate.getHigh().getValue();
                //TODO: confirm timezone handling
                logger.info("makeGetMetricsRequest: with endTime " + (upperBound * 1000) + " " + new Date(upperBound * 1000));
                dataRequestBuilder.endTime(new Date(upperBound * 1000).toInstant());
            }
            else {
                //TODO: confirm timezone handling
                dataRequestBuilder.endTime(new Date(System.currentTimeMillis()).toInstant());
            }
        }
        else {
            //TODO: confirm timezone handling
            dataRequestBuilder.startTime(new Date(0).toInstant());
            dataRequestBuilder.endTime(new Date(System.currentTimeMillis()).toInstant());
        }

        return dataRequestBuilder.build();
    }

    /**
     * Extracts predicates on the {@code timestamp} column from the Substrait query plan and pushes them to
     * Cloudwatch Metrics as {@code startTime}/{@code endTime}. GetMetricData requires both bounds, so this
     * defaults to the full window and narrows it from the plan. Best-effort: unsupported operators or plan
     * shapes leave the bounds at their defaults and let the engine apply all predicates.
     */
    private static void applySubstraitTimeRange(Constraints constraints, GetMetricDataRequest.Builder dataRequestBuilder)
    {
        dataRequestBuilder.startTime(Instant.EPOCH);
        dataRequestBuilder.endTime(Instant.now());

        QueryPlan queryPlan = constraints.getQueryPlan();
        if (queryPlan == null || queryPlan.getSubstraitPlan() == null) {
            return;
        }
        try {
            Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
            SubstraitRelModel relModel = SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
            if (relModel.getFilterRel() == null) {
                return;
            }
            List<String> tableColumns = SubstraitMetadataParser.getTableColumns(relModel);
            Map<String, List<ColumnPredicate>> predicatesByColumn = SubstraitFunctionParser.getColumnPredicatesMap(
                    plan.getExtensionsList(), relModel.getFilterRel().getCondition(), tableColumns);

            for (ColumnPredicate predicate : predicatesByColumn.getOrDefault(TIMESTAMP_FIELD, List.of())) {
                Object value = predicate.getValue();
                if (!(value instanceof Number)) {
                    continue;
                }
                Instant instant = Instant.ofEpochSecond(((Number) value).longValue());
                switch (predicate.getOperator()) {
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL_TO:
                        dataRequestBuilder.startTime(instant);
                        break;
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL_TO:
                        dataRequestBuilder.endTime(instant);
                        break;
                    case EQUAL:
                        dataRequestBuilder.startTime(instant);
                        dataRequestBuilder.endTime(instant);
                        break;
                    default:
                        break;
                }
            }
        }
        catch (Exception e) {
            logger.warn("Unable to push Substrait timestamp range to Cloudwatch Metrics; leaving predicates to the engine.", e);
        }
    }
}
