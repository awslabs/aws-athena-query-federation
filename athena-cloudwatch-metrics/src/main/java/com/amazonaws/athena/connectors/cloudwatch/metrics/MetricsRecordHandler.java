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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricSamplesTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricsTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricDataResult;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricDataQuery;
import com.amazonaws.services.cloudwatch.model.MetricDataResult;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connector.lambda.data.FieldResolver.DEFAULT;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsExceptionFilter.EXCEPTION_FILTER;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsMetadataHandler.STATISTICS;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSIONS_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_VALUE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.PERIOD_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.TIMESTAMP_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.VALUE_FIELD;

/**
 * Handles data read record requests for the Athena Cloudwatch Metrics Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Reads and maps Cloudwatch Metrics and Metric Samples.
 * 2. Attempts to push down time range predicates into Cloudwatch Metrics.
 */
public class MetricsRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsRecordHandler.class);

    //Used to log diagnostic info about this connector
    private static final String SOURCE_TYPE = "metrics";
    //Schema for the metrics table.
    private static final Table METRIC_TABLE = new MetricsTable();
    //Schema for the metric_samples table.
    private static final Table METRIC_DATA_TABLE = new MetricSamplesTable();

    //Throttling configs derived from benchmarking
    private static final long THROTTLING_INITIAL_DELAY = 140;
    private static final long THROTTLING_INCREMENTAL_INCREASE = 20;

    //Used to handle throttling events by applying AIMD congestion control
    private final ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER)
            .withInitialDelayMs(THROTTLING_INITIAL_DELAY)
            .withIncrease(THROTTLING_INCREMENTAL_INCREASE)
            .build();

    private final AmazonS3 amazonS3;
    private final AmazonCloudWatch metrics;

    public MetricsRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                AmazonCloudWatchClientBuilder.standard().build());
    }

    @VisibleForTesting
    protected MetricsRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, AmazonCloudWatch metrics)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.metrics = metrics;
    }

    /**
     * Scans Cloudwatch Metrics for the list of available metrics or the samples for a specific metric.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
            throws TimeoutException
    {
        invoker.setBlockSpiller(blockSpiller);
        if (readRecordsRequest.getTableName().getTableName().equalsIgnoreCase(METRIC_TABLE.getName())) {
            readMetricsWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
        }
        else if (readRecordsRequest.getTableName().getTableName().equalsIgnoreCase(METRIC_DATA_TABLE.getName())) {
            readMetricSamplesWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
        }
    }

    /**
     * Handles retrieving the list of available metrics when the METRICS_TABLE is queried by listing metrics in Cloudwatch Metrics.
     */
    private void readMetricsWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest request, QueryStatusChecker queryStatusChecker)
            throws TimeoutException
    {
        ListMetricsRequest listMetricsRequest = new ListMetricsRequest();
        MetricUtils.pushDownPredicate(request.getConstraints(), listMetricsRequest);
        String prevToken;
        Set<String> requiredFields = new HashSet<>();
        request.getSchema().getFields().stream().forEach(next -> requiredFields.add(next.getName()));
        ValueSet dimensionNameConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = request.getConstraints().getSummary().get(DIMENSION_VALUE_FIELD);
        do {
            prevToken = listMetricsRequest.getNextToken();
            ListMetricsResult result = invoker.invoke(() -> metrics.listMetrics(listMetricsRequest));
            for (Metric nextMetric : result.getMetrics()) {
                blockSpiller.writeRows((Block block, int row) -> {
                    boolean matches = MetricUtils.applyMetricConstraints(blockSpiller.getConstraintEvaluator(), nextMetric, null);
                    if (matches) {
                        matches &= block.offerValue(METRIC_NAME_FIELD, row, nextMetric.getMetricName());
                        matches &= block.offerValue(NAMESPACE_FIELD, row, nextMetric.getNamespace());
                        matches &= block.offerComplexValue(STATISTIC_FIELD, row, DEFAULT, STATISTICS);

                        matches &= block.offerComplexValue(DIMENSIONS_FIELD,
                                row,
                                (Field field, Object val) -> {
                                    if (field.getName().equals(DIMENSION_NAME_FIELD)) {
                                        return ((Dimension) val).getName();
                                    }
                                    else if (field.getName().equals(DIMENSION_VALUE_FIELD)) {
                                        return ((Dimension) val).getValue();
                                    }

                                    throw new RuntimeException("Unexpected field " + field.getName());
                                },
                                nextMetric.getDimensions());

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        String dimName = (dimensionNameConstraint == null || !dimensionNameConstraint.isSingleValue())
                                ? null : (dimensionNameConstraint.getSingleValue().toString());
                        matches &= block.offerValue(DIMENSION_NAME_FIELD, row, dimName);

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        String dimValue = (dimensionValueConstraint == null || !dimensionValueConstraint.isSingleValue())
                                ? null : dimensionValueConstraint.getSingleValue().toString();
                        matches &= block.offerValue(DIMENSION_VALUE_FIELD, row, dimValue);
                    }
                    return matches ? 1 : 0;
                });
            }
            listMetricsRequest.setNextToken(result.getNextToken());
        }
        while (listMetricsRequest.getNextToken() != null && !listMetricsRequest.getNextToken().equalsIgnoreCase(prevToken) && queryStatusChecker.isQueryRunning());
    }

    /**
     * Handles retrieving the samples for a specific metric from Cloudwatch Metrics.
     */
    private void readMetricSamplesWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest request, QueryStatusChecker queryStatusChecker)
            throws TimeoutException
    {
        GetMetricDataRequest dataRequest = MetricUtils.makeGetMetricDataRequest(request);
        Map<String, MetricDataQuery> queries = new HashMap<>();
        for (MetricDataQuery query : dataRequest.getMetricDataQueries()) {
            queries.put(query.getId(), query);
        }

        String prevToken;
        ValueSet dimensionNameConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = request.getConstraints().getSummary().get(DIMENSION_VALUE_FIELD);
        do {
            prevToken = dataRequest.getNextToken();
            GetMetricDataResult result = invoker.invoke(() -> metrics.getMetricData(dataRequest));
            for (MetricDataResult nextMetric : result.getMetricDataResults()) {
                MetricStat metricStat = queries.get(nextMetric.getId()).getMetricStat();
                List<Date> timestamps = nextMetric.getTimestamps();
                List<Double> values = nextMetric.getValues();
                for (int i = 0; i < nextMetric.getValues().size(); i++) {
                    int sampleNum = i;
                    blockSpiller.writeRows((Block block, int row) -> {
                        /**
                         * Most constraints were already applied at split generation so we only need to apply
                         * a subset.
                         */
                        block.offerValue(METRIC_NAME_FIELD, row, metricStat.getMetric().getMetricName());
                        block.offerValue(NAMESPACE_FIELD, row, metricStat.getMetric().getNamespace());
                        block.offerValue(STATISTIC_FIELD, row, metricStat.getStat());

                        block.offerComplexValue(DIMENSIONS_FIELD,
                                row,
                                (Field field, Object val) -> {
                                    if (field.getName().equals(DIMENSION_NAME_FIELD)) {
                                        return ((Dimension) val).getName();
                                    }
                                    else if (field.getName().equals(DIMENSION_VALUE_FIELD)) {
                                        return ((Dimension) val).getValue();
                                    }

                                    throw new RuntimeException("Unexpected field " + field.getName());
                                },
                                metricStat.getMetric().getDimensions());

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        String dimName = (dimensionNameConstraint == null || !dimensionNameConstraint.isSingleValue())
                                ? null : dimensionNameConstraint.getSingleValue().toString();
                        block.offerValue(DIMENSION_NAME_FIELD, row, dimName);

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        String dimVal = (dimensionValueConstraint == null || !dimensionValueConstraint.isSingleValue())
                                ? null : dimensionValueConstraint.getSingleValue().toString();
                        block.offerValue(DIMENSION_VALUE_FIELD, row, dimVal);

                        block.offerValue(PERIOD_FIELD, row, metricStat.getPeriod());

                        boolean matches = true;
                        block.offerValue(VALUE_FIELD, row, values.get(sampleNum));
                        long timestamp = timestamps.get(sampleNum).getTime() / 1000;
                        block.offerValue(TIMESTAMP_FIELD, row, timestamp);

                        return matches ? 1 : 0;
                    });
                }
            }
            dataRequest.setNextToken(result.getNextToken());
        }
        while (dataRequest.getNextToken() != null && !dataRequest.getNextToken().equalsIgnoreCase(prevToken) && queryStatusChecker.isQueryRunning());
    }
}
