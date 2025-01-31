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
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.time.Instant;
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
    private final ThrottlingInvoker invoker;

    private final S3Client amazonS3;
    private final CloudWatchClient cloudwatchClient;

    public MetricsRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(S3Client.create(),
                SecretsManagerClient.create(),
                AthenaClient.create(),
                CloudWatchClient.create(), configOptions);
    }

    @VisibleForTesting
    protected MetricsRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, CloudWatchClient metrics, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE, configOptions);
        this.amazonS3 = amazonS3;
        this.cloudwatchClient = metrics;
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions)
            .withInitialDelayMs(THROTTLING_INITIAL_DELAY)
            .withIncrease(THROTTLING_INCREMENTAL_INCREASE)
            .build();
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
        ListMetricsRequest.Builder listMetricsRequestBuilder = ListMetricsRequest.builder();
        MetricUtils.pushDownPredicate(request.getConstraints(), listMetricsRequestBuilder);
        String prevToken;
        String nextToken;
        Set<String> requiredFields = new HashSet<>();
        request.getSchema().getFields().stream().forEach(next -> requiredFields.add(next.getName()));
        ValueSet dimensionNameConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = request.getConstraints().getSummary().get(DIMENSION_VALUE_FIELD);
        do {
            ListMetricsRequest listMetricsRequest = listMetricsRequestBuilder.build();
            prevToken = listMetricsRequest.nextToken();
            ListMetricsResponse result = invoker.invoke(() -> cloudwatchClient.listMetrics(listMetricsRequest));
            for (Metric nextMetric : result.metrics()) {
                blockSpiller.writeRows((Block block, int row) -> {
                    boolean matches = MetricUtils.applyMetricConstraints(blockSpiller.getConstraintEvaluator(), nextMetric, null);
                    if (matches) {
                        matches &= block.offerValue(METRIC_NAME_FIELD, row, nextMetric.metricName());
                        matches &= block.offerValue(NAMESPACE_FIELD, row, nextMetric.namespace());
                        matches &= block.offerComplexValue(STATISTIC_FIELD, row, DEFAULT, STATISTICS);

                        matches &= block.offerComplexValue(DIMENSIONS_FIELD,
                                row,
                                (Field field, Object val) -> {
                                    if (field.getName().equals(DIMENSION_NAME_FIELD)) {
                                        return ((Dimension) val).name();
                                    }
                                    else if (field.getName().equals(DIMENSION_VALUE_FIELD)) {
                                        return ((Dimension) val).value();
                                    }

                                    throw new RuntimeException("Unexpected field " + field.getName());
                                },
                                nextMetric.dimensions());

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
            nextToken = result.nextToken();
            listMetricsRequestBuilder.nextToken(nextToken);
        }
        while (nextToken != null && !nextToken.equalsIgnoreCase(prevToken) && queryStatusChecker.isQueryRunning());
    }

    /**
     * Handles retrieving the samples for a specific metric from Cloudwatch Metrics.
     */
    private void readMetricSamplesWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest request, QueryStatusChecker queryStatusChecker)
            throws TimeoutException
    {
        GetMetricDataRequest originalDataRequest = MetricUtils.makeGetMetricDataRequest(request);
        Map<String, MetricDataQuery> queries = new HashMap<>();
        for (MetricDataQuery query : originalDataRequest.metricDataQueries()) {
            queries.put(query.id(), query);
        }
        GetMetricDataRequest.Builder dataRequestBuilder = originalDataRequest.toBuilder();

        String prevToken;
        String nextToken;
        ValueSet dimensionNameConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = request.getConstraints().getSummary().get(DIMENSION_VALUE_FIELD);
        do {
            GetMetricDataRequest dataRequest = dataRequestBuilder.build();
            prevToken = dataRequest.nextToken();
            GetMetricDataResponse result = invoker.invoke(() -> cloudwatchClient.getMetricData(dataRequest));
            for (MetricDataResult nextMetric : result.metricDataResults()) {
                MetricStat metricStat = queries.get(nextMetric.id()).metricStat();
                List<Instant> timestamps = nextMetric.timestamps();
                List<Double> values = nextMetric.values();
                for (int i = 0; i < nextMetric.values().size(); i++) {
                    int sampleNum = i;
                    blockSpiller.writeRows((Block block, int row) -> {
                        /**
                         * Most constraints were already applied at split generation so we only need to apply
                         * a subset.
                         */
                        block.offerValue(METRIC_NAME_FIELD, row, metricStat.metric().metricName());
                        block.offerValue(NAMESPACE_FIELD, row, metricStat.metric().namespace());
                        block.offerValue(STATISTIC_FIELD, row, metricStat.stat());

                        block.offerComplexValue(DIMENSIONS_FIELD,
                                row,
                                (Field field, Object val) -> {
                                    if (field.getName().equals(DIMENSION_NAME_FIELD)) {
                                        return ((Dimension) val).name();
                                    }
                                    else if (field.getName().equals(DIMENSION_VALUE_FIELD)) {
                                        return ((Dimension) val).value();
                                    }

                                    throw new RuntimeException("Unexpected field " + field.getName());
                                },
                                metricStat.metric().dimensions());

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

                        block.offerValue(PERIOD_FIELD, row, metricStat.period());

                        boolean matches = true;
                        block.offerValue(VALUE_FIELD, row, values.get(sampleNum));
                        long timestamp = timestamps.get(sampleNum).getEpochSecond();
                        block.offerValue(TIMESTAMP_FIELD, row, timestamp);

                        return matches ? 1 : 0;
                    });
                }
            }
            nextToken = result.nextToken();
            dataRequestBuilder.nextToken(result.nextToken());
        }
        while (nextToken != null && !nextToken.equalsIgnoreCase(prevToken) && queryStatusChecker.isQueryRunning());
    }
}
