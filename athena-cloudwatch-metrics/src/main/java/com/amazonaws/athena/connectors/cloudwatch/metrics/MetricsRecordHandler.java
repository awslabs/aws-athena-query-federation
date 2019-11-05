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

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricSamplesTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricsTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricDataResult;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricDataResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
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

    //Used to handle throttling events by applying AIMD congestion control
    private final ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER).build();
    private final AmazonS3 amazonS3;
    private final AmazonCloudWatch metrics;

    public MetricsRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonCloudWatchClientBuilder.standard().build());
    }

    @VisibleForTesting
    protected MetricsRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonCloudWatch metrics)
    {
        super(amazonS3, secretsManager, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.metrics = metrics;
    }

    /**
     * Scans Cloudwatch Metrics for the list of available metrics or the samples for a specific metric.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest)
            throws TimeoutException
    {
        invoker.setBlockSpiller(blockSpiller);
        if (readRecordsRequest.getTableName().getTableName().equalsIgnoreCase(METRIC_TABLE.getName())) {
            readMetricsWithConstraint(constraintEvaluator, blockSpiller, readRecordsRequest);
        }
        else if (readRecordsRequest.getTableName().getTableName().equalsIgnoreCase(METRIC_DATA_TABLE.getName())) {
            readMetricSamplesWithConstraint(constraintEvaluator, blockSpiller, readRecordsRequest);
        }
    }

    /**
     * Handles retrieving the list of available metrics when the METRICS_TABLE is queried by listing metrics in Cloudwatch Metrics.
     */
    private void readMetricsWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest request)
            throws TimeoutException
    {
        ListMetricsRequest listMetricsRequest = new ListMetricsRequest();
        MetricUtils.pushDownPredicate(request.getConstraints(), listMetricsRequest);
        String prevToken;
        Set<String> requiredFields = new HashSet<>();
        request.getSchema().getFields().stream().forEach(next -> requiredFields.add(next.getName()));
        ValueSet dimensionNameConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        do {
            prevToken = listMetricsRequest.getNextToken();
            ListMetricsResult result = invoker.invoke(() -> metrics.listMetrics(listMetricsRequest));
            for (Metric nextMetric : result.getMetrics()) {
                blockSpiller.writeRows((Block block, int row) -> {
                    boolean matches = MetricUtils.applyMetricConstraints(constraintEvaluator, nextMetric, null);
                    if (matches) {
                        if (requiredFields.contains(METRIC_NAME_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(METRIC_NAME_FIELD), row, nextMetric.getMetricName());
                        }

                        if (requiredFields.contains(NAMESPACE_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(NAMESPACE_FIELD), row, nextMetric.getNamespace());
                        }

                        if (requiredFields.contains(STATISTIC_FIELD)) {
                            BlockUtils.setComplexValue(block.getFieldVector(STATISTIC_FIELD), row, DEFAULT, STATISTICS);
                        }

                        //If needed, populate the List of Dimensions as a List<Struct> using FieldResolver on setComplexValue
                        if (requiredFields.contains(DIMENSIONS_FIELD)) {
                            List<Dimension> dimensions = nextMetric.getDimensions();
                            BlockUtils.setComplexValue(block.getFieldVector(DIMENSIONS_FIELD),
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
                                    dimensions);
                        }

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        if (requiredFields.contains(DIMENSION_NAME_FIELD)) {
                            String value = (dimensionNameConstraint == null || !dimensionNameConstraint.isSingleValue())
                                    ? null : (dimensionNameConstraint.getSingleValue().toString());
                            BlockUtils.setValue(block.getFieldVector(DIMENSION_NAME_FIELD), row, value);
                        }

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        if (requiredFields.contains(DIMENSION_VALUE_FIELD)) {
                            String value = (dimensionValueConstraint == null || !dimensionValueConstraint.isSingleValue())
                                    ? null : dimensionValueConstraint.getSingleValue().toString();
                            BlockUtils.setValue(block.getFieldVector(DIMENSION_VALUE_FIELD), row, value);
                        }
                    }
                    return matches ? 1 : 0;
                });
            }
            listMetricsRequest.setNextToken(result.getNextToken());
        }
        while (listMetricsRequest.getNextToken() != null && !listMetricsRequest.getNextToken().equalsIgnoreCase(prevToken));
    }

    /**
     * Handles retrieving the samples for a specific metric from Cloudwatch Metrics.
     */
    private void readMetricSamplesWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest request)
            throws TimeoutException
    {
        Split split = request.getSplit();
        List<Dimension> dimensions = DimensionSerDe.deserialize(split.getProperty(DimensionSerDe.SERIALZIE_DIM_FIELD_NAME));
        GetMetricDataRequest dataRequest = MetricUtils.makeGetMetricDataRequest(request);

        String prevToken;
        Set<String> requiredFields = new HashSet<>();
        request.getSchema().getFields().stream().forEach(next -> requiredFields.add(next.getName()));
        ValueSet dimensionNameConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        ValueSet dimensionValueConstraint = request.getConstraints().getSummary().get(DIMENSION_NAME_FIELD);
        do {
            prevToken = dataRequest.getNextToken();
            GetMetricDataResult result = invoker.invoke(() -> metrics.getMetricData(dataRequest));
            for (MetricDataResult nextMetric : result.getMetricDataResults()) {
                List<Date> timestamps = nextMetric.getTimestamps();
                List<Double> values = nextMetric.getValues();
                for (int i = 0; i < nextMetric.getValues().size(); i++) {
                    int sampleNum = i;
                    blockSpiller.writeRows((Block block, int row) -> {
                        /**
                         * Most constraints were already applied at split generation so we only need to apply
                         * a subset.
                         */
                        if (requiredFields.contains(METRIC_NAME_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(METRIC_NAME_FIELD), row, split.getProperty(METRIC_NAME_FIELD));
                        }

                        if (requiredFields.contains(NAMESPACE_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(NAMESPACE_FIELD), row, split.getProperty(NAMESPACE_FIELD));
                        }

                        if (requiredFields.contains(STATISTIC_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(STATISTIC_FIELD), row, split.getProperty(STATISTIC_FIELD));
                        }

                        //If needed, populate the List of Dimensions as a List<Struct> using FieldResolver on setComplexValue
                        if (requiredFields.contains(DIMENSIONS_FIELD)) {
                            BlockUtils.setComplexValue(block.getFieldVector(DIMENSIONS_FIELD),
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
                                    dimensions);
                        }

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        if (requiredFields.contains(DIMENSION_NAME_FIELD)) {
                            String value = (dimensionNameConstraint == null || !dimensionNameConstraint.isSingleValue())
                                    ? null : dimensionNameConstraint.getSingleValue().toString();
                            BlockUtils.setValue(block.getFieldVector(DIMENSION_NAME_FIELD), row, value);
                        }

                        //This field is 'faked' in that we just use it as a convenient way to filter single dimensions. As such
                        //we always populate it with the value of the filter if the constraint passed and the filter was singleValue
                        if (requiredFields.contains(DIMENSION_VALUE_FIELD)) {
                            String value = (dimensionValueConstraint == null || !dimensionValueConstraint.isSingleValue())
                                    ? null : dimensionValueConstraint.getSingleValue().toString();
                            BlockUtils.setValue(block.getFieldVector(DIMENSION_VALUE_FIELD), row, value);
                        }

                        if (requiredFields.contains(PERIOD_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(PERIOD_FIELD), row, Integer.valueOf(split.getProperty(PERIOD_FIELD)));
                        }

                        boolean matches = true;

                        long timestamp = timestamps.get(sampleNum).getTime() / 1000;
                        matches &= constraintEvaluator.apply(TIMESTAMP_FIELD, timestamp);
                        if (matches && requiredFields.contains(TIMESTAMP_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(TIMESTAMP_FIELD), row, timestamp);
                        }

                        matches &= constraintEvaluator.apply(VALUE_FIELD, values.get(sampleNum));
                        if (matches && requiredFields.contains(VALUE_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(VALUE_FIELD), row, values.get(sampleNum));
                        }

                        return matches ? 1 : 0;
                    });
                }
            }
            dataRequest.setNextToken(result.getNextToken());
        }
        while (dataRequest.getNextToken() != null && !dataRequest.getNextToken().equalsIgnoreCase(prevToken));
    }
}
