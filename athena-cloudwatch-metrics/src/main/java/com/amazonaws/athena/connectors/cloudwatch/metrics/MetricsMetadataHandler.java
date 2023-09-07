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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricSamplesTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricsTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Lists;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricsExceptionFilter.EXCEPTION_FILTER;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.PERIOD_FIELD;

/**
 * Handles metadata requests for the Athena Cloudwatch Metrics Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Provides two tables (metrics and metric_samples) for accessing Cloudwatch Metrics data via the "default" schema.
 * 2. Supports Predicate Pushdown into Cloudwatch Metrics for most fields.
 * 3. If multiple Metrics (namespace, metric, dimension(s), and statistic) are requested, they can be read in parallel.
 */
public class MetricsMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsMetadataHandler.class);

    //Used to log diagnostic info about this connector
    private static final String SOURCE_TYPE = "metrics";

    //List of available statistics (AVERAGE, p90, etc...).
    protected static final List<String> STATISTICS = new ArrayList<>();
    //The schema (aka database) supported by this connector
    protected static final String SCHEMA_NAME = "default";
    //Schema for the metrics table
    private static final Table METRIC_TABLE;
    //Schema for the metric_samples table.
    private static final Table METRIC_DATA_TABLE;
    //Name of the table which contains details of available metrics.
    private static final String METRIC_TABLE_NAME;
    //Name of the table which contains metric samples.
    private static final String METRIC_SAMPLES_TABLE_NAME;
    //Lookup table for resolving table name to Schema.
    private static final Map<String, Table> TABLES = new HashMap<>();
    //The default metric period to query (60 seconds)
    private static final int DEFAULT_PERIOD_SEC = 60;
    //GetMetricData supports up to 100 Metrics per split
    private static final int MAX_METRICS_PER_SPLIT = 100;
    //The minimum number of splits we'd like to have for some parallelization
    private static final int MIN_NUM_SPLITS_FOR_PARALLELIZATION = 3;
    //Used to handle throttling events by applying AIMD congestion control
    private final ThrottlingInvoker invoker;

    private final AmazonCloudWatch metrics;

    static {
        //The statistics supported by Cloudwatch Metrics by default
        STATISTICS.add("Average");
        STATISTICS.add("Minimum");
        STATISTICS.add("Maximum");
        STATISTICS.add("Sum");
        STATISTICS.add("SampleCount");
        STATISTICS.add("p99");
        STATISTICS.add("p95");
        STATISTICS.add("p90");
        STATISTICS.add("p50");
        STATISTICS.add("p10");

        METRIC_TABLE = new MetricsTable();
        METRIC_DATA_TABLE = new MetricSamplesTable();
        METRIC_TABLE_NAME = METRIC_TABLE.getName();
        METRIC_SAMPLES_TABLE_NAME = METRIC_DATA_TABLE.getName();
        TABLES.put(METRIC_TABLE_NAME, METRIC_TABLE);
        TABLES.put(METRIC_SAMPLES_TABLE_NAME, METRIC_DATA_TABLE);
    }

    public MetricsMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        this.metrics = AmazonCloudWatchClientBuilder.standard().build();
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
    }

    @VisibleForTesting
    protected MetricsMetadataHandler(
        AmazonCloudWatch metrics,
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.metrics = metrics;
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
    }

    /**
     * Only supports a single, static, schema defined by SCHEMA_NAME.
     *
     * @see MetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), Collections.singletonList(SCHEMA_NAME));
    }

    /**
     * Supports a set of static tables defined by: TABLES
     *
     * @see MetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        List<TableName> tables = new ArrayList<>();
        TABLES.keySet().stream().forEach(next -> tables.add(new TableName(SCHEMA_NAME, next)));
        return new ListTablesResponse(listTablesRequest.getCatalogName(), tables, null);
    }

    /**
     * Returns the details of the requested static table.
     *
     * @see MetadataHandler
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        validateTable(getTableRequest.getTableName());
        Table table = TABLES.get(getTableRequest.getTableName().getTableName());
        return new GetTableResponse(getTableRequest.getCatalogName(),
                getTableRequest.getTableName(),
                table.getSchema(),
                table.getPartitionColumns());
    }

    /**
     * Our table doesn't support complex layouts or partitioning so we simply make this method a NoOp and the SDK will
     * automatically generate a single placeholder partition for us since Athena needs at least 1 partition returned
     * if there is potetnailly any data to read. We do this because Cloudwatch Metric's APIs do not support the kind of filtering we need to do
     * reasonably scoped partition pruning. Instead we do the pruning at Split generation time and return a single
     * partition here. The down side to doing it at Split generation time is that we sacrifice parallelizing Split
     * generation. However this is not a significant performance detrement to this connector since we can
     * generate Splits rather quickly and easily.
     *
     * @see MetadataHandler
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        validateTable(request.getTableName());
        //NoOp as we do not support partitioning.
    }

    /**
     * Each 'metric' in cloudwatch is uniquely identified by a quad of Namespace, List<Dimension>, MetricName, Statistic. If the
     * query is for the METRIC_TABLE we return a single split.  If the query is for actual metrics data, we start forming batches
     * of metrics now that will form the basis of GetMetricData requests during readSplits.
     *
     * @see MetadataHandler
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
            throws Exception
    {
        validateTable(getSplitsRequest.getTableName());

        //Handle requests for the METRIC_TABLE which requires only 1 split to list available metrics.
        if (METRIC_TABLE_NAME.equals(getSplitsRequest.getTableName().getTableName())) {
            //The request is just for meta-data about what metrics exist.
            Split metricsSplit = Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey()).build();
            return new GetSplitsResponse(getSplitsRequest.getCatalogName(), metricsSplit);
        }

        //handle generating splits for reading actual metrics data.
        try (ConstraintEvaluator constraintEvaluator = new ConstraintEvaluator(blockAllocator,
                METRIC_DATA_TABLE.getSchema(),
                getSplitsRequest.getConstraints())) {
            ListMetricsRequest listMetricsRequest = new ListMetricsRequest();
            MetricUtils.pushDownPredicate(getSplitsRequest.getConstraints(), listMetricsRequest);
            listMetricsRequest.setNextToken(getSplitsRequest.getContinuationToken());

            String period = getPeriodFromConstraint(getSplitsRequest.getConstraints());
            Set<Split> splits = new HashSet<>();
            ListMetricsResult result = invoker.invoke(() -> metrics.listMetrics(listMetricsRequest));

            List<MetricStat> metricStats = new ArrayList<>(100);
            for (Metric nextMetric : result.getMetrics()) {
                for (String nextStatistic : STATISTICS) {
                    if (MetricUtils.applyMetricConstraints(constraintEvaluator, nextMetric, nextStatistic)) {
                        metricStats.add(new MetricStat()
                                .withMetric(new Metric()
                                        .withNamespace(nextMetric.getNamespace())
                                        .withMetricName(nextMetric.getMetricName())
                                        .withDimensions(nextMetric.getDimensions()))
                                .withPeriod(Integer.valueOf(period))
                                .withStat(nextStatistic));
                    }
                }
            }

            if (CollectionUtils.isNullOrEmpty(metricStats)) {
                logger.info("No metric stats present after filtering predicates.");
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
            }

            List<List<MetricStat>> partitions = Lists.partition(metricStats, calculateSplitSize(metricStats.size()));
            for (List<MetricStat> partition : partitions) {
                String serializedMetricStats = MetricStatSerDe.serialize(partition);
                splits.add(Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey())
                        .add(MetricStatSerDe.SERIALIZED_METRIC_STATS_FIELD_NAME, serializedMetricStats)
                        .build());
            }

            String continuationToken = null;
            if (result.getNextToken() != null &&
                    !result.getNextToken().equalsIgnoreCase(listMetricsRequest.getNextToken())) {
                continuationToken = result.getNextToken();
            }

            return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, continuationToken);
        }
    }

    /**
     * Resolved the metric period to query, using a default if no period constraint is found.
     */
    private String getPeriodFromConstraint(Constraints constraints)
    {
        ValueSet period = constraints.getSummary().get(PERIOD_FIELD);
        if (period != null && period.isSingleValue()) {
            return String.valueOf(period.getSingleValue());
        }

        return String.valueOf(DEFAULT_PERIOD_SEC);
    }

    /**
     * Validates that the requested schema and table exist in our static set of supported tables.
     */
    private void validateTable(TableName tableName)
    {
        if (!SCHEMA_NAME.equals(tableName.getSchemaName())) {
            throw new RuntimeException("Unknown table " + tableName);
        }

        if (TABLES.get(tableName.getTableName()) == null) {
            throw new RuntimeException("Unknown table " + tableName);
        }
    }

    /**
     * Heuristically determines a split size by finding the minimum between:
     * 1. a split size that will allow for some parallelization.
     * 2. the maximum split size possible for a GetMetricData request.
     */
    private int calculateSplitSize(int datapointCount)
    {
        int numDataPointsForParallelization = (int) Math.ceil((double) datapointCount / MIN_NUM_SPLITS_FOR_PARALLELIZATION);
        return Math.min(numDataPointsForParallelization, MAX_METRICS_PER_SPLIT);
    }
}
