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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
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
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.PERIOD_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;

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

    private final AmazonCloudWatch metrics;

    static {
        //The statistics supported by Cloudwatch Metrics by default
        STATISTICS.add("Average");
        STATISTICS.add("Minimum");
        STATISTICS.add("Maximum");
        STATISTICS.add("Sum");
        STATISTICS.add("Sample Count");
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

    public MetricsMetadataHandler()
    {
        super(SOURCE_TYPE);
        metrics = AmazonCloudWatchClientBuilder.standard().build();
    }

    @VisibleForTesting
    protected MetricsMetadataHandler(AmazonCloudWatch metrics,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, secretsManager, SOURCE_TYPE, spillBucket, spillPrefix);
        this.metrics = metrics;
    }

    /**
     * Only supports a single, static, schema defined by SCHEMA_NAME.
     *
     * @see MetadataHandler
     */
    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), Collections.singletonList(SCHEMA_NAME));
    }

    /**
     * Supports a set of static tables defined by: TABLES
     *
     * @see MetadataHandler
     */
    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        List<TableName> tables = new ArrayList<>();
        TABLES.keySet().stream().forEach(next -> tables.add(new TableName(SCHEMA_NAME, next)));
        return new ListTablesResponse(listTablesRequest.getCatalogName(), tables);
    }

    /**
     * Returns the details of the requested static table.
     *
     * @see MetadataHandler
     */
    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        validateTable(getTableRequest.getTableName());
        Table table = TABLES.get(getTableRequest.getTableName().getTableName());
        return new GetTableResponse(getTableRequest.getCatalogName(),
                getTableRequest.getTableName(),
                table.getSchema(),
                table.getPartitionColumns());
    }

    /**
     * Returns single 'partition' since Cloudwatch Metric's APIs do not support the kind of filtering we need to do
     * reasonably scoped partition pruning. Instead we do the pruning at Split generation time and return a single
     * partition here. The down side to doing it at Split generation time is that we sacrifice parallelizing Split
     * generation. However this is not a significant performance detrement to this connector since we can
     * generate Splits rather quickly and easily.
     *
     * @see MetadataHandler
     */
    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest getTableLayoutRequest)
    {
        validateTable(getTableLayoutRequest.getTableName());
        //Even though Cloudwatch Metrics are partitioned by NameSpace and Dimension, there are no performant APIs
        //for generating the list of available partitions. Instead we handle that logic as part of doGetSplits
        //which is pipelined with the table scans.
        Block partitions = BlockUtils.newBlock(blockAllocator, "partitionId", Types.MinorType.INT.getType(), 0);
        return new GetTableLayoutResponse(getTableLayoutRequest.getCatalogName(), getTableLayoutRequest.getTableName(), partitions, new HashSet<>());
    }

    /**
     * Each 'metric' in cloudwatch is uniquely identified by a quad of Namespace, List<Dimension>, MetricName, Statistic. As such
     * we can parallelize each metric as a unique split.
     *
     * @see MetadataHandler
     */
    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
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
            ListMetricsResult result = metrics.listMetrics(listMetricsRequest);
            for (Metric nextMetric : result.getMetrics()) {
                for (String nextStatistic : STATISTICS) {
                    if (MetricUtils.applyMetricConstraints(constraintEvaluator, nextMetric, nextStatistic)) {
                        splits.add(Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey())
                                .add(DimensionSerDe.SERIALZIE_DIM_FIELD_NAME, DimensionSerDe.serialize(nextMetric.getDimensions()))
                                .add(METRIC_NAME_FIELD, nextMetric.getMetricName())
                                .add(NAMESPACE_FIELD, nextMetric.getNamespace())
                                .add(STATISTIC_FIELD, nextStatistic)
                                .add(PERIOD_FIELD, period)
                                .build());
                    }
                }
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
}
