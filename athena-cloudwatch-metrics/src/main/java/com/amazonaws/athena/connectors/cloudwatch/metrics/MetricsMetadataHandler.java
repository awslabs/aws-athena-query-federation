package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
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
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetricsMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsMetadataHandler.class);
    private static final String sourceType = "metrics";
    protected static final String METRIC_TABLE_NAME = "metrics";
    protected static final String METRIC_SAMPLES_TABLE_NAME = "metric_samples";
    protected static final List<String> STATISTICS = new ArrayList<>();
    private static final Schema METRIC_TABLE;
    private static final Schema METRIC_DATA_TABLE;
    private static final Map<String, Schema> TABLES = new HashMap<>();
    private static final int DEFAULT_PERIOD = 60;

    protected static final String SCHEMA_NAME = "default";
    protected static final String METRIC_NAME_FIELD = "metric_name";
    protected static final String NAMESPACE_FIELD = "namespace";
    protected static final String DIMENSIONS_FIELD = "dimensions";
    protected static final String DIMENSION_NAME_FIELD = "dim_name";
    protected static final String DIMENSION_VALUE_FIELD = "dim_value";
    protected static final String TIMESTAMP_FIELD = "timestamp";
    protected static final String VALUE_FIELD = "value";
    protected static final String STATISTIC_FIELD = "statistic";
    protected static final String PERIOD_FIELD = "period";

    private final AmazonCloudWatch metrics;

    static {
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

        METRIC_TABLE = new SchemaBuilder().newBuilder()
                .addStringField(NAMESPACE_FIELD)
                .addStringField(METRIC_NAME_FIELD)
                .addField(FieldBuilder.newBuilder(DIMENSIONS_FIELD, Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder(DIMENSIONS_FIELD, Types.MinorType.STRUCT.getType())
                                .addStringField(DIMENSION_NAME_FIELD)
                                .addStringField(DIMENSION_VALUE_FIELD)
                                .build())
                        .build())
                .addStringField(DIMENSION_NAME_FIELD)
                .addStringField(DIMENSION_VALUE_FIELD)
                .addListField(STATISTIC_FIELD, Types.MinorType.VARCHAR.getType())
                .addMetadata(NAMESPACE_FIELD, "Metric namespace")
                .addMetadata(METRIC_NAME_FIELD, "Metric name")
                .addMetadata(STATISTIC_FIELD, "List of statistics available for this metric (e.g. Maximum, Minimum, Average, Sample Count)")
                .addMetadata(DIMENSIONS_FIELD, "Array of Dimensions for the given metric.")
                .addMetadata(DIMENSION_NAME_FIELD, "Shortcut field that flattens dimension to allow easier filtering for metrics that contain the dimension name. This field is left blank unless used in the where clause.")
                .addMetadata(DIMENSION_VALUE_FIELD, "Shortcut field that flattens  dimension to allow easier filtering for metrics that contain the dimension value. This field is left blank unless used in the where clause.")
                .build();

        METRIC_DATA_TABLE = new SchemaBuilder().newBuilder()
                .addStringField(NAMESPACE_FIELD)
                .addStringField(METRIC_NAME_FIELD)
                .addField(FieldBuilder.newBuilder(DIMENSIONS_FIELD, Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder(DIMENSIONS_FIELD, Types.MinorType.STRUCT.getType())
                                .addStringField(DIMENSION_NAME_FIELD)
                                .addStringField(DIMENSION_VALUE_FIELD)
                                .build())
                        .build())
                .addStringField(DIMENSION_NAME_FIELD)
                .addStringField(DIMENSION_VALUE_FIELD)
                .addIntField(PERIOD_FIELD)
                .addBigIntField(TIMESTAMP_FIELD)
                .addFloat8Field(VALUE_FIELD)
                .addStringField(STATISTIC_FIELD)
                .addMetadata(NAMESPACE_FIELD, "Metric namespace")
                .addMetadata(METRIC_NAME_FIELD, "Metric name")
                .addMetadata(DIMENSIONS_FIELD, "Array of Dimensions for the given metric.")
                .addMetadata(DIMENSION_NAME_FIELD, "Shortcut field that flattens dimension to allow easier filtering on a single dimension name. This field is left blank unless used in the where clause")
                .addMetadata(DIMENSION_VALUE_FIELD, "Shortcut field that flattens  dimension to allow easier filtering on a single dimension value. This field is left blank unless used in the where clause.")
                .addMetadata(STATISTIC_FIELD, "Statistics type of this value (e.g. Maximum, Minimum, Average, Sample Count)")
                .addMetadata(TIMESTAMP_FIELD, "The epoch time (in seconds) the value is for.")
                .addMetadata(PERIOD_FIELD, "The period, in seconds, for the metric (e.g. 60 seconds, 120 seconds)")
                .addMetadata(VALUE_FIELD, "The value for the sample.")
                .build();

        TABLES.put(METRIC_TABLE_NAME, METRIC_TABLE);
        TABLES.put(METRIC_SAMPLES_TABLE_NAME, METRIC_DATA_TABLE);
    }

    public MetricsMetadataHandler()
    {
        super(sourceType);
        metrics = AmazonCloudWatchClientBuilder.standard().build();
    }

    @VisibleForTesting
    protected MetricsMetadataHandler(AmazonCloudWatch metrics,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
        this.metrics = metrics;
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), Collections.singletonList(SCHEMA_NAME));
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        List<TableName> tables = new ArrayList<>();
        TABLES.keySet().stream().forEach(next -> tables.add(new TableName(SCHEMA_NAME, next)));
        return new ListTablesResponse(listTablesRequest.getCatalogName(), tables);
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        Schema schema = TABLES.get(getTableRequest.getTableName().getTableName());
        if (schema == null || !SCHEMA_NAME.equalsIgnoreCase(getTableRequest.getTableName().getSchemaName())) {
            throw new IllegalArgumentException("Unknown table " + getTableRequest.getTableName());
        }
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(), schema);
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest getTableLayoutRequest)
    {
        //Even though Cloudwatch Metrics are partitioned by NameSpace and Dimension, there are no performant APIs
        //for generating the list of available partitions. Instead we handle that logic as part of doGetSplits
        //which is pipelined with the table scans.
        Block partitions = BlockUtils.newBlock(blockAllocator, "partitionId", Types.MinorType.INT.getType(), 0);
        return new GetTableLayoutResponse(getTableLayoutRequest.getCatalogName(), getTableLayoutRequest.getTableName(), partitions, new HashSet<>());
    }

    /**
     * Each 'metric' in cloudwatch is uniquely identified by a quad of Namespace, List<Dimension>, MetricName, Statistic. As such
     * we can parallelize each metric as a unique split.
     */
    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        if (getSplitsRequest.getTableName().getTableName().equalsIgnoreCase(METRIC_TABLE_NAME)) {
            //The request is just for meta-data about what metrics exist.
            Split metricsSplit = Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey()).build();
            return new GetSplitsResponse(getSplitsRequest.getCatalogName(), metricsSplit);
        }

        try (ConstraintEvaluator constraintEvaluator = new ConstraintEvaluator(blockAllocator,
                METRIC_DATA_TABLE,
                getSplitsRequest.getConstraints())) {
            ListMetricsRequest listMetricsRequest = new ListMetricsRequest();
            MetricUtils.pushDownConstraint(getSplitsRequest.getConstraints(), listMetricsRequest);
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
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private String getPeriodFromConstraint(Constraints constraints)
    {
        ValueSet period = constraints.getSummary().get(PERIOD_FIELD);
        if (period != null && period.isSingleValue()) {
            return String.valueOf(period.getSingleValue());
        }

        return String.valueOf(DEFAULT_PERIOD);
    }
}
