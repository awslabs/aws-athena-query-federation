package com.amazonaws.athena.connectors.cloudwatch.metrics.tables;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Set;

/**
 * Defines some commonly required field names used by all tables and consumers of tables in this connector.
 */
public abstract class Table
{
    //The name of the metric name field.
    public static final String METRIC_NAME_FIELD = "metric_name";
    //The name of the namespace field.
    public static final String NAMESPACE_FIELD = "namespace";
    //The name of the dimensions field which houses a list of Cloudwatch Metrics Dimensions.
    public static final String DIMENSIONS_FIELD = "dimensions";
    //The name of the convenience Dimension name field which gives easy access to 1 dimension name.
    public static final String DIMENSION_NAME_FIELD = "dim_name";
    //The name of the convenience Dimension value field which gives easy access to 1 dimension value.
    public static final String DIMENSION_VALUE_FIELD = "dim_value";
    //The name of the timestamp field, denoting the time period a particular metric sample was for.
    public static final String TIMESTAMP_FIELD = "timestamp";
    //The name of the metric value field which holds the value of a metric sample.
    public static final String VALUE_FIELD = "value";
    //The name of the statistic field (e.g. AVERAGE, p90).
    public static final String STATISTIC_FIELD = "statistic";
    //The name of the period field (e.g. 60 seconds).
    public static final String PERIOD_FIELD = "period";

    public abstract String getName();
    public abstract Schema getSchema();
    public abstract Set<String> getPartitionColumns();
}
