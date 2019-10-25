package com.amazonaws.athena.connectors.cloudwatch.metrics.tables;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Set;

/**
 * Defines the metadata associated with our static metrics table.
 * <p>
 * This table contains the available metrics as uniquely defined by a triple of namespace, set<dimension>, name.
 * More specifically, this table contains the following columns.
 * * **namespace** - A VARCHAR containing the namespace.
 * * **metric_name** - A VARCHAR containing the metric name.
 * * **dimensions** - A LIST of STRUCTS comprised of dim_name (VARCHAR) and dim_value (VARCHAR).
 * * **statistic** - A List of VARCH statistics (e.g. p90, AVERAGE, etc..) avialable for the metric.
 */
public class MetricsTable
        extends Table
{
    private final Schema schema;
    private final String name;

    public MetricsTable()
    {
        schema = new SchemaBuilder().newBuilder()
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

        name = "metrics";
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public Set<String> getPartitionColumns()
    {
        return Collections.emptySet();
    }
}
