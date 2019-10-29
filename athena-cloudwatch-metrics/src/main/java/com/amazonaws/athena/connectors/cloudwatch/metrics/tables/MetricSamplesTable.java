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
package com.amazonaws.athena.connectors.cloudwatch.metrics.tables;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Set;

/**
 * Defines the metadata associated with our static metric_samples table.
 * <p>
 * This table contains the available metric samples for each metric named in the **metrics** table.
 * More specifically, the table contains the following columns:
 * <p>
 * **namespace** - A VARCHAR containing the namespace.
 * **metric_name** - A VARCHAR containing the metric name.
 * **dimensions** - A LIST of STRUCTS comprised of dim_name (VARCHAR) and dim_value (VARCHAR).
 * **dim_name** - A VARCHAR convenience field used to easily filter on a single dimension name.
 * **dim_value** - A VARCHAR convenience field used to easily filter on a single dimension value.
 * **period** - An INT field representing the 'period' of the metric in seconds. (e.g. 60 second metric)
 * **timestamp** - A BIGINT field representing the epoch time (in seconds) the metric sample is for.
 * **value** - A FLOAT8 field containing the value of the sample.
 * **statistic** - A VARCHAR containing the statistic type of the sample. (e.g. AVERAGE, p90, etc..)
 */
public class MetricSamplesTable
        extends Table
{
    private final Schema schema;
    private final String name;

    public MetricSamplesTable()
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

        name = "metric_samples";
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
