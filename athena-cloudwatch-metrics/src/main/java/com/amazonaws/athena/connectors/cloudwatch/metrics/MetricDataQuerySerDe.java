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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to serialize and deserialize Cloudwatch Metrics MetricDataQuery objects. This is used
 * when creating and processing Splits.
 */
public class MetricDataQuerySerDe
{
    protected static final String SERIALIZED_METRIC_DATA_QUERIES_FIELD_NAME = "m";
    private static final ObjectMapper mapper = new ObjectMapper();

    private MetricDataQuerySerDe() {}

    /**
     * Serializes the provided List of MetricDataQueries.
     *
     * @param metricDataQueries The list of MetricDataQueries to serialize.
     * @return A String containing the serialized list of MetricDataQueries.
     */
    public static String serialize(List<MetricDataQuery> metricDataQueries)
    {
        try {
            return mapper.writeValueAsString(metricDataQueries.stream().map(query -> query.toBuilder()).collect(Collectors.toList()));
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Deserializes the provided String into a List of MetricDataQueries.
     *
     * @param serializedMetricDataQueries A serialized list of MetricDataQueries.
     * @return The List of MetricDataQueries represented by the serialized string.
     */
    public static List<MetricDataQuery> deserialize(String serializedMetricDataQueries)
    {
        try {
            CollectionType metricDataQueryBuilderCollection = mapper.getTypeFactory().constructCollectionType(List.class, MetricDataQuery.serializableBuilderClass());
            return ((List<MetricDataQuery.Builder>) mapper.readValue(serializedMetricDataQueries, metricDataQueryBuilderCollection)).stream().map(query -> query.build()).collect(Collectors.toList());
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
