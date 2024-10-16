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
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to serialize and deserialize Cloudwatch Metrics MetricStat objects. This is used
 * when creating and processing Splits.
 */
public class MetricStatSerDe
{
    protected static final String SERIALIZED_METRIC_STATS_FIELD_NAME = "m";
    private static final ObjectMapper mapper = new ObjectMapper();

    private MetricStatSerDe() {}

    /**
     * Serializes the provided List of MetricStats.
     *
     * @param metricStats The list of MetricStats to serialize.
     * @return A String containing the serialized list of MetricStats.
     */
    public static String serialize(List<MetricStat> metricStats)
    {
        try {
            return mapper.writeValueAsString(metricStats.stream().map(stat -> stat.toBuilder()).collect(Collectors.toList()));
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Deserializes the provided String into a List of MetricStats.
     *
     * @param serializedMetricStats A serialized list of MetricStats.
     * @return The List of MetricStats represented by the serialized string.
     */
    public static List<MetricStat> deserialize(String serializedMetricStats)
    {
        try {
            CollectionType metricStatBuilderCollection = mapper.getTypeFactory().constructCollectionType(List.class, MetricStat.serializableBuilderClass());
            return ((List<MetricStat.Builder>) mapper.readValue(serializedMetricStats, metricStatBuilderCollection)).stream().map(stat -> stat.build()).collect(Collectors.toList());
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
