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

import com.amazonaws.services.cloudwatch.model.MetricStat;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

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
            return mapper.writeValueAsString(new MetricStatHolder(metricStats));
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
            return mapper.readValue(serializedMetricStats, MetricStatHolder.class).getMetricStats();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Helper which allows us to use Jackson's Object Mapper to serialize a List of MetricStats.
     */
    private static class MetricStatHolder
    {
        private final List<MetricStat> metricStats;

        @JsonCreator
        public MetricStatHolder(@JsonProperty("metricStats") List<MetricStat> metricStats)
        {
            this.metricStats = metricStats;
        }

        @JsonProperty
        public List<MetricStat> getMetricStats()
        {
            return metricStats;
        }
    }
}
