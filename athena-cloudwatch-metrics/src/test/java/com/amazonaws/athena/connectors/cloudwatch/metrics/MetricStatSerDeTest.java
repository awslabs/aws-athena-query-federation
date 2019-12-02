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

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MetricStatSerDeTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetricStatSerDeTest.class);
    private static final String EXPECTED_SERIALIZATION = "{\"metricStats\":[{\"metric\":{\"namespace\":\"namespace\",\"metricName\":\"metricName\",\"dimensions\":[" +
            "{\"name\":\"dim_name1\",\"value\":\"dim_value1\"},{\"name\":\"dim_name2\",\"value\":\"dim_value2\"}]},\"period\":60,\"stat\":\"p90\",\"unit\":null}]}";

    @Test
    public void serializeTest()
    {
        String schema = "schema";
        String table = "table";
        Integer period = 60;
        String statistic = "p90";
        String metricName = "metricName";
        String namespace = "namespace";

        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(new Dimension().withName("dim_name1").withValue("dim_value1"));
        dimensions.add(new Dimension().withName("dim_name2").withValue("dim_value2"));

        List<MetricStat> metricStats = new ArrayList<>();
        metricStats.add(new MetricStat()
                .withMetric(new Metric()
                        .withNamespace(namespace)
                        .withMetricName(metricName)
                        .withDimensions(dimensions))
                .withPeriod(60)
                .withStat(statistic));
        String actualSerialization = MetricStatSerDe.serialize(metricStats);
        logger.info("serializeTest: {}", actualSerialization);
        List<MetricStat> actual = MetricStatSerDe.deserialize(actualSerialization);
        assertEquals(EXPECTED_SERIALIZATION, actualSerialization);
        assertEquals(metricStats, actual);
    }
}
