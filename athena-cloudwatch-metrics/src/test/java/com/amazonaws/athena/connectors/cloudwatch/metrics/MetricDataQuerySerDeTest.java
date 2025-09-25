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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MetricDataQuerySerDeTest
{
	private static final Logger logger = LoggerFactory.getLogger(MetricDataQuerySerDeTest.class);

	private static final String EXPECTED_SERIALIZATION = "[{\"id\":\"m1\",\"metricStat\":{\"metric\":{\"namespace\":\"namespace\",\"metricName\":\"metricName\",\"dimensions\":[{\"name\":\"dim_name1\",\"value\":\"dim_value1\"},{\"name\":\"dim_name2\",\"value\":\"dim_value2\"}]},\"period\":60,\"stat\":\"p90\",\"unit\":null},\"expression\":null,\"label\":null,\"returnData\":null,\"period\":null,\"accountId\":null}]";

    @Test
    public void serializeTest()
    {
        Integer period = 60;
        String statistic = "p90";
        String metricName = "metricName";
        String namespace = "namespace";
        String id = "m1";

        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(Dimension.builder().name("dim_name1").value("dim_value1").build());
        dimensions.add(Dimension.builder().name("dim_name2").value("dim_value2").build());

        List<MetricDataQuery> metricDataQueries = new ArrayList<>();
        metricDataQueries.add(MetricDataQuery.builder()
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(namespace)
                                .metricName(metricName)
                                .dimensions(dimensions)
                                .build())
                        .period(period)
                        .stat(statistic)
                        .build())
                .id(id)
                .build());

        String actualSerialization = MetricDataQuerySerDe.serialize(metricDataQueries);
        logger.info("serializeTest: {}", actualSerialization);
        List<MetricDataQuery> actual = MetricDataQuerySerDe.deserialize(actualSerialization);
        assertEquals(EXPECTED_SERIALIZATION, actualSerialization);
		assertEquals(metricDataQueries, actual);
    }
}
