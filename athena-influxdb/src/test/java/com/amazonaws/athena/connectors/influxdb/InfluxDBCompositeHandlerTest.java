/*-
 * #%L
 * athena-influxdb
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.influxdb;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class InfluxDBCompositeHandlerTest
{
    static {
        // The Lambda-style constructors build AWS SDK clients, which need a region to resolve.
        System.setProperty("aws.region", "us-east-1");
    }

    @Test
    public void testCompositeHandlerConstructsFromEnvironment()
    {
        final InfluxDBCompositeHandler handler = new InfluxDBCompositeHandler();
        assertNotNull(handler);
    }

    @Test
    public void testLambdaConstructorsWireConnectionFactory()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        config.put("enable_query_parallelism", "true");

        final InfluxDBMetadataHandler metadataHandler = new InfluxDBMetadataHandler(config);
        assertTrue(metadataHandler.parallelismEnabled());

        final InfluxDBRecordHandler recordHandler = new InfluxDBRecordHandler(config);
        assertNotNull(recordHandler);
    }
}
