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

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

import java.util.Map;

/**
 * The Lambda entry point. The metadata and record handlers share one {@link InfluxDBConnectionFactory}, so the
 * container resolves the auth token once and keeps one client cache.
 */
public class InfluxDBCompositeHandler extends CompositeHandler
{
    public InfluxDBCompositeHandler()
    {
        this(System.getenv());
    }

    InfluxDBCompositeHandler(final Map<String, String> configOptions)
    {
        this(configOptions, new InfluxDBConnectionFactory(configOptions, null));
    }

    private InfluxDBCompositeHandler(final Map<String, String> configOptions,
            final InfluxDBConnectionFactory connectionFactory)
    {
        super(new InfluxDBMetadataHandler(configOptions, connectionFactory),
                new InfluxDBRecordHandler(configOptions, connectionFactory));
    }
}
