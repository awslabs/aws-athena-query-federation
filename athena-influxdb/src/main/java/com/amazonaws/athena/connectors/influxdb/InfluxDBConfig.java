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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Helpers for reading connector configuration options.
 */
final class InfluxDBConfig
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBConfig.class);

    private InfluxDBConfig()
    {
    }

    /**
     * Reads an integer option and clamps it to {@code [min, max]}.
     *
     * A missing, blank, or non-numeric value returns {@code defaultValue}. A non-numeric value also logs a
     * warning. Clamping keeps a misconfigured value from causing unbounded work.
     */
    static int clampedInt(final Map<String, String> configOptions, final String key, final int defaultValue,
            final int min, final int max)
    {
        final String configured = configOptions.get(key);
        if (configured == null || configured.isBlank()) {
            return defaultValue;
        }
        try {
            return Math.max(min, Math.min(Integer.parseInt(configured.trim()), max));
        }
        catch (final NumberFormatException e) {
            logger.warn("Invalid {} value '{}'; using default {}", key, configured, defaultValue);
            return defaultValue;
        }
    }
}
