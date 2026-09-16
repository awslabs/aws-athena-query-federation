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

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Query passthrough signature for InfluxDB 3. Exposed to Athena as the table function
 * {@code system.query(DATABASE => '&lt;db&gt;', QUERY => '&lt;native SQL&gt;')}.
 *
 * <p>Unlike a fully SQL-inferred passthrough, InfluxDB 3's Flight SQL client is bound to a specific database, so the
 * caller must supply both the target {@code DATABASE} and the native {@code QUERY} to run against it.
 */
public class InfluxDBQueryPassthrough implements QueryPassthroughSignature
{
    // The database (InfluxDB schema) the passthrough query runs against.
    public static final String DATABASE = "DATABASE";
    // The native InfluxDB 3 SQL to execute verbatim.
    public static final String QUERY = "QUERY";

    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";
    public static final List<String> ARGUMENTS = Arrays.asList(DATABASE, QUERY);
    private static final Pattern VALID_DATABASE_NAME = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9/_-]{0,63}$");

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBQueryPassthrough.class);

    @Override
    public String getFunctionSchema()
    {
        return SCHEMA_NAME;
    }

    @Override
    public String getFunctionName()
    {
        return NAME;
    }

    @Override
    public List<String> getFunctionArguments()
    {
        return ARGUMENTS;
    }

    @Override
    public Logger getLogger()
    {
        return LOGGER;
    }

    @Override
    public void customConnectorVerifications(Map<String, String> engineQptArguments)
    {
        String databaseName = engineQptArguments.get(DATABASE);
        if (databaseName == null || !VALID_DATABASE_NAME.matcher(databaseName).matches()) {
            throw new IllegalArgumentException(
                "Invalid InfluxDB v3 database name '" + databaseName + "'. Names must "
                + "be a maximum of 64 characters, start with a letter or number and "
                + "contain only letters, numbers, '_', '-', or '/'.");
        }
    }
}
