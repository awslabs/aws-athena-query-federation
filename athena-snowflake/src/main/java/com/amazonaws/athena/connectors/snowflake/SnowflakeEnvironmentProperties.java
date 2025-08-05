/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connectors.jdbc.JdbcEnvironmentProperties;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SCHEMA;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.WAREHOUSE;

public class SnowflakeEnvironmentProperties extends JdbcEnvironmentProperties
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeEnvironmentProperties.class);
    private static final String WAREHOUSE_PROPERTY_KEY = "warehouse";
    private static final String DB_PROPERTY_KEY = "db";
    private static final String SCHEMA_PROPERTY_KEY = "schema";
    private static final String SNOWFLAKE_ESCAPE_CHARACTER = "\"";
    public static final String ENABLE_S3_EXPORT = "SNOWFLAKE_ENABLE_S3_EXPORT";

    private final boolean enableS3Export;

    public SnowflakeEnvironmentProperties(Map<String, String> properties)
    {
        this.enableS3Export = Boolean.parseBoolean(properties.getOrDefault(ENABLE_S3_EXPORT, "false"));
    }

    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        HashMap<String, String> environment = new HashMap<>();

        // put it as environment variable so we can put it as JDBC parameters later when creation connection (not with JDBC)
        Optional.ofNullable(connectionProperties.get(WAREHOUSE)).ifPresent(x -> environment.put(WAREHOUSE, x));
        Optional.ofNullable(connectionProperties.get(DATABASE)).ifPresent(x -> environment.put(DATABASE, x));
        Optional.ofNullable(connectionProperties.get(SCHEMA)).ifPresent(x -> environment.put(SCHEMA, x));

        // now construct jdbc string, Snowflake JDBC should just be plain JDBC String. Parameter in JDBC string will get upper case.
        StringBuilder connectionStringBuilder = new StringBuilder(getConnectionStringPrefix(connectionProperties));
        connectionStringBuilder.append(connectionProperties.get(HOST));
        if (connectionProperties.containsKey(PORT)) {
            connectionStringBuilder
                    .append(":")
                    .append(connectionProperties.get(PORT));
        }

        String jdbcParametersString = getJdbcParameters(connectionProperties);
        if (!Strings.isNullOrEmpty(jdbcParametersString)) {
            LOGGER.info("JDBC parameters found, adding to JDBC String");
            connectionStringBuilder.append(getSnowflakeJDBCParameterPrefix()).append(getJdbcParameters(connectionProperties));
        }

        environment.put(DEFAULT, connectionStringBuilder.toString());
        return environment;
    }

    @Override
    protected String getConnectionStringPrefix(Map<String, String> connectionProperties)
    {
        return "snowflake://jdbc:snowflake://";
    }

    /**
     * For Snowflake, we don't put warehouse, database or schema information to the JDBC String to avoid casing issues.
     * @param connectionProperties
     * @return
     */
    @Override
    protected String getDatabase(Map<String, String> connectionProperties)
    {
        return "";
    }

    @Override
    protected String getJdbcParametersSeparator()
    {
        return "&";
    }

    private String getSnowflakeJDBCParameterPrefix()
    {
        return "/?";
    }

    private static String getValueWrapperWithEscapedCharacter(String input)
    {
        return SNOWFLAKE_ESCAPE_CHARACTER + input + SNOWFLAKE_ESCAPE_CHARACTER;
    }

    private static boolean isGlueConnection(Map<String, String> properties)
    {
        return properties.containsKey(DEFAULT_GLUE_CONNECTION);
    }

    public static Map<String, String> getSnowFlakeParameter(Map<String, String> baseProperty, Map<String, String> connectionProperties)
    {
        logger.debug("getSnowFlakeParameter, Loading connection properties");
        Map<String, String> parameters = new HashMap<>(baseProperty);

        if (!isGlueConnection(connectionProperties)) {
            return parameters;
        }

        if (!connectionProperties.containsKey(SCHEMA)) {
            logger.debug("No schema specified in connection string");
        }

        parameters.put(WAREHOUSE_PROPERTY_KEY, getValueWrapperWithEscapedCharacter(connectionProperties.get(WAREHOUSE)));
        parameters.put(DB_PROPERTY_KEY, getValueWrapperWithEscapedCharacter(connectionProperties.get(DATABASE)));

        if (connectionProperties.containsKey(SCHEMA)) {
            logger.debug("Found schema specified");
            parameters.put(SCHEMA_PROPERTY_KEY, getValueWrapperWithEscapedCharacter(connectionProperties.get(SCHEMA)));
        }

        return parameters;
    }

    public boolean isS3ExportEnabled()
    {
        return enableS3Export;
    }
}
