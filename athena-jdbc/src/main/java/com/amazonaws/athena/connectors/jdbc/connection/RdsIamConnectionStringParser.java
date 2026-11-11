/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.credentials.RdsIamAuthConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Parses JDBC connection strings for optional RDS IAM database authentication settings.
 */
public final class RdsIamConnectionStringParser
{
    public static final String IAM_AUTH_PARAM = "iamAuth";
    public static final String REGION_PARAM = "region";

    private static final Pattern JDBC_URL_PATTERN = Pattern.compile("^jdbc:([^:]+)://([^:/?#]+)(?::(\\d+))?(/[^?]*)?(\\?.*)?$");
    private static final Pattern RDS_HOST_REGION_PATTERN = Pattern.compile("\\.([a-z]{2}-[a-z]+-\\d+)\\.rds\\.amazonaws\\.com(\\.cn)?$");

    private RdsIamConnectionStringParser() {}

    public static final class ParsedJdbcConnection
    {
        private final String jdbcConnectionString;
        private final RdsIamAuthConfiguration iamAuthConfiguration;

        private ParsedJdbcConnection(final String jdbcConnectionString, final RdsIamAuthConfiguration iamAuthConfiguration)
        {
            this.jdbcConnectionString = jdbcConnectionString;
            this.iamAuthConfiguration = iamAuthConfiguration;
        }

        public String getJdbcConnectionString()
        {
            return jdbcConnectionString;
        }

        public RdsIamAuthConfiguration getIamAuthConfiguration()
        {
            return iamAuthConfiguration;
        }

        public boolean isIamAuthEnabled()
        {
            return iamAuthConfiguration != null;
        }
    }

    public static ParsedJdbcConnection parse(final String jdbcConnectionString)
    {
        Validate.notBlank(jdbcConnectionString, "jdbcConnectionString must not be blank");

        final Matcher matcher = JDBC_URL_PATTERN.matcher(jdbcConnectionString);
        if (!matcher.matches()) {
            return new ParsedJdbcConnection(jdbcConnectionString, null);
        }

        final String subprotocol = matcher.group(1);
        final String hostname = matcher.group(2);
        final String portGroup = matcher.group(3);
        final String path = matcher.group(4) != null ? matcher.group(4) : "";
        final String query = matcher.group(5);

        if (StringUtils.isBlank(query)) {
            return new ParsedJdbcConnection(jdbcConnectionString, null);
        }

        final Map<String, String> queryParams = parseQueryParams(query.substring(1));
        if (!Boolean.parseBoolean(queryParams.get(IAM_AUTH_PARAM))) {
            return new ParsedJdbcConnection(jdbcConnectionString, null);
        }

        if (portGroup == null) {
            throw new AthenaConnectorException("RDS IAM authentication requires port in the JDBC connection string",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        final int port = Integer.parseInt(portGroup);
        final String username = getUsername(queryParams);
        if (StringUtils.isBlank(username)) {
            throw new AthenaConnectorException("RDS IAM authentication requires a database user via the user connection property",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        final Region region = resolveRegion(hostname, queryParams.get(REGION_PARAM));
        final String sanitizedJdbcConnectionString = buildSanitizedJdbcConnectionString(subprotocol, hostname, port, path, queryParams);

        return new ParsedJdbcConnection(
                sanitizedJdbcConnectionString,
                new RdsIamAuthConfiguration(hostname, port, username, region));
    }

    private static Map<String, String> parseQueryParams(final String query)
    {
        final Map<String, String> params = new LinkedHashMap<>();
        if (StringUtils.isBlank(query)) {
            return params;
        }

        Arrays.stream(query.split("&"))
                .map(pair -> pair.split("=", 2))
                .filter(parts -> parts.length == 2 && StringUtils.isNotBlank(parts[0]))
                .forEach(parts -> params.put(parts[0], parts[1]));
        return params;
    }

    private static String getUsername(final Map<String, String> queryParams)
    {
        final String user = queryParams.get("user");
        if (StringUtils.isNotBlank(user)) {
            return user;
        }
        return queryParams.get("username");
    }

    private static Region resolveRegion(final String hostname, final String regionOverride)
    {
        if (StringUtils.isNotBlank(regionOverride)) {
            return Region.of(regionOverride);
        }

        final Matcher regionMatcher = RDS_HOST_REGION_PATTERN.matcher(hostname);
        if (regionMatcher.find()) {
            return Region.of(regionMatcher.group(1));
        }

        throw new AthenaConnectorException("Unable to determine AWS region for RDS IAM authentication. "
                        + "Provide the region connection property or use a standard RDS endpoint hostname.",
                ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
    }

    private static String buildSanitizedJdbcConnectionString(
            final String subprotocol,
            final String hostname,
            final int port,
            final String path,
            final Map<String, String> queryParams)
    {
        final String sanitizedQuery = queryParams.entrySet().stream()
                .filter(entry -> !isIamAuthControlParam(entry.getKey()))
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));

        final String base = "jdbc:" + subprotocol + "://" + hostname + ":" + port + path;
        if (StringUtils.isBlank(sanitizedQuery)) {
            return base;
        }
        return base + "?" + sanitizedQuery;
    }

    private static boolean isIamAuthControlParam(final String key)
    {
        return IAM_AUTH_PARAM.equalsIgnoreCase(key)
                || REGION_PARAM.equalsIgnoreCase(key)
                || "user".equalsIgnoreCase(key)
                || "username".equalsIgnoreCase(key)
                || "password".equalsIgnoreCase(key);
    }
}
