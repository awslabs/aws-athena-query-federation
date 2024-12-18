
/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;

public class OracleCaseResolver
{
    private static final Logger logger = LoggerFactory.getLogger(OracleCaseResolver.class);

    // the environment variable that can be set to specify which casing mode to use
    static final String CASING_MODE = "casing_mode";

    private OracleCaseResolver() {}

    private enum OracleCasingMode
    {
        LOWER,      // casing mode to use whatever the engine returns (in trino's case, lower)
        UPPER,      // casing mode to upper case everything (oracle by default upper cases everything)
        SEARCH      // casing mode to perform case insensitive search
    }

    public static String getSchemaNameCaseInsensitiveMatch(Map<String, String> configOptions, MongoClient client, String unresolvedSchemaName)
    {
        String resolvedSchemaName = unresolvedSchemaName;
        if (isCaseInsensitiveMatchEnable(configOptions)) {
            logger.info("CaseInsensitiveMatch enable, SchemaName input: {}", resolvedSchemaName);
            List<String> candidateSchemaNames = StreamSupport.stream(client.listDatabaseNames().spliterator(), false)
                    .filter(candidateSchemaName -> candidateSchemaName.equalsIgnoreCase(unresolvedSchemaName))
                    .collect(Collectors.toList());
            if (candidateSchemaNames.size() != 1) {
                throw new IllegalArgumentException(String.format("Schema name is empty or more than 1 for case insensitive match. schemaName: %s, size: %d",
                        unresolvedSchemaName, candidateSchemaNames.size()));
            }
            resolvedSchemaName = candidateSchemaNames.get(0);
            logger.info("CaseInsensitiveMatch, SchemaName resolved to: {}", resolvedSchemaName);
        }

        return resolvedSchemaName;
    }

    public static String getTableNameCaseInsensitiveMatch(Map<String, String> configOptions, MongoDatabase mongoDatabase, String unresolvedTableName)
    {
        String resolvedTableName = unresolvedTableName;
        if (isCaseInsensitiveMatchEnable(configOptions)) {
            logger.info("CaseInsensitiveMatch enable, TableName input: {}", resolvedTableName);
            List<String> candidateTableNames = StreamSupport.stream(mongoDatabase.listCollectionNames().spliterator(), false)
                    .filter(candidateTableName -> candidateTableName.equalsIgnoreCase(unresolvedTableName))
                    .collect(Collectors.toList());
            if (candidateTableNames.size() != 1) {
                throw new IllegalArgumentException(String.format("Table name is empty or more than 1 for case insensitive match. schemaName: %s, size: %d",
                        unresolvedTableName, candidateTableNames.size()));
            }
            resolvedTableName = candidateTableNames.get(0);
            logger.info("CaseInsensitiveMatch, TableName resolved to: {}", resolvedTableName);
        }
        return resolvedTableName;
    }

    private static OracleCasingMode getCasingMode(Map<String, String> configOptions)
    {
        boolean isGlueConnection = StringUtils.isNotBlank(configOptions.get(DEFAULT_GLUE_CONNECTION));
        if (!configOptions.containsKey(CASING_MODE)) {
            logger.info("CASING MODE not set");
            return isGlueConnection ? OracleCasingMode.LOWER : OracleCasingMode.UPPER;
        }

        try {
            OracleCasingMode oracleCasingMode = OracleCasingMode.valueOf(configOptions.get(CASING_MODE).toUpperCase());
            logger.info("CASING MODE enable: {}", oracleCasingMode.toString());
            return oracleCasingMode;
        }
        catch (Exception ex) {
            // print error log for customer along with list of input
            logger.error("Invalid input for:{}, input value:{}, valid values:{}", CASING_MODE, configOptions.get(CASING_MODE), Arrays.asList(OracleCasingMode.values()), ex);
            throw ex;
        }
    }
}
