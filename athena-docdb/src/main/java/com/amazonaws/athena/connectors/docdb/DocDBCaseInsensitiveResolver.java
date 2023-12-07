/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DocDBCaseInsensitiveResolver
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBCaseInsensitiveResolver.class);

    // This enable_case_insensitive match for schema and table name due to Athena lower case schema and table name.
    // Capital letters are permitted for DocumentDB.
    private static final String ENABLE_CASE_INSENSITIVE_MATCH = "enable_case_insensitive_match";

    private DocDBCaseInsensitiveResolver() {}

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

    private static boolean isCaseInsensitiveMatchEnable(Map<String, String> configOptions)
    {
        String enableCaseInsensitiveMatchEnvValue = configOptions.getOrDefault(ENABLE_CASE_INSENSITIVE_MATCH, "false").toLowerCase();
        boolean enableCaseInsensitiveMatch = enableCaseInsensitiveMatchEnvValue.equals("true");
        logger.info("{} environment variable set to: {}. Resolved to: {}",
                ENABLE_CASE_INSENSITIVE_MATCH, enableCaseInsensitiveMatchEnvValue, enableCaseInsensitiveMatch);

        return enableCaseInsensitiveMatch;
    }
}
