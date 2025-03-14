/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.resolver;

import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultJDBCCaseResolver extends JDBCCaseResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJDBCCaseResolver.class);

    public DefaultJDBCCaseResolver(String sourceType)
    {
        super(sourceType, FederationSDKCasingMode.NONE, FederationSDKCasingMode.NONE);
    }

    public DefaultJDBCCaseResolver(String sourceType, FederationSDKCasingMode nonGlueDefaultMode, FederationSDKCasingMode glueConnectionDefaultMode)
    {
        super(sourceType, nonGlueDefaultMode, glueConnectionDefaultMode);
    }

    protected List<String> doGetSchemaNameCaseInsensitively(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
    {
        List<String> results = new ArrayList<>();
        try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                .withConnection(connection)
                .withQuery(getCaseInsensitivelySchemaNameQueryTemplate())
                .withParameters(List.of(schemaNameInput.toLowerCase())).build();
                ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                String schemaNameCandidate = resultSet.getString(getCaseInsensitivelySchemaNameColumnKey());
                LOGGER.debug("Case insensitive search on columLabel: {}, schema name: {}", getCaseInsensitivelySchemaNameColumnKey(), schemaNameCandidate);
                results.add(schemaNameCandidate);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(String.format("getSchemaNameCaseInsensitively query failed for %s", schemaNameInput), e);
        }

        return results;
    }

    protected List<String> doGetTableNameCaseInsensitively(final Connection connection, String schemaNameInCorrectCase, String tableNameInput, Map<String, String> configOptions)
    {
        List<String> results = new ArrayList<>();
        List<String> caseInsensitivelyTableNameQueryTemplate = getCaseInsensitivelyTableNameQueryTemplate();
        LOGGER.debug("Number of query templates: {} for case insensitive search for tableName", caseInsensitivelyTableNameQueryTemplate.size());
        for (String tableNameQueryTemplate : caseInsensitivelyTableNameQueryTemplate) {
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                    .withConnection(connection)
                    .withQuery(tableNameQueryTemplate)
                    .withParameters(List.of(schemaNameInCorrectCase, tableNameInput.toLowerCase())).build();
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String schemaNameCandidate = resultSet.getString(getCaseInsensitivelyTableNameColumnKey());
                    LOGGER.debug("Case insensitive search on columLabel: {}, schema name: {}", getCaseInsensitivelyTableNameColumnKey(), schemaNameCandidate);
                    results.add(schemaNameCandidate);
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(String.format("getTableNameCaseInsensitively query failed for schema: %s tableNameInput: %s", schemaNameInCorrectCase, tableNameInput), e);
            }
        }

        return results;
    }
}
