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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class JDBCCaseResolver
        extends CaseResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCCaseResolver.class);
    // We only want to support below type for annotation for backward compatibility.
    private static final Set<String> ANNOTATION_SUPPORTED_SOURCE_TYPES = Set.of("snowflake", "saphana");
    private static final String ANNOTATION_CASE_UPPER = "upper";
    private static final String ANNOTATION_CASE_LOWER = "lower";

    public JDBCCaseResolver(String sourceType)
    {
        super(sourceType, FederationSDKCasingMode.NONE, FederationSDKCasingMode.NONE);
    }

    public JDBCCaseResolver(String sourceType, FederationSDKCasingMode nonGlueBasedDefaultCasingMode, FederationSDKCasingMode glueConnectionBasedDefaultCasingMode)
    {
        super(sourceType, nonGlueBasedDefaultCasingMode, glueConnectionBasedDefaultCasingMode);
    }

    public final TableName getAdjustedTableNameObject(final Connection connection, TableName tableNameObject, Map<String, String> configOptions)
    {
        // only support annotation for backward compatibility...
        if (getCasingMode(configOptions) == FederationSDKCasingMode.ANNOTATION) {
            return Optional.of(sourceType)
                    .filter(ANNOTATION_SUPPORTED_SOURCE_TYPES::contains)
                    .map(s -> getTableNameFromQueryAnnotation(tableNameObject))
                    .orElseThrow(() -> new UnsupportedOperationException(String.format("casing mode `ANNOTATION` is not supported for type: '%s'", sourceType)));
        }

        String schemaNameInCorrectCasing = getAdjustedSchemaNameString(connection, tableNameObject.getSchemaName(), configOptions);
        String tableNameInCorrectCasing = getAdjustedTableNameString(connection, schemaNameInCorrectCasing, tableNameObject.getTableName(), configOptions);

        return new TableName(schemaNameInCorrectCasing, tableNameInCorrectCasing);
    }

    public final String getAdjustedSchemaNameString(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
    {
        FederationSDKCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case LOWER:
                LOGGER.debug("casing mode is `LOWER`: adjusting casing from input to lower case from:{}, to:{}", schemaNameInput, schemaNameInput.toLowerCase());
                return schemaNameInput.toLowerCase();
            case UPPER:
                LOGGER.debug("casing mode is `UPPER`: adjusting casing from input to upper case from:{}, to:{}", schemaNameInput, schemaNameInput.toUpperCase());
                return schemaNameInput.toUpperCase();
            case CASE_INSENSITIVE_SEARCH:
                return getSchemaNameCaseInsensitively(connection, schemaNameInput, configOptions);
            case ANNOTATION:
                throw new UnsupportedOperationException("casing mode `ANNOTATION` is not supported for Name level. Please use CASE_INSENSITIVE_SEARCH mode");
            case NONE:
                LOGGER.debug("casing mode is `NONE`: not adjust casing from input: {}", schemaNameInput);
                return schemaNameInput;
        }
        LOGGER.debug("NO casing mode : not adjust casing from input: {}", schemaNameInput);
        return schemaNameInput;
    }

    public final String getAdjustedTableNameString(final Connection connection, String schemaNameInCorrectCase, String tableNameInput, Map<String, String> configOptions)
    {
        FederationSDKCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case LOWER:
                LOGGER.debug("casing mode is `LOWER`: adjusting casing for 'TABLE_NAME' from input to lower case from:{}, to:{}", tableNameInput, tableNameInput.toLowerCase());
                return tableNameInput.toLowerCase();
            case UPPER:
                LOGGER.debug("casing mode is `UPPER`: adjusting casing 'TABLE_NAME' from input to upper case from:{}, to:{}", tableNameInput, tableNameInput.toUpperCase());
                return tableNameInput.toUpperCase();
            case CASE_INSENSITIVE_SEARCH:
                return getTableNameCaseInsensitively(connection, schemaNameInCorrectCase, tableNameInput, configOptions);
            case ANNOTATION:
                throw new UnsupportedOperationException("casing mode `ANNOTATION` is not supported for Name level. Please use CASE_INSENSITIVE_SEARCH mode");
            case NONE:
                LOGGER.debug("casing mode is `NONE`: not adjust casing 'TABLE_NAME' input: {}", tableNameInput);
                return tableNameInput;
        }
        LOGGER.debug("NO casing mode : not adjust casing 'TABLE_NAME' input: {}", tableNameInput);
        return tableNameInput;
    }

    private String getSchemaNameCaseInsensitively(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
    {
        List<String> strings = doGetSchemaNameCaseInsensitively(connection, schemaNameInput, configOptions);
        if (strings.size() != 1) {
            throw new RuntimeException(String.format("Schema name case insensitive match failed, number of match : %d, values: %s", strings.size(), strings));
        }

        LOGGER.info("casing mode is `CASE_INSENSITIVE_SEARCH`: adjusting casing for `Schema` from {}, to {}", schemaNameInput, strings.get(0));
        return strings.get(0);
    }

    protected List<String> doGetSchemaNameCaseInsensitively(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
    {
        throw new UnsupportedOperationException(String.format("CASE_INSENSITIVE_SEARCH is not supported for type: '%s'", sourceType));
    }

    private String getTableNameCaseInsensitively(final Connection connection, String schemaNameInCorrectCase, String tableNameInput, Map<String, String> configOptions)
    {
        List<String> strings = doGetTableNameCaseInsensitively(connection, schemaNameInCorrectCase, tableNameInput, configOptions);
        if (strings.size() != 1) {
            throw new RuntimeException(String.format("Table name case insensitive match failed, number of match : %d, values: %s", strings.size(), strings));
        }

        LOGGER.info("casing mode is `ANNOTATION`: adjusting casing for `Table` from {}, to {}", tableNameInput, strings.get(0));
        return strings.get(0);
    }

    protected List<String> doGetTableNameCaseInsensitively(final Connection connection, String schemaNameInCorrectCase, String tableNameInput, Map<String, String> configOptions)
    {
        throw new UnsupportedOperationException(String.format("CASE_INSENSITIVE_SEARCH is not supported for type: '%s'", sourceType));
    }

    protected String getCaseInsensitivelySchemaNameQueryTemplate()
    {
        throw new UnsupportedOperationException(String.format("CASE_INSENSITIVE_SEARCH is not supported for type: '%s', please provide SchemaNameQuery", sourceType));
    }

    protected String getCaseInsensitivelySchemaNameColumnKey()
    {
        throw new UnsupportedOperationException(String.format("CASE_INSENSITIVE_SEARCH is not supported for type: '%s', please provide SchemaNameColumnKey", sourceType));
    }

    protected List<String> getCaseInsensitivelyTableNameQueryTemplate()
    {
        throw new UnsupportedOperationException(String.format("CASE_INSENSITIVE_SEARCH is not supported for type: '%s', please provide TableNameQuery", sourceType));
    }

    protected String getCaseInsensitivelyTableNameColumnKey()
    {
        throw new UnsupportedOperationException(String.format("CASE_INSENSITIVE_SEARCH is not supported for type: '%s', please provide TableNameColumnKey", sourceType));
    }

    /*
    **This only works on TableName Object level**
    Keep previous implementation of table name casing adjustment from query hint.
    This is to keep backward compatibility.
     */
    @Deprecated
    protected TableName getTableNameFromQueryAnnotation(TableName table)
    {
        LOGGER.info("getTableNameFromQueryAnnotation: " + table);
        //if no query hints has been passed then return input table name
        if (!table.getTableName().contains("@")) {
            return new TableName(table.getSchemaName(), table.getTableName());
        }
        //analyze the hint to find table and schema case
        String[] tbNameWithQueryHint = table.getTableName().split("@");
        String[] hintDetails = tbNameWithQueryHint[1].split("&");
        String schemaCase = ANNOTATION_CASE_UPPER;
        String tableCase = ANNOTATION_CASE_UPPER;
        String tableName = tbNameWithQueryHint[0];
        for (String str : hintDetails) {
            String[] hintDetail = str.split("=");
            if (hintDetail[0].contains("schema")) {
                schemaCase = hintDetail[1];
            }
            else if (hintDetail[0].contains("table")) {
                tableCase = hintDetail[1];
            }
        }
        if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toLowerCase());
        }
        else if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toLowerCase());
        }
        else {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
    }
}
