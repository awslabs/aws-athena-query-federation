/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle.resolver;

import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OracleJDBCCaseResolver
        extends DefaultJDBCCaseResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleJDBCCaseResolver.class);
    private static final String SCHEMA_NAME_QUERY_TEMPLATE = "SELECT DISTINCT OWNER as \"OWNER\" FROM all_tables WHERE lower(OWNER) = ?";
    private static final String TABLE_NAME_QUERY_TEMPLATE = "SELECT DISTINCT TABLE_NAME as \"TABLE_NAME\" FROM all_tables WHERE OWNER = ? and lower(TABLE_NAME) = ?";
    private static final String SCHEMA_NAME_COLUMN_KEY = "OWNER";
    private static final String TABLE_NAME_COLUMN_KEY = "TABLE_NAME";
    private static final String ORACLE_STRING_LITERAL_CHARACTER = "\'";

    public OracleJDBCCaseResolver(String sourceType)
    {
        super(sourceType, FederationSDKCasingMode.UPPER, FederationSDKCasingMode.LOWER);
    }

    @Override
    protected String getCaseInsensitivelySchemaNameQueryTemplate()
    {
        return SCHEMA_NAME_QUERY_TEMPLATE;
    }

    @Override
    protected String getCaseInsensitivelySchemaNameColumnKey()
    {
        return SCHEMA_NAME_COLUMN_KEY;
    }

    @Override
    protected List<String> getCaseInsensitivelyTableNameQueryTemplate()
    {
        return List.of(TABLE_NAME_QUERY_TEMPLATE);
    }

    @Override
    protected String getCaseInsensitivelyTableNameColumnKey()
    {
        return TABLE_NAME_COLUMN_KEY;
    }

    public static String convertToLiteral(String input)
    {
        if (!input.contains(ORACLE_STRING_LITERAL_CHARACTER)) {
            input = ORACLE_STRING_LITERAL_CHARACTER + input + ORACLE_STRING_LITERAL_CHARACTER;
        }
        return input;
    }
}
