/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver.resolver;

import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SQLServerJDBCCaseResolver
        extends DefaultJDBCCaseResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLServerJDBCCaseResolver.class);

    private static final String SCHEMA_NAME_QUERY_TEMPLATE = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(SCHEMA_NAME) = ?";
    private static final String SCHEMA_NAME_COLUMN_KEY = "SCHEMA_NAME";
    private static final String TABLE_NAME_QUERY_TEMPLATE = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND LOWER(TABLE_NAME) = ?";
    private static final String TABLE_NAME_COLUMN_KEY = "TABLE_NAME";

    public SQLServerJDBCCaseResolver(String sourceType)
    {
        super(sourceType, FederationSDKCasingMode.NONE, FederationSDKCasingMode.LOWER);
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
}
