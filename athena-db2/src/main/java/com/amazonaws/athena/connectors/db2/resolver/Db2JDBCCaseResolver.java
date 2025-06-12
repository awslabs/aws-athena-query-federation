/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2.resolver;

import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;

import java.util.List;

public class Db2JDBCCaseResolver
        extends DefaultJDBCCaseResolver
{
    private static final String TABLE_NAME_QUERY_TEMPLATE = "SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND LOWER(TABNAME) = ?";
    private static final String SCHEMA_NAME_QUERY_TEMPLATE = "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE LOWER(SCHEMANAME) = ?";

    private static final String SCHEMA_NAME_COLUMN_KEY = "SCHEMANAME";
    private static final String TABLE_NAME_COLUMN_KEY = "TABNAME";

    public Db2JDBCCaseResolver(String sourceType)
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
}
