/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryFactory;
import com.amazonaws.athena.connectors.jdbc.manager.TemplateBasedJdbcFederationExpressionParser;

/**
 * SQL Server implementation of FederationExpressionParser using StringTemplate.
 * Extends TemplateBasedJdbcFederationExpressionParser which provides the common
 * template-based implementation for all migrated JDBC connectors.
 */
public class SqlServerFederationExpressionParser extends TemplateBasedJdbcFederationExpressionParser
{
    public SqlServerFederationExpressionParser(String quoteChar)
    {
        super(quoteChar);
    }

    @Override
    protected JdbcQueryFactory getQueryFactory()
    {
        return SqlServerSqlUtils.getQueryFactory();
    }
}
