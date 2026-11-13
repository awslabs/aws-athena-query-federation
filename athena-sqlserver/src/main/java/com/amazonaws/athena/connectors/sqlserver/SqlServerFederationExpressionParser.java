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

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_END;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_START;

public class SqlServerFederationExpressionParser extends JdbcFederationExpressionParser
{
    /**
     * Parent requires a quote-character argument. It is unused: wrapping uses
     * {@link SqlServerConstants#SQLSERVER_QUOTE_START} and {@link SqlServerConstants#SQLSERVER_QUOTE_END}.
     */
    public SqlServerFederationExpressionParser()
    {
        super("");
    }

    /**
     * SQL Server column names are quoted with square brackets ({@code [col]}), valid regardless of the
     * session {@code QUOTED_IDENTIFIER} setting. An embedded closing bracket is escaped by doubling it.
     */
    @Override
    public String parseVariableExpression(VariableExpression variableExpression)
    {
        return SQLSERVER_QUOTE_START + variableExpression.getColumnName().replace(SQLSERVER_QUOTE_END, SQLSERVER_QUOTE_END + SQLSERVER_QUOTE_END) + SQLSERVER_QUOTE_END;
    }

    @Override
    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        return Joiner.on(", ").join(arguments);
    }    
}
