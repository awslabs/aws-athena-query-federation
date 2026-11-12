/*-
 * #%L
 * Amazon Athena Redshift Connector
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
package com.amazonaws.athena.connectors.redshift;

import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlQueryStringBuilder;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;

public class RedshiftSqlQueryStringBuilder extends PostGreSqlQueryStringBuilder
{
    public RedshiftSqlQueryStringBuilder(String quoteCharacters, FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
    }

    @Override
    protected SqlDialect getSqlDialect()
    {
        return RedshiftSqlDialect.DEFAULT;
    }
}
