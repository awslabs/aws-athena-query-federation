/*-
 * #%L
 * Amazon Athena JDBC Connector
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
package com.amazonaws.athena.connectors.jdbc.manager;

import org.apache.calcite.sql.SqlDialect;

public abstract class JdbcCasingSqlDialect extends SqlDialect
{
    private final boolean catalogCasingFilter;
    private final String openQuote;
    private final String closeQuote;

    protected JdbcCasingSqlDialect(DatabaseProduct product, String quoteString, boolean catalogCasingFilter)
    {
        this(product, quoteString, quoteString, catalogCasingFilter);
    }

    protected JdbcCasingSqlDialect(DatabaseProduct product, String openQuote, String closeQuote, boolean catalogCasingFilter)
    {
        super(EMPTY_CONTEXT.withDatabaseProduct(product).withIdentifierQuoteString(openQuote));
        this.catalogCasingFilter = catalogCasingFilter;
        this.openQuote = openQuote;
        this.closeQuote = closeQuote;
    }

    @Override
    public StringBuilder quoteIdentifier(StringBuilder buf, String identifier)
    {
        if (catalogCasingFilter) {
            return buf.append(openQuote).append(identifier.toUpperCase()).append(closeQuote);
        }
        return super.quoteIdentifier(buf, identifier);
    }
}
