/*-
 * #%L
 * Amazon Athena Oracle Connector
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
package com.amazonaws.athena.connectors.oracle;

import org.apache.calcite.sql.SqlDialect;

public class OracleSqlDialect extends SqlDialect
{
    private final boolean catalogCasingFilter;

    public static final SqlDialect DEFAULT = org.apache.calcite.sql.dialect.OracleSqlDialect.DEFAULT;

    private OracleSqlDialect(Context context, boolean catalogCasingFilter)
    {
        super(context);
        this.catalogCasingFilter = catalogCasingFilter;
    }

    public OracleSqlDialect(boolean catalogCasingFilter)
    {
        this(EMPTY_CONTEXT
                .withDatabaseProduct(DatabaseProduct.ORACLE)
                .withIdentifierQuoteString("\""), catalogCasingFilter);
    }

    @Override
    public StringBuilder quoteIdentifier(StringBuilder buf, String identifier)
    {
        if (catalogCasingFilter) {
            // Oracle requires uppercase identifiers to be in quotes for case-sensitive tables
            return buf.append("\"").append(identifier.toUpperCase()).append("\"");
        }
        else {
            return super.quoteIdentifier(buf, identifier);
        }
    }
}
