/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;

import java.util.Locale;

/**
 * SQL Server-specific SQL dialect with catalog casing filter support. Uses square brackets ({@code []}) for identifier quoting.
 */
public class SqlServerDialect extends MssqlSqlDialect
{
    public static final SqlDialect DEFAULT = MssqlSqlDialect.DEFAULT;

    private final boolean catalogCasingFilter;

    public SqlServerDialect(boolean catalogCasingFilter)
    {
        super(DEFAULT_CONTEXT);
        this.catalogCasingFilter = catalogCasingFilter;
    }

    @Override
    public StringBuilder quoteIdentifier(StringBuilder buf, String identifier)
    {
        if (catalogCasingFilter) {
            String upper = identifier.toUpperCase(Locale.ROOT);
            return buf.append("[").append(upper.replace("]", "]]")).append("]");
        }
        return super.quoteIdentifier(buf, identifier);
    }
}
