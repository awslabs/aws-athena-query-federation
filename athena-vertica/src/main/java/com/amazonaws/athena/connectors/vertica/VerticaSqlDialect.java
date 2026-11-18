/*-
 * #%L
 * athena-vertica
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
package com.amazonaws.athena.connectors.vertica;

import org.apache.calcite.sql.SqlDialect;

import java.util.Locale;

/**
 * Vertica SQL dialect used to render Substrait-derived query plans into the SELECT that backs the
 * connector's EXPORT TO PARQUET statement. Vertica quotes identifiers with double quotes. When the
 * catalog casing filter is enabled, identifiers are upper-cased before quoting.
 */
public class VerticaSqlDialect extends org.apache.calcite.sql.dialect.VerticaSqlDialect
{
    public static final SqlDialect DEFAULT = org.apache.calcite.sql.dialect.VerticaSqlDialect.DEFAULT;

    private final boolean catalogCasingFilter;

    public VerticaSqlDialect(boolean catalogCasingFilter)
    {
        super(DEFAULT_CONTEXT);
        this.catalogCasingFilter = catalogCasingFilter;
    }

    @Override
    public StringBuilder quoteIdentifier(StringBuilder buf, String identifier)
    {
        String value = catalogCasingFilter ? identifier.toUpperCase(Locale.ROOT) : identifier;
        return buf.append("\"").append(value.replace("\"", "\"\"")).append("\"");
    }
}
