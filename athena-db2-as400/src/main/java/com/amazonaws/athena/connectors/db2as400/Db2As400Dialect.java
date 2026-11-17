/*-
 * #%L
 * athena-db2-as400
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
package com.amazonaws.athena.connectors.db2as400;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.Db2SqlDialect;

import java.util.Locale;

/**
 * IBM Db2 for i (iSeries) SQL dialect used to render Substrait-derived query plans. Db2 quotes
 * identifiers with double quotes. When the catalog casing filter is enabled, identifiers are
 * upper-cased before quoting.
 */
public class Db2As400Dialect extends Db2SqlDialect
{
    public static final SqlDialect DEFAULT = Db2SqlDialect.DEFAULT;

    private final boolean catalogCasingFilter;

    public Db2As400Dialect(boolean catalogCasingFilter)
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

    /**
     * Db2 for i does not support the {@code NULLS FIRST}/{@code NULLS LAST} keyword and rejects it
     * with SQL0199 at prepare time. When a Substrait sort requests a null ordering that differs from
     * Db2's default null collation, emulate it with an {@code IS NULL} companion sort key instead of
     * emitting the unsupported keyword, so the generated {@code ORDER BY} is valid on Db2 for i.
     */
    @Override
    public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc)
    {
        return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }
}
