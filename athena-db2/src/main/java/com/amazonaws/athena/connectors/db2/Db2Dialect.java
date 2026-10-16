/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.Db2SqlDialect;

import java.util.Locale;

/**
 * IBM DB2 SQL dialect used to render Substrait-derived query plans. DB2 quotes identifiers with
 * double quotes. When the catalog casing filter is enabled, identifiers are upper-cased before quoting.
 */
public class Db2Dialect extends Db2SqlDialect
{
    public static final SqlDialect DEFAULT = Db2SqlDialect.DEFAULT;

    private final boolean catalogCasingFilter;

    public Db2Dialect(boolean catalogCasingFilter)
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
