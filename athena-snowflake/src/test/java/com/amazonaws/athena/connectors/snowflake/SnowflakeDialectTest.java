/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SnowflakeDialectTest
{
    /**
     * With the casing filter on, identifiers are uppercased before quoting, because Snowflake's
     * information_schema stores them uppercase.
     */
    @Test
    public void quoteIdentifier_withCasingFilter_uppercases()
    {
        SnowflakeDialect dialect = new SnowflakeDialect(true);

        assertEquals("\"MYTABLE\"", dialect.quoteIdentifier(new StringBuilder(), "mytable").toString());
        assertEquals("\"MYTABLE\"", dialect.quoteIdentifier(new StringBuilder(), "MyTable").toString());
        assertEquals("\"ALREADY_UPPER\"", dialect.quoteIdentifier(new StringBuilder(), "ALREADY_UPPER").toString());
    }

    /**
     * With the casing filter off, quoting is left to Calcite's SnowflakeSqlDialect, which preserves
     * the identifier as given.
     */
    @Test
    public void quoteIdentifier_withoutCasingFilter_preservesCase()
    {
        SnowflakeDialect dialect = new SnowflakeDialect(false);

        assertEquals("\"mytable\"", dialect.quoteIdentifier(new StringBuilder(), "mytable").toString());
        assertEquals("\"MyTable\"", dialect.quoteIdentifier(new StringBuilder(), "MyTable").toString());
    }

    /**
     * A double quote inside an identifier must be escaped by doubling it, otherwise the generated
     * SQL would terminate the quoted identifier early.
     */
    @Test
    public void quoteIdentifier_withCasingFilter_escapesEmbeddedQuote()
    {
        SnowflakeDialect dialect = new SnowflakeDialect(true);

        assertEquals("\"WEIRD\"\"NAME\"", dialect.quoteIdentifier(new StringBuilder(), "weird\"name").toString());
    }

    /**
     * quoteIdentifier appends to the supplied buffer rather than replacing it.
     */
    @Test
    public void quoteIdentifier_appendsToExistingBuffer()
    {
        SnowflakeDialect dialect = new SnowflakeDialect(true);

        StringBuilder buf = new StringBuilder("SELECT * FROM ");
        assertEquals("SELECT * FROM \"T\"", dialect.quoteIdentifier(buf, "t").toString());
    }

    @Test
    public void defaultDialect_isAvailable()
    {
        assertNotNull(SnowflakeDialect.DEFAULT);
    }
}
