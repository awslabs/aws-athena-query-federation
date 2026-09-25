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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VerticaSqlDialectTest
{
    @Test
    public void defaultDialect_IsNonNull()
    {
        assertNotNull(VerticaSqlDialect.DEFAULT);
    }

    @Test
    public void quoteIdentifier_WithoutCasingFilter_QuotesVerbatim()
    {
        VerticaSqlDialect dialect = new VerticaSqlDialect(false);
        assertEquals("\"col\"", dialect.quoteIdentifier(new StringBuilder(), "col").toString());
    }

    @Test
    public void quoteIdentifier_WithCasingFilter_UpperCasesBeforeQuoting()
    {
        VerticaSqlDialect dialect = new VerticaSqlDialect(true);
        assertEquals("\"COL\"", dialect.quoteIdentifier(new StringBuilder(), "col").toString());
    }

    @Test
    public void quoteIdentifier_EmbeddedDoubleQuote_IsDoubled()
    {
        VerticaSqlDialect dialect = new VerticaSqlDialect(false);
        assertEquals("\"a\"\"b\"", dialect.quoteIdentifier(new StringBuilder(), "a\"b").toString());
    }
}
