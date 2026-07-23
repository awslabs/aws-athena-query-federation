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

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class OracleSqlDialectTest
{
    @Test
    void testDefaultDialect()
    {
        assertNotNull(OracleSqlDialect.DEFAULT);
    }

    @Test
    void testQuoteIdentifierWithFilter()
    {
        OracleSqlDialect dialect = new OracleSqlDialect(true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("\"EMPLOYEES\"", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithoutFilter()
    {
        OracleSqlDialect dialect = new OracleSqlDialect(false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("\"employees\"", buf.toString());
    }
}
