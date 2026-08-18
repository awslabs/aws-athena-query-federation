/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DataLakeGen2DialectTest
{
    @Test
    void testDefaultDialect()
    {
        assertNotNull(DataLakeGen2Dialect.DEFAULT);
    }

    @Test
    void testQuoteIdentifierWithFilter()
    {
        DataLakeGen2Dialect dialect = new DataLakeGen2Dialect(true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("[EMPLOYEES]", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithoutFilter()
    {
        DataLakeGen2Dialect dialect = new DataLakeGen2Dialect(false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("[employees]", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithFilterUpperCasesMixedCaseInput()
    {
        DataLakeGen2Dialect dialect = new DataLakeGen2Dialect(true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "Employees");
        assertEquals("[EMPLOYEES]", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithoutFilterPreservesUpperCaseInput()
    {
        DataLakeGen2Dialect dialect = new DataLakeGen2Dialect(false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "EMPLOYEES");
        assertEquals("[EMPLOYEES]", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithFilterEscapesClosingBracket()
    {
        DataLakeGen2Dialect dialect = new DataLakeGen2Dialect(true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "emp]loyees");
        assertEquals("[EMP]]LOYEES]", buf.toString());
    }
}
