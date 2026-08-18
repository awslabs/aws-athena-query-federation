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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class Db2As400DialectTest
{
    @Test
    void testDefaultDialect()
    {
        assertNotNull(Db2As400Dialect.DEFAULT);
    }

    @Test
    void testQuoteIdentifierWithFilter()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("\"EMPLOYEES\"", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithoutFilter()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("\"employees\"", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithFilterUpperCasesMixedCaseInput()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "Employees");
        assertEquals("\"EMPLOYEES\"", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithoutFilterPreservesUpperCaseInput()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "EMPLOYEES");
        assertEquals("\"EMPLOYEES\"", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithFilterEscapesDoubleQuote()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "emp\"loyees");
        assertEquals("\"EMP\"\"LOYEES\"", buf.toString());
    }

    // Db2 for i rejects the NULLS FIRST/LAST keyword (SQL0199). For a non-default null ordering the
    // dialect must emulate it with an "IS NULL" companion sort key rather than emit the keyword.
    private static SqlNode column()
    {
        return new SqlIdentifier("ID", SqlParserPos.ZERO);
    }

    @Test
    void testEmulateNullDirectionDescNullsLastUsesIsNull()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(true);
        SqlNode emulated = dialect.emulateNullDirection(column(), false, true);
        assertNotNull(emulated);
        assertEquals("\"ID\" IS NULL", emulated.toSqlString(dialect).getSql());
    }

    @Test
    void testEmulateNullDirectionAscNullsFirstUsesIsNull()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(true);
        SqlNode emulated = dialect.emulateNullDirection(column(), true, false);
        assertNotNull(emulated);
        assertEquals("\"ID\" IS NULL DESC", emulated.toSqlString(dialect).getSql());
    }

    @Test
    void testEmulateNullDirectionDefaultOrderEmitsNoKeyword()
    {
        Db2As400Dialect dialect = new Db2As400Dialect(true);
        // Db2 null collation is HIGH: DESC NULLS FIRST and ASC NULLS LAST are the defaults, so no
        // emulation and no NULLS keyword are required.
        assertNull(dialect.emulateNullDirection(column(), true, true));
        assertNull(dialect.emulateNullDirection(column(), false, false));
    }
}
