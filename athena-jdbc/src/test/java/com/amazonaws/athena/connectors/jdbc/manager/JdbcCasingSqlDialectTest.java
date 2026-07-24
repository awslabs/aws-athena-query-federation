/*-
 * #%L
 * Amazon Athena JDBC Connector
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
package com.amazonaws.athena.connectors.jdbc.manager;

import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcCasingSqlDialectTest
{
    private static class TestDialect extends JdbcCasingSqlDialect
    {
        TestDialect(DatabaseProduct product, String quoteString, boolean catalogCasingFilter)
        {
            super(product, quoteString, catalogCasingFilter);
        }

        TestDialect(DatabaseProduct product, String openQuote, String closeQuote, boolean catalogCasingFilter)
        {
            super(product, openQuote, closeQuote, catalogCasingFilter);
        }
    }

    @Test
    void testQuoteIdentifierWithFilterEnabled()
    {
        TestDialect dialect = new TestDialect(SqlDialect.DatabaseProduct.POSTGRESQL, "\"", true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("\"EMPLOYEES\"", buf.toString());
    }

    @Test
    void testQuoteIdentifierWithFilterDisabled()
    {
        TestDialect dialect = new TestDialect(SqlDialect.DatabaseProduct.POSTGRESQL, "\"", false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        // Default Calcite behavior — quotes as-is
        assertEquals("\"employees\"", buf.toString());
    }

    @Test
    void testAsymmetricQuotesWithFilter()
    {
        TestDialect dialect = new TestDialect(SqlDialect.DatabaseProduct.MSSQL, "[", "]", true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        assertEquals("[EMPLOYEES]", buf.toString());
    }

    @Test
    void testAsymmetricQuotesWithoutFilter()
    {
        TestDialect dialect = new TestDialect(SqlDialect.DatabaseProduct.MSSQL, "[", "]", false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "employees");
        // Default Calcite MSSQL behavior
        assertEquals("[employees]", buf.toString());
    }

    @Test
    void testBacktickQuotingWithFilter()
    {
        TestDialect dialect = new TestDialect(SqlDialect.DatabaseProduct.MYSQL, "`", true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "my_table");
        assertEquals("`MY_TABLE`", buf.toString());
    }

    @Test
    void testAlreadyUppercaseWithFilter()
    {
        TestDialect dialect = new TestDialect(SqlDialect.DatabaseProduct.ORACLE, "\"", true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "EMPLOYEES");
        assertEquals("\"EMPLOYEES\"", buf.toString());
    }

    static Stream<Arguments> provideDialectConfigs()
    {
        return Stream.of(
                Arguments.of(SqlDialect.DatabaseProduct.POSTGRESQL, "\"", "\"", "test_schema", "\"TEST_SCHEMA\""),
                Arguments.of(SqlDialect.DatabaseProduct.MYSQL, "`", "`", "test_schema", "`TEST_SCHEMA`"),
                Arguments.of(SqlDialect.DatabaseProduct.ORACLE, "\"", "\"", "test_schema", "\"TEST_SCHEMA\""),
                Arguments.of(SqlDialect.DatabaseProduct.MSSQL, "[", "]", "test_schema", "[TEST_SCHEMA]")
        );
    }

    @ParameterizedTest
    @MethodSource("provideDialectConfigs")
    void testAllDialectsWithFilter(SqlDialect.DatabaseProduct product, String openQuote, String closeQuote,
                                   String identifier, String expected)
    {
        TestDialect dialect = new TestDialect(product, openQuote, closeQuote, true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, identifier);
        assertEquals(expected, buf.toString());
    }
}
