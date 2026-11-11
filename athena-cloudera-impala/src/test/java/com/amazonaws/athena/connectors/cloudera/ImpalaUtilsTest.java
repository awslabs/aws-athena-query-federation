/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ImpalaUtilsTest
{
    @Test
    public void quoteIdentifier_whenNameIsSimple_returnsBacktickWrappedName()
    {
        assertEquals("`a`", ImpalaUtils.quoteIdentifier("a"));
    }

    @Test
    public void quoteIdentifier_whenNameContainsBacktick_doublesEmbeddedBackticks()
    {
        assertEquals("`a``b`", ImpalaUtils.quoteIdentifier("a`b"));
    }

    @Test
    public void quoteStringLiteral_whenValueIsSimple_returnsSingleQuotedLiteral()
    {
        assertEquals("'1'", ImpalaUtils.quoteStringLiteral("1"));
    }

    @Test
    public void quoteStringLiteral_whenValueContainsOrTrue_staysInsideQuotedLiteral()
    {
        assertEquals("'1 OR true --'", ImpalaUtils.quoteStringLiteral("1 OR true --"));
    }

    @Test
    public void quoteStringLiteral_whenValueContainsSingleQuote_doublesEmbeddedQuote()
    {
        assertEquals("'O''REILLY'", ImpalaUtils.quoteStringLiteral("O'REILLY"));
    }

    @Test
    public void quoteStringLiteral_whenValueEndsWithBackslash_escapesBackslashSoLiteralStaysClosed()
    {
        assertEquals("'x\\\\'", ImpalaUtils.quoteStringLiteral("x\\"));
    }

    @Test
    public void partitionValueExpression_whenBooleanTrue_returnsUnquotedTrue()
    {
        assertEquals("true", ImpalaUtils.partitionValueExpression("boolean", "true"));
        assertEquals("TRUE", ImpalaUtils.partitionValueExpression("BOOLEAN", "TRUE"));
    }

    @Test
    public void partitionValueExpression_whenBooleanFalse_returnsUnquotedFalse()
    {
        assertEquals("false", ImpalaUtils.partitionValueExpression("boolean", "false"));
    }

    @Test
    public void partitionValueExpression_whenIntValue_returnsUnquotedLiteral()
    {
        assertEquals("2020", ImpalaUtils.partitionValueExpression("int", "2020"));
    }

    @Test
    public void partitionValueExpression_whenStringLikeType_quotesAsStringLiteral()
    {
        assertEquals("'Hyderabad'", ImpalaUtils.partitionValueExpression("string", "Hyderabad"));
        assertEquals("'Hyderabad'", ImpalaUtils.partitionValueExpression("varchar", "Hyderabad"));
        assertEquals("'2'", ImpalaUtils.partitionValueExpression("char(64)", "2"));
        assertEquals("'1 OR true --'", ImpalaUtils.partitionValueExpression("char(64)", "1 OR true --"));
        assertEquals("'2020-01-01'", ImpalaUtils.partitionValueExpression("date", "2020-01-01"));
        assertEquals("'2020-01-01'", ImpalaUtils.partitionValueExpression("date(10)", "2020-01-01"));
        assertEquals("'2020-01-01'", ImpalaUtils.partitionValueExpression("DATE(", "2020-01-01"));
    }

    @Test
    public void partitionValueExpression_whenColumnTypeIsNull_returnsUnquotedValue()
    {
        assertEquals("Hyderabad", ImpalaUtils.partitionValueExpression(null, "Hyderabad"));
    }
    
    @Test
    public void quoteIdentifier_whenIdentifierIsEmpty_returnsQuotedEmptyIdentifier()
    {
        assertEquals("``", ImpalaUtils.quoteIdentifier(""));
    }

    @Test(expected = NullPointerException.class)
    public void quoteIdentifier_whenIdentifierIsNull_throwsNullPointerException()
    {
        ImpalaUtils.quoteIdentifier(null);
    }

    @Test
    public void qualifiedTableForMetadataSql_whenTableNameIsStandard_returnsQuotedQualifiedName()
    {
        TableName tableName = new TableName("testSchema", "testTable");
        assertEquals("`TESTSCHEMA`.`TESTTABLE`", ImpalaUtils.qualifiedTableForMetadataSql(tableName));
    }

    @Test
    public void qualifiedTableForMetadataSql_whenTableNameHasSpecialCharacters_keepsCharactersInsideQuotes()
    {
        TableName tableName = new TableName("demo_security", "test; INSERT INTO x VALUES (1);--");
        String qualified = ImpalaUtils.qualifiedTableForMetadataSql(tableName);
        assertEquals("`DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`", qualified);
        assertEquals("describe FORMATTED `DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`",
                ImpalaMetadataHandler.GET_METADATA_QUERY + qualified);
        assertFalse(qualified.contains("`DEMO_SECURITY`.TEST;"));
    }

    @Test(expected = NullPointerException.class)
    public void qualifiedTableForMetadataSql_whenTableNameIsNull_throwsNullPointerException()
    {
        ImpalaUtils.qualifiedTableForMetadataSql(null);
    }
}
