/*-
 * #%L
 * athena-cloudera-hive
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
import static org.junit.Assert.assertTrue;

public class HiveUtilsTest
{
    @Test
    public void quoteIdentifier_whenNameIsSimple_returnsBacktickWrappedName()
    {
        assertEquals("`a`", HiveUtils.quoteIdentifier("a"));
    }

    @Test
    public void quoteIdentifier_whenNameContainsBacktick_doublesEmbeddedBackticks()
    {
        assertEquals("`a``b`", HiveUtils.quoteIdentifier("a`b"));
    }

    @Test
    public void quoteIdentifier_whenIdentifierIsEmpty_returnsQuotedEmptyIdentifier()
    {
        assertEquals("``", HiveUtils.quoteIdentifier(""));
    }

    @Test(expected = NullPointerException.class)
    public void quoteIdentifier_whenIdentifierIsNull_throwsNullPointerException()
    {
        HiveUtils.quoteIdentifier(null);
    }

    @Test
    public void qualifiedTableForMetadataSql_whenTableNameIsStandard_returnsQuotedQualifiedName()
    {
        TableName tableName = new TableName("testSchema", "testTable");
        assertEquals("`TESTSCHEMA`.`TESTTABLE`", HiveUtils.qualifiedTableForMetadataSql(tableName));
    }

    @Test
    public void qualifiedTableForMetadataSql_whenTableNameHasSpecialCharacters_keepsCharactersInsideQuotes()
    {
        TableName tableName = new TableName("demo_security", "test; INSERT INTO x VALUES (1);--");
        String qualified = HiveUtils.qualifiedTableForMetadataSql(tableName);
        assertEquals("`DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`", qualified);
        assertEquals("describe `DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`",
                HiveMetadataHandler.GET_METADATA_QUERY + qualified);
    }

    @Test(expected = NullPointerException.class)
    public void qualifiedTableForMetadataSql_whenTableNameIsNull_throwsNullPointerException()
    {
        HiveUtils.qualifiedTableForMetadataSql(null);
    }

    @Test
    public void likePatternLiteral_whenPatternIsSimple_returnsUppercaseQuotedLiteral()
    {
        assertEquals("'TESTTABLE'", HiveUtils.likePatternLiteral("testTable"));
    }

    @Test
    public void likePatternLiteral_whenPatternContainsSingleQuote_escapesSingleQuote()
    {
        assertEquals("'O''REILLY'", HiveUtils.likePatternLiteral("o'reilly"));
    }

    @Test
    public void likePatternLiteral_whenPatternIsTableName_returnsUppercasedSecrets()
    {
        assertEquals("'SECRETS'", HiveUtils.likePatternLiteral("secrets"));
    }

    @Test
    public void likePatternLiteral_whenPatternContainsSqlSuffix_staysInsideSingleQuotedLiteral()
    {
        String literal = HiveUtils.likePatternLiteral("test'; DROP TABLE x;--");
        assertEquals("'TEST''; DROP TABLE X;--'", literal);
        assertTrue(literal.startsWith("'"));
        assertTrue(literal.endsWith("'"));
        String showExtendedSql = "show table extended in `DEMO` like " + literal;
        assertEquals("show table extended in `DEMO` like 'TEST''; DROP TABLE X;--'", showExtendedSql);
        assertFalse(showExtendedSql.contains("like 'TEST';"));
    }

    @Test
    public void likePatternLiteral_whenPatternIsQuoteOnlyPayload_returnsEscapedQuotedLiteral()
    {
        assertEquals("'''; SELECT 1;--'", HiveUtils.likePatternLiteral("'; SELECT 1;--"));
    }

    @Test
    public void likePatternLiteral_whenPatternContainsBackslash_escapesBackslashBeforeQuoting()
    {
        String literal = HiveUtils.likePatternLiteral("test\\'; DROP TABLE x;--");
        assertEquals("'TEST\\\\''; DROP TABLE X;--'", literal);
        String showExtendedSql = "show table extended in `DEMO` like " + literal;
        assertFalse(showExtendedSql.contains("like 'TEST';"));
    }

    @Test
    public void likePatternLiteral_whenPatternContainsJavaEscapedQuote_hasSameOutputAsPlainQuote()
    {
        assertEquals("'TEST''; DROP TABLE X;--'", HiveUtils.likePatternLiteral("test\'; DROP TABLE x;--"));
    }

    @Test
    public void likePatternLiteral_whenPatternContainsBackslashOnly_doublesBackslashes()
    {
        assertEquals("'A\\\\B'", HiveUtils.likePatternLiteral("a\\b"));
    }

    @Test
    public void likePatternLiteral_whenPatternContainsHiveCStyleEscapes_doublesBackslashes() {
        assertEquals("'QUOTE\\\\''ESCAPE'", HiveUtils.likePatternLiteral("quote\\'escape"));
        assertEquals("'SLASH\\\\\\\\ESCAPE'", HiveUtils.likePatternLiteral("slash\\\\escape"));
        assertEquals("'NEWLINE\\\\NESCAPE'", HiveUtils.likePatternLiteral("newline\\nescape"));
        assertEquals("'TAB\\\\TESCAPE'", HiveUtils.likePatternLiteral("tab\\tescape"));
    }

   
    @Test(expected = NullPointerException.class)
    public void likePatternLiteral_whenPatternIsNull_throwsNullPointerException()
    {
        HiveUtils.likePatternLiteral(null);
    }
}
