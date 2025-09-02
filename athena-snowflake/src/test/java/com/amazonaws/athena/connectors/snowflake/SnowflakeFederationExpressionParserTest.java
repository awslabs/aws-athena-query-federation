package com.amazonaws.athena.connectors.snowflake;

/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeFederationExpressionParserTest
{
    private static final String QUOTE_CHAR_DOUBLE = "\"";
    private static final String QUOTE_CHAR_BACKTICK = "`";
    private static final String TEST_ARG_1 = "arg1";
    private static final String TEST_ARG_2 = "arg2";
    private static final String TEST_ARG_3 = "arg3";
    private static final String TEST_ARG_4 = "arg4";
    private static final String TEST_ARG_5 = "arg5";
    private static final String TEST_ARG_6 = "arg6";
    private static final String TEST_ARG_7 = "arg7";
    private static final String TEST_ARG_8 = "arg8";
    private static final String TEST_ARG_9 = "arg9";
    private static final String TEST_ARG_10 = "arg10";
    private static final String TEST_ARG_WITH_COMMA = "arg2, with comma";
    private static final String TEST_ARG_SPECIAL = "test_arg";
    private static final String EXPECTED_SINGLE_ARG = "test_arg";
    private static final String EXPECTED_TWO_ARGS = "arg1, arg2";
    private static final String EXPECTED_THREE_ARGS = "arg1, arg2, arg3";
    private static final String EXPECTED_TEN_ARGS = "arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10";
    private static final String EXPECTED_MIXED_ARGS = "string, 123, true, null";
    private static final String EXPECTED_SPECIAL_CHARS = "arg1, arg2, with comma, arg3";
    private static final String EXPECTED_NUMBERS = "1, 2, 3";
    private static final String EXPECTED_EMPTY_STRINGS = ", arg2, ";
    private static final String EXPECTED_WHITESPACE = " arg1 ,  arg2 ,  arg3 ";

    private SnowflakeFederationExpressionParser parser;

    @Before
    public void setUp()
    {
        parser = new SnowflakeFederationExpressionParser(QUOTE_CHAR_DOUBLE);
    }

    @Test
    public void testConstructorWithDifferentQuoteChar()
    {
        SnowflakeFederationExpressionParser parser = new SnowflakeFederationExpressionParser(QUOTE_CHAR_BACKTICK);
        assertNotNull(parser);
    }

    @Test
    public void testWriteArrayConstructorClauseWithSingleArgument()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Collections.singletonList(TEST_ARG_SPECIAL);
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_SINGLE_ARG, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithMultipleArguments()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Arrays.asList(TEST_ARG_1, TEST_ARG_2, TEST_ARG_3);
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_THREE_ARGS, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithEmptyList()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Collections.emptyList();
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals("", result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithNullArguments()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Arrays.asList(TEST_ARG_1, TEST_ARG_2); // Don't include null values
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_TWO_ARGS, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithDifferentArrowTypes()
    {
        // Test with different Arrow types
        ArrowType[] types = {
            new ArrowType.Int(32, true),
            new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
            ArrowType.Bool.INSTANCE,
            new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY),
            new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)
        };
        
        List<String> arguments = Arrays.asList(TEST_ARG_1, TEST_ARG_2);
        
        for (ArrowType type : types) {
            String result = parser.writeArrayConstructorClause(type, arguments);
            assertEquals(EXPECTED_TWO_ARGS, result);
        }
    }

    @Test
    public void testWriteArrayConstructorClauseWithSpecialCharacters()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Arrays.asList(TEST_ARG_1, TEST_ARG_WITH_COMMA, TEST_ARG_3);
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_SPECIAL_CHARS, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithNumbers()
    {
        ArrowType type = new ArrowType.Int(32, true);
        List<String> arguments = Arrays.asList("1", "2", "3");
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_NUMBERS, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithMixedTypes()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Arrays.asList("string", "123", "true", "null");
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_MIXED_ARGS, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithLargeList()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Arrays.asList(
            TEST_ARG_1, TEST_ARG_2, TEST_ARG_3, TEST_ARG_4, TEST_ARG_5, 
            TEST_ARG_6, TEST_ARG_7, TEST_ARG_8, TEST_ARG_9, TEST_ARG_10
        );
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_TEN_ARGS, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithEmptyStrings()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Arrays.asList("", TEST_ARG_2, "");
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_EMPTY_STRINGS, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithWhitespace()
    {
        ArrowType type = ArrowType.Utf8.INSTANCE;
        List<String> arguments = Arrays.asList(" arg1 ", " arg2 ", " arg3 ");
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals(EXPECTED_WHITESPACE, result);
    }
}
