/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata;

import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TeradataFederationExpressionParserTest
{
    private static final String TEST_QUOTE_CHAR = "\"";
    private static final String TEST_ARG_A = "a";
    private static final String TEST_ARG_B = "b";
    private static final String TEST_ARG_C = "c";
    private static final String TEST_MODULUS_FUNCTION = "$modulus";
    private static final String TEST_ARRAY_FUNCTION = "$array";
    private static final String TEST_ADD_FUNCTION = "$add";
    private static final String TEST_IS_NULL_FUNCTION = "$is_null";
    private static final String TEST_AND_FUNCTION = "$and";

    private TeradataFederationExpressionParser parser;

    @Before
    public void setup()
    {
        parser = new TeradataFederationExpressionParser(TEST_QUOTE_CHAR);
    }

    @Test
    public void writeArrayConstructorClause_withMultipleArguments_returnsJoinedString()
    {
        ArrowType type = new ArrowType.Int(32, true);
        List<String> arguments = Arrays.asList("1", "2", "3");
        String result = parser.writeArrayConstructorClause(type, arguments);
        Assert.assertEquals("1, 2, 3", result);
    }

    @Test
    public void writeArrayConstructorClause_withSingleArgument_returnsSingleArgument()
    {
        ArrowType type = new ArrowType.Int(32, true);
        List<String> arguments = Collections.singletonList("1");
        String result = parser.writeArrayConstructorClause(type, arguments);
        Assert.assertEquals("1", result);
    }

    @Test
    public void writeArrayConstructorClause_withEmptyList_returnsEmptyString()
    {
        ArrowType type = new ArrowType.Int(32, true);
        List<String> arguments = Collections.emptyList();
        String result = parser.writeArrayConstructorClause(type, arguments);
        Assert.assertEquals("", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withModulusFunction_returnsModSyntax()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_MODULUS_FUNCTION),
                new ArrowType.Int(32, true),
                Arrays.asList(TEST_ARG_A, TEST_ARG_B));
        Assert.assertEquals("(a MOD b)", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withArrayConstructorFunction_returnsJoinedArguments()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_ARRAY_FUNCTION),
                new ArrowType.Int(32, true),
                Arrays.asList("1", "2", "3"));
        Assert.assertEquals("(1, 2, 3)", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withArrayConstructorFunctionAndSingleArgument_returnsSingleArgument()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_ARRAY_FUNCTION),
                new ArrowType.Int(32, true),
                Collections.singletonList("1"));
        Assert.assertEquals("(1)", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapFunctionToDataSourceSyntax_withArrayConstructorFunctionAndEmptyArguments_throwsException()
    {
        // Empty arguments should throw IllegalArgumentException
        parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_ARRAY_FUNCTION),
                new ArrowType.Int(32, true),
                Collections.emptyList());
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withDefaultFunction_callsSuperMethod()
    {
        // Test that default case calls super.mapFunctionToDataSourceSyntax
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_ADD_FUNCTION),
                new ArrowType.Int(32, true),
                Arrays.asList(TEST_ARG_A, TEST_ARG_B));
        // Parent class returns "(a + b)", then Teradata parser wraps it in another set of parentheses
        Assert.assertEquals("((a + b))", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapFunctionToDataSourceSyntax_withNullArguments_throwsException()
    {
        parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_ADD_FUNCTION),
                new ArrowType.Int(32, true),
                null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapFunctionToDataSourceSyntax_withEmptyArguments_throwsException()
    {
        parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_ADD_FUNCTION),
                new ArrowType.Int(32, true),
                Collections.emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapFunctionToDataSourceSyntax_withUnaryFunctionAndWrongArgumentCount_throwsException()
    {
        parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_IS_NULL_FUNCTION),
                null,
                Arrays.asList(TEST_ARG_A, TEST_ARG_B));
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withUnaryFunctionAndCorrectArgumentCount_returnsWrappedResult()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_IS_NULL_FUNCTION),
                null,
                Collections.singletonList(TEST_ARG_A));
        // Parent class returns "(a IS NULL)", then Teradata parser wraps it in another set of parentheses
        Assert.assertEquals("((a IS NULL))", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapFunctionToDataSourceSyntax_withBinaryFunctionAndWrongArgumentCount_throwsException()
    {
        parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_MODULUS_FUNCTION),
                new ArrowType.Int(32, true),
                Collections.singletonList(TEST_ARG_A));
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withBinaryFunctionAndCorrectArgumentCount_returnsModSyntax()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_MODULUS_FUNCTION),
                new ArrowType.Int(32, true),
                Arrays.asList(TEST_ARG_A, TEST_ARG_B));
        Assert.assertEquals("(a MOD b)", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withVarargFunctionAndMultipleArguments_returnsWrappedResult()
    {
        // AND function is VARARG, should accept any number of arguments
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_AND_FUNCTION),
                null,
                Arrays.asList(TEST_ARG_A, TEST_ARG_B, TEST_ARG_C));
        // Parent class returns "(a AND b AND c)", then Teradata parser wraps it in another set of parentheses
        Assert.assertEquals("((a AND b AND c))", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withVarargFunctionAndSingleArgument_returnsWrappedResult()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_AND_FUNCTION),
                null,
                Collections.singletonList(TEST_ARG_A));
        // Parent class returns "(a)", then Teradata parser wraps it in another set of parentheses
        Assert.assertEquals("((a))", result);
    }

    @Test
    public void mapFunctionToDataSourceSyntax_withVarargFunctionAndTwoArguments_returnsWrappedResult()
    {
        String result = parser.mapFunctionToDataSourceSyntax(
                new FunctionName(TEST_AND_FUNCTION),
                null,
                Arrays.asList(TEST_ARG_A, TEST_ARG_B));
        // Parent class returns "(a AND b)", then Teradata parser wraps it in another set of parentheses
        Assert.assertEquals("((a AND b))", result);
    }
}
