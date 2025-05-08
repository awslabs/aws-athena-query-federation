/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class JdbcFederationExpressionParserTest
{
    private static final Class<AthenaConnectorException> ATHENA_EXCEPTION = AthenaConnectorException.class;

    private static class TestJdbcFederationExpressionParser extends JdbcFederationExpressionParser
    {
        public TestJdbcFederationExpressionParser(String quoteChar)
        {
            super(quoteChar);
        }

        @Override
        public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
        {
            return "ARRAY[" + String.join(",", arguments) + "]";
        }
    }

    private final JdbcFederationExpressionParser parser = new TestJdbcFederationExpressionParser("\"");

    private void assertAthenaConnectorException(String functionName, List<String> arguments, String expectedMessage)
    {
        AthenaConnectorException ex = assertThrows(
                ATHENA_EXCEPTION,
                () -> parser.mapFunctionToDataSourceSyntax(new FunctionName(functionName), null, arguments)
        );
        assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testParseVariableExpression()
    {
        VariableExpression expr = new VariableExpression("my_col", new ArrowType.Int(32, true));
        String result = parser.parseVariableExpression(expr);
        assertEquals("\"my_col\"", result);
    }

    @Test
    public void testAddFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$add"), new ArrowType.Int(32, true), List.of("a", "b"));
        assertEquals("(a + b)", result);
    }

    @Test
    public void testAndFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$and"), null, List.of("a", "b"));
        assertEquals("(a AND b)", result);
    }

    @Test
    public void testInPredicate()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$in"), null, List.of("a", "(1, 2, 3)"));
        assertEquals("(a IN (1, 2, 3))", result);
    }

    @Test
    public void testUnaryFunction_Valid()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$is_null"), null, List.of("x"));
        assertEquals("(x IS NULL)", result);
    }

    @Test
    public void testBinaryFunction_Valid()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$equal"), null, List.of("x", "y"));
        assertEquals("(x = y)", result);
    }

    @Test
    public void testNegateFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$negate"), null, List.of("5"));
        assertEquals("(-5)", result);
    }

    @Test
    public void testNullIfFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$nullif"), null, List.of("a", "b"));
        assertEquals("(NULLIF(a, b))", result);
    }

    @Test
    public void testWriteArrayConstructorFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$array"), new ArrowType.Int(32, true), List.of("1", "2", "3"));
        assertEquals("(ARRAY[1,2,3])", result);
    }

    @Test
    public void testEmptyArgumentsThrowsException()
    {
        assertAthenaConnectorException("$add", List.of(), "Arguments cannot be null or empty.");
    }

    @Test
    public void testUnaryFunctionInvalidArgCount()
    {
        assertAthenaConnectorException("$is_null", List.of("a", "b"), "Unary function type $is_null was provided with 2 arguments.");
    }

    @Test
    public void testBinaryFunctionInvalidArgCount()
    {
        assertAthenaConnectorException("$add", List.of("a"), "Binary function type $add was provided with 1 arguments.");
    }

    @Test
    public void testNotEqualFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$not_equal"), null, List.of("x", "y"));
        assertEquals("(x <> y)", result);
    }

    @Test
    public void testGreaterThanFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$greater_than"), null, List.of("x", "y"));
        assertEquals("(x > y)", result);
    }

    @Test
    public void testGreaterThanOrEqualFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$greater_than_or_equal"), null, List.of("x", "y"));
        assertEquals("(x >= y)", result);
    }

    @Test
    public void testLessThanFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$less_than"), null, List.of("x", "y"));
        assertEquals("(x < y)", result);
    }

    @Test
    public void testLessThanOrEqualFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$less_than_or_equal"), null, List.of("x", "y"));
        assertEquals("(x <= y)", result);
    }

    @Test
    public void testNotFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$not"), null, List.of("x"));
        assertEquals("( NOT x)", result);
    }

    @Test
    public void testOrFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$or"), null, List.of("x", "y"));
        assertEquals("(x OR y)", result);
    }

    @Test
    public void testModFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$modulus"), new ArrowType.Int(32, true), List.of("10", "3"));
        assertEquals("(10 % 3)", result);
    }

    @Test
    public void testMultiplyFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$multiply"), new ArrowType.Int(32, true), List.of("2", "3"));
        assertEquals("(2 * 3)", result);
    }

    @Test
    public void testDivideFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$divide"), new ArrowType.Int(32, true), List.of("6", "2"));
        assertEquals("(6 / 2)", result);
    }

    @Test
    public void testSubtractFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$subtract"), new ArrowType.Int(32, true), List.of("7", "4"));
        assertEquals("(7 - 4)", result);
    }

    @Test
    public void testLikeFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName("$like_pattern"), null, List.of("name", "'J%'"));
        assertEquals("(name LIKE 'J%')", result);
    }
}
