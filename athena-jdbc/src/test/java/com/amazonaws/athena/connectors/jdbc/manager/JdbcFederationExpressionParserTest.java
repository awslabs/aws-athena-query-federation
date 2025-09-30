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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class JdbcFederationExpressionParserTest
{
    private static final Class<AthenaConnectorException> ATHENA_EXCEPTION = AthenaConnectorException.class;
    private static final String TEST_COLUMN = "my_col";
    private static final String TEST_COLUMN_QUOTED = "\"my_col\"";
    private static final String TEST_ARG_A = "a";
    private static final String TEST_ARG_B = "b";
    private static final String TEST_ARG_X = "x";
    private static final String TEST_ARG_Y = "y";
    private static final String TEST_QUOTE_CHAR = "\"";
    private static final String TEST_ADD_FUNCTION = "$add";
    private static final String TEST_AND_FUNCTION = "$and";
    private static final String TEST_IN_FUNCTION = "$in";
    private static final String TEST_IS_NULL_FUNCTION = "$is_null";
    private static final String TEST_EQUAL_FUNCTION = "$equal";
    private static final String TEST_NEGATE_FUNCTION = "$negate";
    private static final String TEST_NULLIF_FUNCTION = "$nullif";
    private static final String TEST_ARRAY_FUNCTION = "$array";
    private static final String TEST_NOT_EQUAL_FUNCTION = "$not_equal";
    private static final String TEST_GREATER_THAN_FUNCTION = "$greater_than";
    private static final String TEST_GREATER_THAN_OR_EQUAL_FUNCTION = "$greater_than_or_equal";
    private static final String TEST_LESS_THAN_FUNCTION = "$less_than";
    private static final String TEST_LESS_THAN_OR_EQUAL_FUNCTION = "$less_than_or_equal";
    private static final String TEST_NOT_FUNCTION = "$not";
    private static final String TEST_OR_FUNCTION = "$or";
    private static final String TEST_MODULUS_FUNCTION = "$modulus";
    private static final String TEST_MULTIPLY_FUNCTION = "$multiply";
    private static final String TEST_DIVIDE_FUNCTION = "$divide";
    private static final String TEST_SUBTRACT_FUNCTION = "$subtract";
    private static final String TEST_LIKE_FUNCTION = "$like_pattern";
    private static final String TEST_AGE_LITERAL = "age";
    private static final String TEST_NAME_LITERAL = "name";
    private static final String TEST_EMAIL_LITERAL = "email";
    private static final String TEST_COMPANY_EMAIL_PATTERN = "%@company.com";
    private static final String TEST_J_PATTERN = "J%";
    private static final int TEST_AGE_THRESHOLD = 18;

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

    private final JdbcFederationExpressionParser parser = new TestJdbcFederationExpressionParser(TEST_QUOTE_CHAR);

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
        VariableExpression expr = new VariableExpression(TEST_COLUMN, new ArrowType.Int(32, true));
        String result = parser.parseVariableExpression(expr);
        assertEquals(TEST_COLUMN_QUOTED, result);
    }

    @Test
    public void testAddFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_ADD_FUNCTION), new ArrowType.Int(32, true), List.of(TEST_ARG_A, TEST_ARG_B));
        assertEquals("(" + TEST_ARG_A + " + " + TEST_ARG_B + ")", result);
    }

    @Test
    public void testAndFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_AND_FUNCTION), null, List.of(TEST_ARG_A, TEST_ARG_B));
        assertEquals("(" + TEST_ARG_A + " AND " + TEST_ARG_B + ")", result);
    }

    @Test
    public void testInPredicate()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_IN_FUNCTION), null, List.of(TEST_ARG_A, "(1, 2, 3)"));
        assertEquals("(" + TEST_ARG_A + " IN (1, 2, 3))", result);
    }

    @Test
    public void testUnaryFunction_Valid()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_IS_NULL_FUNCTION), null, List.of(TEST_ARG_X));
        assertEquals("(" + TEST_ARG_X + " IS NULL)", result);
    }

    @Test
    public void testBinaryFunction_Valid()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_EQUAL_FUNCTION), null, List.of(TEST_ARG_X, TEST_ARG_Y));
        assertEquals("(" + TEST_ARG_X + " = " + TEST_ARG_Y + ")", result);
    }

    @Test
    public void testNegateFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_NEGATE_FUNCTION), null, List.of("5"));
        assertEquals("(-5)", result);
    }

    @Test
    public void testNullIfFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_NULLIF_FUNCTION), null, List.of(TEST_ARG_A, TEST_ARG_B));
        assertEquals("(NULLIF(" + TEST_ARG_A + ", " + TEST_ARG_B + "))", result);
    }

    @Test
    public void testWriteArrayConstructorFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_ARRAY_FUNCTION), new ArrowType.Int(32, true), List.of("1", "2", "3"));
        assertEquals("(ARRAY[1,2,3])", result);
    }

    @Test
    public void testEmptyArgumentsThrowsException()
    {
        assertAthenaConnectorException(TEST_ADD_FUNCTION, List.of(), "Arguments cannot be null or empty.");
    }

    @Test
    public void testUnaryFunctionInvalidArgCount()
    {
        assertAthenaConnectorException(TEST_IS_NULL_FUNCTION, List.of(TEST_ARG_A, TEST_ARG_B), "Unary function type " + TEST_IS_NULL_FUNCTION + " was provided with 2 arguments.");
    }

    @Test
    public void testBinaryFunctionInvalidArgCount()
    {
        assertAthenaConnectorException(TEST_ADD_FUNCTION, List.of(TEST_ARG_A), "Binary function type " + TEST_ADD_FUNCTION + " was provided with 1 arguments.");
    }

    @Test
    public void testNotEqualFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_NOT_EQUAL_FUNCTION), null, List.of(TEST_ARG_X, TEST_ARG_Y));
        assertEquals("(" + TEST_ARG_X + " <> " + TEST_ARG_Y + ")", result);
    }

    @Test
    public void testGreaterThanFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_GREATER_THAN_FUNCTION), null, List.of(TEST_ARG_X, TEST_ARG_Y));
        assertEquals("(" + TEST_ARG_X + " > " + TEST_ARG_Y + ")", result);
    }

    @Test
    public void testGreaterThanOrEqualFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_GREATER_THAN_OR_EQUAL_FUNCTION), null, List.of(TEST_ARG_X, TEST_ARG_Y));
        assertEquals("(" + TEST_ARG_X + " >= " + TEST_ARG_Y + ")", result);
    }

    @Test
    public void testLessThanFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_LESS_THAN_FUNCTION), null, List.of(TEST_ARG_X, TEST_ARG_Y));
        assertEquals("(" + TEST_ARG_X + " < " + TEST_ARG_Y + ")", result);
    }

    @Test
    public void testLessThanOrEqualFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_LESS_THAN_OR_EQUAL_FUNCTION), null, List.of(TEST_ARG_X, TEST_ARG_Y));
        assertEquals("(" + TEST_ARG_X + " <= " + TEST_ARG_Y + ")", result);
    }

    @Test
    public void testNotFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_NOT_FUNCTION), null, List.of(TEST_ARG_X));
        assertEquals("( NOT " + TEST_ARG_X + ")", result);
    }

    @Test
    public void testOrFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_OR_FUNCTION), null, List.of(TEST_ARG_X, TEST_ARG_Y));
        assertEquals("(" + TEST_ARG_X + " OR " + TEST_ARG_Y + ")", result);
    }

    @Test
    public void testModFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_MODULUS_FUNCTION), new ArrowType.Int(32, true), List.of("10", "3"));
        assertEquals("(10 % 3)", result);
    }

    @Test
    public void testMultiplyFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_MULTIPLY_FUNCTION), new ArrowType.Int(32, true), List.of("2", "3"));
        assertEquals("(2 * 3)", result);
    }

    @Test
    public void testDivideFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_DIVIDE_FUNCTION), new ArrowType.Int(32, true), List.of("6", "2"));
        assertEquals("(6 / 2)", result);
    }

    @Test
    public void testSubtractFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_SUBTRACT_FUNCTION), new ArrowType.Int(32, true), List.of("7", "4"));
        assertEquals("(7 - 4)", result);
    }

    @Test
    public void testLikeFunction()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_LIKE_FUNCTION), null, List.of(TEST_NAME_LITERAL, "'" + TEST_J_PATTERN + "'"));
        assertEquals("(" + TEST_NAME_LITERAL + " LIKE '" + TEST_J_PATTERN + "')", result);
    }


    @Test
    public void testNullArguments()
    {
        assertAthenaConnectorException(TEST_ADD_FUNCTION, null, "Arguments cannot be null or empty.");
    }

    @Test
    public void testTernaryFunctionInvalidArgCount()
    {
        assertAthenaConnectorException(TEST_NULLIF_FUNCTION, List.of(TEST_ARG_A, TEST_ARG_B, TEST_ARG_X),
            "Binary function type " + TEST_NULLIF_FUNCTION + " was provided with 3 arguments.");
    }

    @Test
    public void testComplexNestedFunctions()
    {
        String result = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_ARRAY_FUNCTION),
            new ArrowType.Utf8(), List.of("'hello'", "'world'", "'test'"));
        assertEquals("(ARRAY['hello','world','test'])", result);
    }

    @Test
    public void testArithmeticFunctionsWithDifferentTypes()
    {
        // Test arithmetic functions with different arrow types
        String floatResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_ADD_FUNCTION), 
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), List.of("3.14", "2.86"));
        assertEquals("(3.14 + 2.86)", floatResult);

        String decimalResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_MULTIPLY_FUNCTION), 
            new ArrowType.Decimal(10, 2, 128), List.of("100.50", "2"));
        assertEquals("(100.50 * 2)", decimalResult);
    }

    @Test
    public void testLogicalFunctionsWithMultipleArguments()
    {
        String andResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_AND_FUNCTION),
            null, List.of("condition1", "condition2"));
        assertEquals("(condition1 AND condition2)", andResult);

        String orResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_OR_FUNCTION), 
            null, List.of("condition1", "condition2"));
        assertEquals("(condition1 OR condition2)", orResult);
    }

    @Test
    public void testComparisonFunctionsEdgeCases()
    {
        // Test all comparison functions
        String gtResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_GREATER_THAN_FUNCTION), 
            null, List.of("age", "18"));
        assertEquals("(age > 18)", gtResult);

        String gteResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_GREATER_THAN_OR_EQUAL_FUNCTION), 
            null, List.of("score", "90"));
        assertEquals("(score >= 90)", gteResult);

        String ltResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_LESS_THAN_FUNCTION), 
            null, List.of("count", "100"));
        assertEquals("(count < 100)", ltResult);

        String lteResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_LESS_THAN_OR_EQUAL_FUNCTION), 
            null, List.of("limit", "50"));
        assertEquals("(limit <= 50)", lteResult);
    }

    @Test
    public void testUnaryFunctionsEdgeCases()
    {
        // Test NOT function
        String notResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_NOT_FUNCTION), 
            null, List.of("active"));
        assertEquals("( NOT active)", notResult);

        // Test NEGATE function with different numeric types
        String negateFloatResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_NEGATE_FUNCTION), 
            null, List.of("3.14"));
        assertEquals("(-3.14)", negateFloatResult);
    }

    @Test
    public void testComplexNestedArithmeticExpressions()
    {
        // Test deeply nested arithmetic expressions
        String complexResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_ADD_FUNCTION), 
            new ArrowType.Int(32, true), List.of("((a + b) * c)", "((d - e) / f)"));
        assertEquals("(((a + b) * c) + ((d - e) / f))", complexResult);
    }

    @Test
    public void testComplexLogicalExpressions()
    {
        // Test complex logical expressions with multiple conditions
        String andResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_AND_FUNCTION),
            null, List.of("(age > 18)", "(salary > 50000)", "(department = 'IT')"));
        assertEquals("((age > 18) AND (salary > 50000) AND (department = 'IT'))", andResult);

        String orResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_OR_FUNCTION),
            null, List.of("(status = 'active')", "(status = 'pending')", "(status = 'approved')"));
        assertEquals("((status = 'active') OR (status = 'pending') OR (status = 'approved'))", orResult);
    }

    @Test
    public void testComplexStringExpressions()
    {
        // Test complex string manipulation expressions using existing functions
        String likeResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_LIKE_FUNCTION),
            null, List.of(TEST_EMAIL_LITERAL, "'" + TEST_COMPANY_EMAIL_PATTERN + "'"));
        assertEquals("(" + TEST_EMAIL_LITERAL + " LIKE '" + TEST_COMPANY_EMAIL_PATTERN + "')", likeResult);

        // Test complex logical expressions with string conditions
        String andResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_AND_FUNCTION),
            null, List.of("(" + TEST_NAME_LITERAL + " LIKE '" + TEST_J_PATTERN + "')", "(" + TEST_AGE_LITERAL + " > " + TEST_AGE_THRESHOLD + ")"));
        assertEquals("((" + TEST_NAME_LITERAL + " LIKE '" + TEST_J_PATTERN + "') AND (" + TEST_AGE_LITERAL + " > " + TEST_AGE_THRESHOLD + "))", andResult);
    }

    @Test
    public void testComplexDateExpressions()
    {
        // Test complex date/time expressions using existing arithmetic functions
        String addResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_ADD_FUNCTION),
            new ArrowType.Int(32, true), List.of("order_date", "30"));
        assertEquals("(order_date + 30)", addResult);

        String subtractResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_SUBTRACT_FUNCTION),
            new ArrowType.Int(32, true), List.of("end_date", "start_date"));
        assertEquals("(end_date - start_date)", subtractResult);
    }

    @Test
    public void testComplexAggregateExpressions()
    {
        // Test complex aggregate expressions using existing arithmetic functions
        String multiplyResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_MULTIPLY_FUNCTION),
            new ArrowType.Int(32, true), List.of("price", "quantity"));
        assertEquals("(price * quantity)", multiplyResult);

        String divideResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_DIVIDE_FUNCTION),
            new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), 
            List.of("score", "total_questions"));
        assertEquals("(score / total_questions)", divideResult);
    }

    @Test
    public void testComplexCaseExpressions()
    {
        // Test complex CASE expressions using existing comparison functions
        String greaterThanResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_GREATER_THAN_OR_EQUAL_FUNCTION),
            null, List.of("age", "65"));
        assertEquals("(age >= 65)", greaterThanResult);

        String lessThanResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_LESS_THAN_OR_EQUAL_FUNCTION),
            null, List.of("age", "18"));
        assertEquals("(age <= 18)", lessThanResult);
    }

    @Test
    public void testComplexNullHandlingExpressions()
    {
        // Test complex NULL handling expressions using existing functions
        String nullIfResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_NULLIF_FUNCTION),
            new ArrowType.Int(32, true), List.of("(salary * 1.1)", "0"));
        assertEquals("(NULLIF((salary * 1.1), 0))", nullIfResult);

        // Test complex arithmetic with NULL handling
        String multiplyResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_MULTIPLY_FUNCTION),
            new ArrowType.Int(32, true), List.of("salary", "1.1"));
        assertEquals("(salary * 1.1)", multiplyResult);
    }

    @Test
    public void testComplexWindowFunctionExpressions()
    {
        // Test complex window function expressions using existing arithmetic functions
        String addResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_ADD_FUNCTION),
            new ArrowType.Int(32, true), List.of("salary", "bonus"));
        assertEquals("(salary + bonus)", addResult);

        // Test complex logical expressions for window functions
        String andResult = parser.mapFunctionToDataSourceSyntax(new FunctionName(TEST_AND_FUNCTION),
            null, List.of("(department = 'IT')", "(salary > 50000)"));
        assertEquals("((department = 'IT') AND (salary > 50000))", andResult);
    }
}
