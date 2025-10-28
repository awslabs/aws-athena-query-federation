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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FederationExpressionParserTest {

    private static final String TEST_COLUMN_NAME = "column";
    private static final String TEST_VALUE = "val";
    private static final String TEST_AGE_COLUMN = "age";
    private static final int TEST_AGE_VALUE = 25;
    private static final String TEST_COL_NAME = "col";
    private static final String TEST_COL1_NAME = "col1";
    private static final String TEST_COL2_NAME = "col2";
    private static final String TEST_INT_COL_NAME = "int_col";
    private static final String TEST_STRING_COL_NAME = "string_col";
    private static final String TEST_BOOL_COL_NAME = "bool_col";
    private static final int TEST_ROW_COUNT = 2;
    private static final int TEST_SINGLE_ROW_COUNT = 1;
    private static final int TEST_CONSTANT_VALUE_42 = 42;
    private static final String TEST_FUNCTION_NAME = "ADD";
    private static final String TEST_EQUALS_FUNCTION = "EQUALS";
    private static final String TEST_IS_NULL_FUNCTION = "IS_NULL";
    private static final String TEST_NOT_NULL_FUNCTION = "NOT_NULL";
    private static final String TEST_SUBTRACT_FUNCTION = "SUBTRACT";
    private static final String TEST_MULTIPLY_FUNCTION = "MULTIPLY";
    private static final String TEST_GREATER_THAN_FUNCTION = "GREATER_THAN";
    private static final String TEST_CASE_WHEN_FUNCTION = "CASE_WHEN";
    private static final String TEST_SUM_FUNCTION = "SUM";
    private static final String TEST_LIKE_FUNCTION = "LIKE";
    private static final String TEST_CONCAT_FUNCTION = "CONCAT";
    private static final String TEST_COALESCE_FUNCTION = "COALESCE";
    private static final String TEST_COL1_LITERAL = "col1";
    private static final String TEST_COL2_LITERAL = "col2";
    private static final String TEST_AGE_LITERAL = "age";
    private static final String TEST_SALARY_LITERAL = "salary";
    private static final String TEST_BONUS_LITERAL = "bonus";
    private static final String TEST_NAME_LITERAL = "name";
    private static final String TEST_PATTERN_LITERAL = "pattern";
    private static final String TEST_SUFFIX_LITERAL = "_suffix";
    private static final String TEST_PLACEHOLDER_SINGLE = "?";
    private static final String TEST_PLACEHOLDER_DOUBLE = "?,?";
    private static final int TEST_AGE_THRESHOLD = 18;
    private static final int TEST_DEFAULT_VALUE = 0;

    private FederationExpressionParser parser;

    @Before
    public void setup() {
        parser = new FederationExpressionParser() {
            @Override
            public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments) {
                return functionName.getFunctionName() + "(" + String.join(",", arguments) + ")";
            }
        };
    }

    @Test
    public void testParseVariableExpression() {
        VariableExpression variableExpression = new VariableExpression(TEST_COLUMN_NAME, new ArrowType.Int(32, true));
        assertEquals(TEST_COLUMN_NAME, parser.parseVariableExpression(variableExpression));
    }

    @Test
    public void testParseConstantExpression() {
        Block mockBlock = createMockBlock(TEST_ROW_COUNT, TEST_VALUE);
        ConstantExpression constantExpression = createConstantExpression(mockBlock, new ArrowType.Utf8());

        List<TypeAndValue> acc = new ArrayList<>();
        String result = parser.parseConstantExpression(constantExpression, acc);

        assertEquals(TEST_PLACEHOLDER_DOUBLE, result);
        assertEquals(TEST_ROW_COUNT, acc.size());
        assertEquals(TEST_VALUE, acc.get(0).getValue());
    }

    @Test
    public void testParseFunctionCallExpressionWithAllTypes() {
        VariableExpression variableExpr = new VariableExpression(TEST_AGE_COLUMN, new ArrowType.Int(32, true));

        Block mockBlock = createMockBlock(TEST_SINGLE_ROW_COUNT, TEST_AGE_VALUE);
        ConstantExpression constantExpr = createConstantExpression(mockBlock, new ArrowType.Int(32, true));

        FunctionCallExpression nestedFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true),
                new FunctionName(TEST_FUNCTION_NAME),
                List.of(variableExpr, constantExpr)
        );

        List<TypeAndValue> acc = new ArrayList<>();
        String result = parser.parseFunctionCallExpression(nestedFunc, acc);

        assertTrue(result.startsWith(TEST_FUNCTION_NAME + "("));
        assertEquals(TEST_SINGLE_ROW_COUNT, acc.size());
        assertEquals(TEST_AGE_VALUE, acc.get(0).getValue());
    }

    @Test(expected = AthenaConnectorException.class)
    public void testParseFunctionCallExpressionWithUnknownArgument() {
        FederationExpression unknown = mock(FederationExpression.class);
        FunctionCallExpression functionCall = new FunctionCallExpression(
                new ArrowType.Bool(),
                new FunctionName(TEST_EQUALS_FUNCTION),
                List.of(unknown)
        );

        parser.parseFunctionCallExpression(functionCall, new ArrayList<>());
    }

    @Test
    public void testParseComplexExpressionsWithEmptyConstraints() {
        Constraints emptyConstraints = createEmptyConstraints();
        List<String> result = parser.parseComplexExpressions(List.of(), emptyConstraints, new ArrayList<>());
        assertTrue(result.isEmpty());

        Constraints constraintsWithEmptyExpressions = createConstraintsWithExpressions(Collections.emptyList());
        result = parser.parseComplexExpressions(List.of(), constraintsWithEmptyExpressions, new ArrayList<>());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseComplexExpressionsSingleFunction() {
        VariableExpression variableExpr = new VariableExpression(TEST_COL_NAME, new ArrowType.Int(32, true));

        FunctionCallExpression funcExpr = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName(TEST_IS_NULL_FUNCTION), List.of(variableExpr));

        Constraints constraints = createConstraintsWithExpressions(List.of(funcExpr));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), constraints, acc);

        assertEquals(1, expressions.size());
        assertEquals(TEST_IS_NULL_FUNCTION + "(" + TEST_COL_NAME + ")", expressions.get(0));
    }

    @Test
    public void testParseComplexExpressionsMultipleFunctions() {
        VariableExpression variableExpr1 = new VariableExpression(TEST_COL1_NAME, new ArrowType.Int(32, true));
        VariableExpression variableExpr2 = new VariableExpression(TEST_COL2_NAME, new ArrowType.Utf8());

        FunctionCallExpression funcExpr1 = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName(TEST_IS_NULL_FUNCTION), List.of(variableExpr1));

        FunctionCallExpression funcExpr2 = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName(TEST_NOT_NULL_FUNCTION), List.of(variableExpr2));

        Constraints constraints = createConstraintsWithExpressions(List.of(funcExpr1, funcExpr2));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), constraints, acc);

        assertEquals(2, expressions.size());
        assertEquals(TEST_IS_NULL_FUNCTION + "(" + TEST_COL1_NAME + ")", expressions.get(0));
        assertEquals(TEST_NOT_NULL_FUNCTION + "(" + TEST_COL2_NAME + ")", expressions.get(1));
    }

    @Test
    public void testParseConstantExpressionSingleValue() {
        Block mockBlock = createMockBlock(TEST_SINGLE_ROW_COUNT, TEST_CONSTANT_VALUE_42);
        ConstantExpression constantExpression = createConstantExpression(mockBlock, new ArrowType.Int(32, true));

        List<TypeAndValue> acc = new ArrayList<>();
        String result = parser.parseConstantExpression(constantExpression, acc);

        assertEquals(TEST_PLACEHOLDER_SINGLE, result);
        assertEquals(TEST_SINGLE_ROW_COUNT, acc.size());
        assertEquals(TEST_CONSTANT_VALUE_42, acc.get(0).getValue());
    }

    @Test
    public void testParseVariableExpressionDifferentTypes() {
        // Test with different arrow types
        VariableExpression intExpr = new VariableExpression(TEST_INT_COL_NAME, new ArrowType.Int(32, true));
        VariableExpression stringExpr = new VariableExpression(TEST_STRING_COL_NAME, new ArrowType.Utf8());
        VariableExpression boolExpr = new VariableExpression(TEST_BOOL_COL_NAME, new ArrowType.Bool());

        assertEquals(TEST_INT_COL_NAME, parser.parseVariableExpression(intExpr));
        assertEquals(TEST_STRING_COL_NAME, parser.parseVariableExpression(stringExpr));
        assertEquals(TEST_BOOL_COL_NAME, parser.parseVariableExpression(boolExpr));
    }

    @Test
    public void testParseComplexExpressionsWithNestedFunctions() {
        // Test complex nested function expressions
        VariableExpression varExpr1 = new VariableExpression(TEST_COL1_LITERAL, new ArrowType.Int(32, true));
        VariableExpression varExpr2 = new VariableExpression(TEST_COL2_LITERAL, new ArrowType.Int(32, true));

        // Create nested function: ADD(SUBTRACT(col1, col2), MULTIPLY(col1, col2))
        FunctionCallExpression subtractFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true), new FunctionName(TEST_SUBTRACT_FUNCTION), List.of(varExpr1, varExpr2));
        
        FunctionCallExpression multiplyFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true), new FunctionName(TEST_MULTIPLY_FUNCTION), List.of(varExpr1, varExpr2));
        
        FunctionCallExpression addFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true), new FunctionName(TEST_FUNCTION_NAME), List.of(subtractFunc, multiplyFunc));

        Constraints constraints = createConstraintsWithExpressions(List.of(addFunc));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), constraints, acc);

        assertEquals(1, expressions.size());
        assertTrue(expressions.get(0).contains(TEST_FUNCTION_NAME));
        assertTrue(expressions.get(0).contains(TEST_SUBTRACT_FUNCTION));
        assertTrue(expressions.get(0).contains(TEST_MULTIPLY_FUNCTION));
    }

    @Test
    public void testParseComplexExpressionsWithCaseWhen() {
        // Test CASE WHEN expressions
        VariableExpression conditionVar = new VariableExpression(TEST_AGE_LITERAL, new ArrowType.Int(32, true));
        VariableExpression thenVar = new VariableExpression(TEST_SALARY_LITERAL, new ArrowType.Int(32, true));
        VariableExpression elseVar = new VariableExpression(TEST_BONUS_LITERAL, new ArrowType.Int(32, true));

        // Create CASE WHEN expression: CASE WHEN age > 18 THEN salary ELSE bonus END
        FunctionCallExpression greaterThanFunc = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName(TEST_GREATER_THAN_FUNCTION), List.of(conditionVar, new ConstantExpression(createMockBlock(1, TEST_AGE_THRESHOLD), new ArrowType.Int(32, true))));
        
        FunctionCallExpression caseFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true), new FunctionName(TEST_CASE_WHEN_FUNCTION), List.of(greaterThanFunc, thenVar, elseVar));

        Constraints constraints = createConstraintsWithExpressions(List.of(caseFunc));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), constraints, acc);

        assertEquals(1, expressions.size());
        assertTrue(expressions.get(0).contains(TEST_CASE_WHEN_FUNCTION));
        assertTrue(expressions.get(0).contains(TEST_GREATER_THAN_FUNCTION));
    }

    @Test
    public void testParseComplexExpressionsWithAggregateFunctions() {
        // Test aggregate functions in complex expressions
        VariableExpression varExpr = new VariableExpression(TEST_COL1_LITERAL, new ArrowType.Int(32, true));
        
        // Create aggregate function: SUM(col1 + col1)
        FunctionCallExpression addFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true), new FunctionName(TEST_FUNCTION_NAME), List.of(varExpr, varExpr));
        
        FunctionCallExpression sumFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true), new FunctionName(TEST_SUM_FUNCTION), List.of(addFunc));

        Constraints constraints = createConstraintsWithExpressions(List.of(sumFunc));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), constraints, acc);

        assertEquals(1, expressions.size());
        assertTrue(expressions.get(0).contains(TEST_SUM_FUNCTION));
        assertTrue(expressions.get(0).contains(TEST_FUNCTION_NAME));
    }

    @Test
    public void testParseComplexExpressionsWithStringFunctions() {
        // Test string manipulation functions
        VariableExpression stringVar = new VariableExpression(TEST_NAME_LITERAL, new ArrowType.Utf8());
        VariableExpression patternVar = new VariableExpression(TEST_PATTERN_LITERAL, new ArrowType.Utf8());
        
        // Create LIKE function: name LIKE pattern
        FunctionCallExpression likeFunc = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName(TEST_LIKE_FUNCTION), List.of(stringVar, patternVar));
        
        // Create CONCAT function: CONCAT(name, '_suffix')
        FunctionCallExpression concatFunc = new FunctionCallExpression(
                new ArrowType.Utf8(), new FunctionName(TEST_CONCAT_FUNCTION), List.of(stringVar, new ConstantExpression(createMockBlock(1, TEST_SUFFIX_LITERAL), new ArrowType.Utf8())));

        Constraints constraints = createConstraintsWithExpressions(List.of(likeFunc, concatFunc));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), constraints, acc);

        assertEquals(2, expressions.size());
        assertTrue(expressions.get(0).contains(TEST_LIKE_FUNCTION));
        assertTrue(expressions.get(1).contains(TEST_CONCAT_FUNCTION));
    }

    @Test
    public void testParseComplexExpressionsWithNullHandling() {
        // Test NULL handling functions
        VariableExpression varExpr1 = new VariableExpression(TEST_COL1_LITERAL, new ArrowType.Int(32, true));
        VariableExpression varExpr2 = new VariableExpression(TEST_COL2_LITERAL, new ArrowType.Int(32, true));
        
        // Create COALESCE function: COALESCE(col1, col2, 0)
        FunctionCallExpression coalesceFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true), new FunctionName(TEST_COALESCE_FUNCTION), 
                List.of(varExpr1, varExpr2, new ConstantExpression(createMockBlock(1, TEST_DEFAULT_VALUE), new ArrowType.Int(32, true))));
        
        // Create IS_NULL function: IS_NULL(col1)
        FunctionCallExpression isNullFunc = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName(TEST_IS_NULL_FUNCTION), List.of(varExpr1));

        Constraints constraints = createConstraintsWithExpressions(List.of(coalesceFunc, isNullFunc));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), constraints, acc);

        assertEquals(2, expressions.size());
        assertTrue(expressions.get(0).contains(TEST_COALESCE_FUNCTION));
        assertTrue(expressions.get(1).contains(TEST_IS_NULL_FUNCTION));
    }

    private Constraints createEmptyConstraints() {
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }


    private Constraints createConstraintsWithExpressions(List<FederationExpression> expressions) {
        return new Constraints(Collections.emptyMap(), expressions, Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }

    private Block createMockBlock(int rowCount, Object returnValue) {
        FieldReader mockReader = mock(FieldReader.class);
        when(mockReader.readObject()).thenReturn(returnValue);

        Block mockBlock = mock(Block.class);
        when(mockBlock.getRowCount()).thenReturn(rowCount);
        when(mockBlock.getFieldReader(anyString())).thenReturn(mockReader);

        return mockBlock;
    }

    private ConstantExpression createConstantExpression(Block block, ArrowType arrowType) {
        return new ConstantExpression(block, arrowType);
    }
}
