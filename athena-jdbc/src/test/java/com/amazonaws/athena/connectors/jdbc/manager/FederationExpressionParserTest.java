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

        assertEquals("?,?", result);
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
    public void parseFunctionCallExpression_withUnknownArgument_throwsAthenaConnectorException() {
        FederationExpression unknown = mock(FederationExpression.class);
        FunctionCallExpression functionCall = new FunctionCallExpression(
                new ArrowType.Bool(),
                new FunctionName(TEST_EQUALS_FUNCTION),
                List.of(unknown)
        );

        parser.parseFunctionCallExpression(functionCall, new ArrayList<>());
    }

    @Test
    public void parseComplexExpressions_withNullOrEmptyConstraints_shouldReturnEmptyList() {
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

        assertEquals("?", result);
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
