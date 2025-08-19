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
    private static final int TEST_ROW_COUNT = 2;
    private static final int TEST_SINGLE_ROW_COUNT = 1;
    private static final String TEST_FUNCTION_NAME = "ADD";
    private static final String TEST_EQUALS_FUNCTION = "EQUALS";
    private static final String TEST_IS_NULL_FUNCTION = "IS_NULL";

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
        FieldReader mockReader = mock(FieldReader.class);
        when(mockReader.readObject()).thenReturn(TEST_VALUE);

        Block mockBlock = mock(Block.class);
        when(mockBlock.getRowCount()).thenReturn(TEST_ROW_COUNT);
        when(mockBlock.getFieldReader(anyString())).thenReturn(mockReader);

        ConstantExpression constantExpression = new ConstantExpression(mockBlock, new ArrowType.Utf8());

        List<TypeAndValue> acc = new ArrayList<>();
        String result = parser.parseConstantExpression(constantExpression, acc);

        assertEquals("?,?", result);
        assertEquals(TEST_ROW_COUNT, acc.size());
        assertEquals(TEST_VALUE, acc.get(0).getValue());
    }

    @Test
    public void testParseFunctionCallExpressionWithAllTypes() {
        VariableExpression variableExpr = new VariableExpression(TEST_AGE_COLUMN, new ArrowType.Int(32, true));

        Block mockBlock = mock(Block.class);
        FieldReader mockReader = mock(FieldReader.class);
        when(mockReader.readObject()).thenReturn(TEST_AGE_VALUE);
        when(mockBlock.getRowCount()).thenReturn(TEST_SINGLE_ROW_COUNT);
        when(mockBlock.getFieldReader(anyString())).thenReturn(mockReader);

        ConstantExpression constantExpr = new ConstantExpression(mockBlock, new ArrowType.Int(32, true));

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
        Constraints mockConstraints = mock(Constraints.class);
        when(mockConstraints.getExpression()).thenReturn(null);
        List<String> result = parser.parseComplexExpressions(List.of(), mockConstraints, new ArrayList<>());
        assertTrue(result.isEmpty());

        when(mockConstraints.getExpression()).thenReturn(Collections.emptyList());
        result = parser.parseComplexExpressions(List.of(), mockConstraints, new ArrayList<>());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseComplexExpressionsSingleFunction() {
        VariableExpression variableExpr = new VariableExpression(TEST_COL_NAME, new ArrowType.Int(32, true));

        FunctionCallExpression funcExpr = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName(TEST_IS_NULL_FUNCTION), List.of(variableExpr));

        Constraints mockConstraints = mock(Constraints.class);
        when(mockConstraints.getExpression()).thenReturn(List.of(funcExpr));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), mockConstraints, acc);

        assertEquals(1, expressions.size());
        assertEquals(TEST_IS_NULL_FUNCTION + "(" + TEST_COL_NAME + ")", expressions.get(0));
    }
}
