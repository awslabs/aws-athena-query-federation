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
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.*;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FederationExpressionParserTest {

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
        VariableExpression variableExpression = new VariableExpression("column", new ArrowType.Int(32, true));
        assertEquals("column", parser.parseVariableExpression(variableExpression));
    }

    @Test
    public void testParseConstantExpression() {
        FieldReader mockReader = mock(FieldReader.class);
        when(mockReader.readObject()).thenReturn("val");

        Block mockBlock = mock(Block.class);
        when(mockBlock.getRowCount()).thenReturn(2);
        when(mockBlock.getFieldReader(anyString())).thenReturn(mockReader);

        ConstantExpression constantExpression = new ConstantExpression(mockBlock, new ArrowType.Utf8());

        List<TypeAndValue> acc = new ArrayList<>();
        String result = parser.parseConstantExpression(constantExpression, acc);

        assertEquals("?,?", result);
        assertEquals(2, acc.size());
        assertEquals("val", acc.get(0).getValue());
    }

    @Test
    public void testParseFunctionCallExpressionWithAllTypes() {
        VariableExpression variableExpr = new VariableExpression("age", new ArrowType.Int(32, true));

        Block mockBlock = mock(Block.class);
        FieldReader mockReader = mock(FieldReader.class);
        when(mockReader.readObject()).thenReturn(25);
        when(mockBlock.getRowCount()).thenReturn(1);
        when(mockBlock.getFieldReader(anyString())).thenReturn(mockReader);

        ConstantExpression constantExpr = new ConstantExpression(mockBlock, new ArrowType.Int(32, true));

        FunctionCallExpression nestedFunc = new FunctionCallExpression(
                new ArrowType.Int(32, true),
                new FunctionName("ADD"),
                List.of(variableExpr, constantExpr)
        );

        List<TypeAndValue> acc = new ArrayList<>();
        String result = parser.parseFunctionCallExpression(nestedFunc, acc);

        assertTrue(result.startsWith("ADD("));
        assertEquals(1, acc.size());
        assertEquals(25, acc.get(0).getValue());
    }

    @Test(expected = AthenaConnectorException.class)
    public void testUnknownExpressionThrows() {
        FederationExpression unknown = mock(FederationExpression.class);
        FunctionCallExpression functionCall = new FunctionCallExpression(
                new ArrowType.Bool(),
                new FunctionName("EQUALS"),
                List.of(unknown)
        );

        parser.parseFunctionCallExpression(functionCall, new ArrayList<>());
    }

    @Test
    public void testParseComplexExpressionsNullOrEmpty() {
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
        VariableExpression variableExpr = new VariableExpression("col", new ArrowType.Int(32, true));

        FunctionCallExpression funcExpr = new FunctionCallExpression(
                new ArrowType.Bool(), new FunctionName("IS_NULL"), List.of(variableExpr));

        Constraints mockConstraints = mock(Constraints.class);
        when(mockConstraints.getExpression()).thenReturn(List.of(funcExpr));

        List<TypeAndValue> acc = new ArrayList<>();
        List<String> expressions = parser.parseComplexExpressions(List.of(), mockConstraints, acc);

        assertEquals(1, expressions.size());
        assertEquals("IS_NULL(col)", expressions.get(0));
    }
}
