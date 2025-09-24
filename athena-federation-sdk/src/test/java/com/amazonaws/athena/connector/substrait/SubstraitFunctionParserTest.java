/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.substrait;

import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SubstraitFunctionParserTest
{
    private static final List<String> COLUMN_NAMES = Arrays.asList("id", "name", "age", "active");

    @Test
    void testGetColumnPredicatesMapWithSinglePredicate()
    {
        SimpleExtensionDeclaration extension = createExtensionDeclaration(1, "equal:any_any");
        List<SimpleExtensionDeclaration> extensions = Arrays.asList(extension);
        Expression expression = createBinaryExpression(1, 0, 123);
        
        Map<String, List<ColumnPredicate>> result = SubstraitFunctionParser.getColumnPredicatesMap(extensions, expression, COLUMN_NAMES);
        
        assertEquals(1, result.size());
        assertTrue(result.containsKey("id"));
        assertEquals(1, result.get("id").size());
        
        ColumnPredicate predicate = result.get("id").get(0);
        assertEquals("id", predicate.getColumn());
        assertEquals(SubstraitOperator.EQUAL, predicate.getOperator());
        assertEquals(123, predicate.getValue());
    }

    @Test
    void testGetColumnPredicatesMapWithMultiplePredicates()
    {
        SimpleExtensionDeclaration equalExt = createExtensionDeclaration(1, "equal:any_any");
        SimpleExtensionDeclaration gtExt = createExtensionDeclaration(2, "gt:any_any");
        SimpleExtensionDeclaration andExt = createExtensionDeclaration(3, "and:bool");
        List<SimpleExtensionDeclaration> extensions = Arrays.asList(equalExt, gtExt, andExt);
        
        Expression idEquals = createBinaryExpression(1, 0, 123);
        Expression ageGreater = createBinaryExpression(2, 2, 18);
        Expression andExpression = createLogicalExpression(3, idEquals, ageGreater);
        
        Map<String, List<ColumnPredicate>> result = SubstraitFunctionParser.getColumnPredicatesMap(extensions, andExpression, COLUMN_NAMES);
        
        assertEquals(2, result.size());
        assertTrue(result.containsKey("id"));
        assertTrue(result.containsKey("age"));
        
        ColumnPredicate idPredicate = result.get("id").get(0);
        assertEquals(SubstraitOperator.EQUAL, idPredicate.getOperator());
        assertEquals(123, idPredicate.getValue());
        
        ColumnPredicate agePredicate = result.get("age").get(0);
        assertEquals(SubstraitOperator.GREATER_THAN, agePredicate.getOperator());
        assertEquals(18, agePredicate.getValue());
    }

    @Test
    void testParseColumnPredicatesWithUnaryOperator()
    {
        SimpleExtensionDeclaration extension = createExtensionDeclaration(1, "is_null:any");
        List<SimpleExtensionDeclaration> extensions = Arrays.asList(extension);
        Expression expression = createUnaryExpression(1, 1);
        
        List<ColumnPredicate> result = SubstraitFunctionParser.parseColumnPredicates(extensions, expression, COLUMN_NAMES);
        
        assertEquals(1, result.size());
        ColumnPredicate predicate = result.get(0);
        assertEquals("name", predicate.getColumn());
        assertEquals(SubstraitOperator.IS_NULL, predicate.getOperator());
        assertNull(predicate.getValue());
    }

    @Test
    void testParseColumnPredicatesWithStringLiteral()
    {
        SimpleExtensionDeclaration extension = createExtensionDeclaration(1, "equal:any_any");
        List<SimpleExtensionDeclaration> extensions = Arrays.asList(extension);
        Expression expression = createBinaryExpressionWithString(1, 1, "John");
        
        List<ColumnPredicate> result = SubstraitFunctionParser.parseColumnPredicates(extensions, expression, COLUMN_NAMES);
        
        assertEquals(1, result.size());
        ColumnPredicate predicate = result.get(0);
        assertEquals("name", predicate.getColumn());
        assertEquals(SubstraitOperator.EQUAL, predicate.getOperator());
        assertEquals("John", predicate.getValue());
        assertInstanceOf(ArrowType.Utf8.class, predicate.getArrowType());
    }

    @Test
    void testParseColumnPredicatesWithBooleanLiteral()
    {
        SimpleExtensionDeclaration extension = createExtensionDeclaration(1, "equal:any_any");
        List<SimpleExtensionDeclaration> extensions = Arrays.asList(extension);
        Expression expression = createBinaryExpressionWithBoolean(1, 3, true);
        
        List<ColumnPredicate> result = SubstraitFunctionParser.parseColumnPredicates(extensions, expression, COLUMN_NAMES);
        
        assertEquals(1, result.size());
        ColumnPredicate predicate = result.get(0);
        assertEquals("active", predicate.getColumn());
        assertEquals(SubstraitOperator.EQUAL, predicate.getOperator());
        assertEquals(true, predicate.getValue());
        assertInstanceOf(ArrowType.Bool.class, predicate.getArrowType());
    }

    @Test
    void testParseColumnPredicatesWithUnsupportedOperator()
    {
        SimpleExtensionDeclaration extension = createExtensionDeclaration(1, "unsupported:function");
        List<SimpleExtensionDeclaration> extensions = Arrays.asList(extension);
        Expression expression = createBinaryExpression(1, 0, 123);
        
        assertThrows(UnsupportedOperationException.class, () -> 
            SubstraitFunctionParser.parseColumnPredicates(extensions, expression, COLUMN_NAMES));
    }

    @Test
    void testParseColumnPredicatesWithEmptyExpression()
    {
        List<SimpleExtensionDeclaration> extensions = Arrays.asList();
        Expression expression = Expression.newBuilder().build();
        
        List<ColumnPredicate> result = SubstraitFunctionParser.parseColumnPredicates(extensions, expression, COLUMN_NAMES);
        
        assertTrue(result.isEmpty());
    }

    private SimpleExtensionDeclaration createExtensionDeclaration(int anchor, String functionName)
    {
        return SimpleExtensionDeclaration.newBuilder()
                .setExtensionFunction(SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                        .setFunctionAnchor(anchor)
                        .setName(functionName)
                        .build())
                .build();
    }

    private Expression createBinaryExpression(int functionRef, int fieldIndex, int value)
    {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(functionRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(createFieldReference(fieldIndex))
                                .build())
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(createIntLiteral(value))
                                .build())
                        .build())
                .build();
    }

    private Expression createBinaryExpressionWithString(int functionRef, int fieldIndex, String value)
    {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(functionRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(createFieldReference(fieldIndex))
                                .build())
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(createStringLiteral(value))
                                .build())
                        .build())
                .build();
    }

    private Expression createBinaryExpressionWithBoolean(int functionRef, int fieldIndex, boolean value)
    {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(functionRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(createFieldReference(fieldIndex))
                                .build())
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(createBooleanLiteral(value))
                                .build())
                        .build())
                .build();
    }

    private Expression createUnaryExpression(int functionRef, int fieldIndex)
    {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(functionRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(createFieldReference(fieldIndex))
                                .build())
                        .build())
                .build();
    }

    private Expression createLogicalExpression(int functionRef, Expression left, Expression right)
    {
        return Expression.newBuilder()
                .setScalarFunction(Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(functionRef)
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(left)
                                .build())
                        .addArguments(FunctionArgument.newBuilder()
                                .setValue(right)
                                .build())
                        .build())
                .build();
    }

    private Expression createFieldReference(int fieldIndex)
    {
        return Expression.newBuilder()
                .setSelection(Expression.FieldReference.newBuilder()
                        .setDirectReference(Expression.ReferenceSegment.newBuilder()
                                .setStructField(Expression.ReferenceSegment.StructField.newBuilder()
                                        .setField(fieldIndex)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    private Expression createIntLiteral(int value)
    {
        return Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setI32(value)
                        .build())
                .build();
    }

    private Expression createStringLiteral(String value)
    {
        return Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setString(value)
                        .build())
                .build();
    }

    private Expression createBooleanLiteral(boolean value)
    {
        return Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setBoolean(value)
                        .build())
                .build();
    }
}