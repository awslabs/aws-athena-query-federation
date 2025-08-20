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

import io.substrait.expression.ExpressionCreator;
import io.substrait.proto.Expression;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class SubstraitLiteralConverterTest
{
    @Test
    public void testExtractStringLiteral()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setString("test")
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals("test", result.getLeft());
        assertTrue(result.getRight() instanceof ArrowType.Utf8);
    }

    @Test
    public void testExtractIntegerLiterals()
    {
        Expression expr32 = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setI32(42)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr32);
        
        assertEquals(42, result.getLeft());
        ArrowType.Int intType = (ArrowType.Int) result.getRight();
        assertEquals(32, intType.getBitWidth());
    }

    @Test
    public void testExtractBooleanLiteral()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setBoolean(true)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals(true, result.getLeft());
        assertTrue(result.getRight() instanceof ArrowType.Bool);
    }

    @Test
    public void testExtractFloatingPointLiterals()
    {
        Expression expr64 = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setFp64(3.14)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr64);
        
        assertEquals(3.14, (Double) result.getLeft(), 0.001);
        ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) result.getRight();
        assertEquals(FloatingPointPrecision.DOUBLE, fpType.getPrecision());
    }

    @Test
    public void testCreateStringLiteral()
    {
        ArrowType arrowType = new ArrowType.Utf8();
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression("hello", arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateIntegerLiteral()
    {
        ArrowType arrowType = new ArrowType.Int(32, true);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(123, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateBooleanLiteral()
    {
        ArrowType arrowType = new ArrowType.Bool();
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(false, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateDecimalLiteral()
    {
        ArrowType arrowType = new ArrowType.Decimal(10, 2, 128);
        BigDecimal value = new BigDecimal("123.45");
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(value, arrowType);
        
        assertNotNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractNonLiteralExpression()
    {
        Expression expr = Expression.newBuilder().build();
        SubstraitLiteralConverter.extractLiteralValue(expr);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUnsupportedArrowType()
    {
        ArrowType unsupportedType = new ArrowType.List();
        SubstraitLiteralConverter.createLiteralExpression("test", unsupportedType);
    }
}