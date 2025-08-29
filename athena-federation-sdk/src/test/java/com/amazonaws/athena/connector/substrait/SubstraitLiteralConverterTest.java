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

import com.google.protobuf.ByteString;
import io.substrait.proto.Expression;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;

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
    public void testExtractFp32Literal()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setFp32(2.5f)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals(2.5f, (Float) result.getLeft(), 0.001);
        ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) result.getRight();
        assertEquals(FloatingPointPrecision.SINGLE, fpType.getPrecision());
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

    @Test
    public void testExtractBinaryLiteral()
    {
        byte[] data = {1, 2, 3, 4};
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setBinary(ByteString.copyFrom(data))
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertArrayEquals(data, (byte[]) result.getLeft());
        assertTrue(result.getRight() instanceof ArrowType.Binary);
    }

    @Test
    public void testExtractDateLiteral()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setDate(18628)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals(18628L, result.getLeft());
        ArrowType.Date dateType = (ArrowType.Date) result.getRight();
        assertEquals(DateUnit.DAY, dateType.getUnit());
    }

    @Test
    public void testExtractTimestampLiteral()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setTimestamp(1609459200000000L)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals(1609459200000000L, result.getLeft());
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) result.getRight();
        assertEquals(TimeUnit.MICROSECOND, timestampType.getUnit());
        assertNull(timestampType.getTimezone());
    }



    @Test
    public void testExtractVarCharLiteral()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setVarChar(Expression.Literal.VarChar.newBuilder()
                                .setValue("varchar_test")
                                .setLength(20)
                                .build())
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals("varchar_test", result.getLeft());
        assertTrue(result.getRight() instanceof ArrowType.Utf8);
    }

    @Test
    public void testExtractFixedCharLiteral()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setFixedChar("fixed")
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals("fixed", result.getLeft());
        assertTrue(result.getRight() instanceof ArrowType.Utf8);
    }

    @Test
    public void testExtractI8Literal()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setI8(8)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals(8, result.getLeft());
        assertEquals(8, ((ArrowType.Int) result.getRight()).getBitWidth());
    }

    @Test
    public void testExtractI16Literal()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setI16(16)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals(16, result.getLeft());
        assertEquals(16, ((ArrowType.Int) result.getRight()).getBitWidth());
    }

    @Test
    public void testExtractI64Literal()
    {
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setI64(64L)
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertEquals(64L, result.getLeft());
        assertEquals(64, ((ArrowType.Int) result.getRight()).getBitWidth());
    }

    @Test
    public void testExtractDecimalLiteral()
    {
        // Create a proper 16-byte array for decimal representation
        byte[] decimalBytes = new byte[16];
        decimalBytes[15] = 123; // Simple value in the least significant byte
        
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .setDecimal(Expression.Literal.Decimal.newBuilder()
                                .setValue(ByteString.copyFrom(decimalBytes))
                                .setPrecision(10)
                                .setScale(2)
                                .build())
                        .build())
                .build();
        
        Pair<Object, ArrowType> result = SubstraitLiteralConverter.extractLiteralValue(expr);
        
        assertTrue(result.getLeft() instanceof BigDecimal);
        ArrowType.Decimal decimalType = (ArrowType.Decimal) result.getRight();
        assertEquals(10, decimalType.getPrecision());
        assertEquals(2, decimalType.getScale());
    }

    @Test
    public void testCreateI8Literal()
    {
        ArrowType arrowType = new ArrowType.Int(8, true);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(8, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateI16Literal()
    {
        ArrowType arrowType = new ArrowType.Int(16, true);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(16, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateI64Literal()
    {
        ArrowType arrowType = new ArrowType.Int(64, true);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(64L, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateFp32Literal()
    {
        ArrowType arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(3.14f, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateFp64Literal()
    {
        ArrowType arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(3.14, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateBinaryLiteral()
    {
        ArrowType arrowType = new ArrowType.Binary();
        byte[] data = {1, 2, 3};
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(data, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateDateLiteral()
    {
        ArrowType arrowType = new ArrowType.Date(DateUnit.DAY);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(18628L, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateTimestampLiteral()
    {
        ArrowType arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(1609459200000000L, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateTimestampTzLiteral()
    {
        ArrowType arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        ZonedDateTime zonedDateTime = ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(zonedDateTime, arrowType);
        
        assertNotNull(result);
    }

    @Test
    public void testCreateIntegerDefaultBitWidth()
    {
        ArrowType arrowType = new ArrowType.Int(128, true); // Unsupported bit width
        io.substrait.expression.Expression result = SubstraitLiteralConverter.createLiteralExpression(42, arrowType);
        
        assertNotNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUnsupportedArrowType()
    {
        ArrowType arrowType = new ArrowType.List();
        SubstraitLiteralConverter.createLiteralExpression("test", arrowType);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExtractUnsupportedLiteralType()
    {
        // Create a literal with LITERALTYPE_NOT_SET to trigger the default case
        Expression expr = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder()
                        .build())
                .build();
        
        SubstraitLiteralConverter.extractLiteralValue(expr);
    }
}