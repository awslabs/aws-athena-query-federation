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
import io.substrait.util.DecimalUtil;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

/**
 * Utility class for converting between Substrait literal expressions and Java objects with Arrow types.
 */
public final class SubstraitLiteralConverter
{
    private SubstraitLiteralConverter()
    {
        // Utility class - prevent instantiation
    }

    /**
     * Extracts a literal value and its corresponding Arrow type from a Substrait literal expression.
     */
    public static Pair<Object, ArrowType> extractLiteralValue(Expression expr)
    {
        if (!expr.hasLiteral()) {
            throw new IllegalArgumentException("Expected literal value in expression");
        }

        Expression.Literal literal = expr.getLiteral();
        ArrowType arrowType;

        switch (literal.getLiteralTypeCase()) {
            case I8:
                arrowType = new ArrowType.Int(8, true);
                return Pair.of(literal.getI8(), arrowType);
            case I16:
                arrowType = new ArrowType.Int(16, true);
                return Pair.of(literal.getI16(), arrowType);
            case I32:
                arrowType = new ArrowType.Int(32, true);
                return Pair.of(literal.getI32(), arrowType);
            case I64:
                arrowType = new ArrowType.Int(64, true);
                return Pair.of(literal.getI64(), arrowType);
            case FP32:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                return Pair.of(literal.getFp32(), arrowType);
            case FP64:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                return Pair.of(literal.getFp64(), arrowType);
            case STRING:
                arrowType = new ArrowType.Utf8();
                return Pair.of(literal.getString(), arrowType);
            case BOOLEAN:
                arrowType = new ArrowType.Bool();
                return Pair.of(literal.getBoolean(), arrowType);
            case BINARY:
                arrowType = new ArrowType.Binary();
                return Pair.of(literal.getBinary().toByteArray(), arrowType);
            case DECIMAL:
                Expression.Literal.Decimal decimal = literal.getDecimal();
                arrowType = new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), 16);
                return Pair.of(DecimalUtil.getBigDecimalFromBytes(decimal.getValue().toByteArray(),
                        decimal.getScale(), 16), arrowType);
            case DATE:
                arrowType = new ArrowType.Date(DateUnit.DAY);
                return Pair.of((long) literal.getDate(), arrowType);
            case TIMESTAMP:
                if (literal.hasTimestampTz()) {
                    arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
                    return Pair.of(literal.getTimestampTz(), arrowType);
                }
                else {
                    arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
                    return Pair.of(literal.getTimestamp(), arrowType);
                }
            case VAR_CHAR:
                arrowType = new ArrowType.Utf8();
                return Pair.of(literal.getVarChar().getValue(), arrowType);
            case FIXED_CHAR:
                arrowType = new ArrowType.Utf8();
                return Pair.of(literal.getFixedChar(), arrowType);
            default:
                throw new UnsupportedOperationException("Unsupported literal type: " + literal.getLiteralTypeCase());
        }
    }

    /**
     * Creates a Substrait literal expression from a Java object and Arrow type.
     */
    public static io.substrait.expression.Expression createLiteralExpression(Object value, ArrowType arrowType)
    {
        switch (arrowType.getTypeID()) {
            case Utf8:
                return ExpressionCreator.string(true, (String) value);
            case Binary:
                return ExpressionCreator.binary(true, (byte[]) value);
            case Decimal:
                BigDecimal bigDecimal = (BigDecimal) value;
                return ExpressionCreator.decimal(true, bigDecimal, bigDecimal.precision(), bigDecimal.scale());
            case Date:
                return ExpressionCreator.date(true, ((Long) value).intValue());
            case Int:
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (intType.getBitWidth() == 8) {
                    return ExpressionCreator.i8(true, ((Integer) value).byteValue());
                }
                else if (intType.getBitWidth() == 16) {
                    return ExpressionCreator.i16(true, ((Integer) value).shortValue());
                }
                else if (intType.getBitWidth() == 32) {
                    return ExpressionCreator.i32(true, (Integer) value);
                }
                else if (intType.getBitWidth() == 64) {
                    return ExpressionCreator.i64(true, (Long) value);
                }
                else {
                    return ExpressionCreator.i32(true, (Integer) value);
                }
            case Bool:
                return ExpressionCreator.bool(true, (Boolean) value);
            case FloatingPoint:
                ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
                if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
                    return ExpressionCreator.fp64(true, (Double) value);
                }
                else {
                    return ExpressionCreator.fp32(true, (Float) value);
                }
            case Timestamp:
                if (arrowType instanceof ArrowType.Timestamp && ((ArrowType.Timestamp) arrowType).getTimezone() != null) {
                    ZonedDateTime zonedDateTime = (ZonedDateTime) value;
                    return ExpressionCreator.timestampTZ(true, zonedDateTime.getLong(ChronoField.MICRO_OF_SECOND));
                }
                else {
                    return ExpressionCreator.timestamp(true, (Long) value);
                }
            default:
                throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
        }
    }
}
