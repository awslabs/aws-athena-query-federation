package com.amazonaws.athena.connector.lambda.domain.predicate.functions;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
//Based on https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/expression/StandardFunctions.java
public enum StandardFunctions
{
/**
     * $and is a vararg function accepting boolean arguments
     */
    AND_FUNCTION_NAME(new FunctionName("$and"), OperatorType.VARARG),

    /**
     * $or is a vararg function accepting boolean arguments
     */
    OR_FUNCTION_NAME(new FunctionName("$or"), OperatorType.VARARG),

    /**
     * $not is a function accepting boolean argument
     */
    NOT_FUNCTION_NAME(new FunctionName("$not"), OperatorType.UNARY),

    IS_NULL_FUNCTION_NAME(new FunctionName("$is_null"), OperatorType.UNARY),
    /**
     * $nullif is a function accepting two arguments. Returns null if both values are the same, otherwise returns the first value.
     */
    NULLIF_FUNCTION_NAME(new FunctionName("$nullif"), OperatorType.BINARY),

    /**
     * $cast function result type is determined by the {@link Call#getType()}
     * Cast is nuanced and very connector specific. We omit it for initial release.
     */
    // CAST_FUNCTION_NAME(new FunctionName("$cast"), OperatorType.UNARY),

    EQUAL_OPERATOR_FUNCTION_NAME(new FunctionName("$equal"), OperatorType.BINARY),
    NOT_EQUAL_OPERATOR_FUNCTION_NAME(new FunctionName("$not_equal"), OperatorType.BINARY),
    LESS_THAN_OPERATOR_FUNCTION_NAME(new FunctionName("$less_than"), OperatorType.BINARY),
    LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME(new FunctionName("$less_than_or_equal"), OperatorType.BINARY),
    GREATER_THAN_OPERATOR_FUNCTION_NAME(new FunctionName("$greater_than"), OperatorType.BINARY),
    GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME(new FunctionName("$greater_than_or_equal"), OperatorType.BINARY),
    IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME(new FunctionName("$is_distinct_from"), OperatorType.BINARY),

    /**
     * Arithmetic addition.
     */
    ADD_FUNCTION_NAME(new FunctionName("$add"), OperatorType.BINARY),

    /**
     * Arithmetic subtraction.
     */
    SUBTRACT_FUNCTION_NAME(new FunctionName("$subtract"), OperatorType.BINARY),

    /**
     * Arithmetic multiplication.
     */
    MULTIPLY_FUNCTION_NAME(new FunctionName("$multiply"), OperatorType.BINARY),

    /**
     * Arithmetic division.
     */
    DIVIDE_FUNCTION_NAME(new FunctionName("$divide"), OperatorType.BINARY),

    /**
     * Arithmetic modulus.
     */
    MODULUS_FUNCTION_NAME(new FunctionName("$modulus"), OperatorType.BINARY),

    /**
     * Arithmetic unary minus.
     */
    NEGATE_FUNCTION_NAME(new FunctionName("$negate"), OperatorType.UNARY),

    LIKE_PATTERN_FUNCTION_NAME(new FunctionName("$like_pattern"), OperatorType.BINARY),

    /**
     * {@code $in(value, array)} returns {@code true} when value is equal to an element of the array,
     * otherwise returns {@code NULL} when comparing value to an element of the array returns an
     * indeterminate result, otherwise returns {@code false}
     */
    IN_PREDICATE_FUNCTION_NAME(new FunctionName("$in"), OperatorType.BINARY),

    /**
     * $array creates instance of {@link Array Type}
     */
    ARRAY_CONSTRUCTOR_FUNCTION_NAME(new FunctionName("$array"));

    final FunctionName functionName;
    final OperatorType operatorType;

    private StandardFunctions(FunctionName functionName)
    {
        this.functionName = functionName;
        this.operatorType = OperatorType.VARARG;
    }

    private StandardFunctions(FunctionName functionName, OperatorType operatorType)
    {
        this.functionName = functionName;
        this.operatorType = operatorType;
    }

    public FunctionName getFunctionName()
    {
        return this.functionName;
    }
    
    public OperatorType getOperatorType()
    {
        return this.operatorType;
    }

    public static StandardFunctions fromFunctionName(FunctionName functionName)
    {
        for (StandardFunctions function : StandardFunctions.values()) {
            if (function.getFunctionName().equals(functionName)) {
                return function;
            }
        }
        return null;
    }
}
