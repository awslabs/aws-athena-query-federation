/*-
 * #%L
 * athena-bigquery
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.OperatorType;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

/**
 * Based on com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser class
 */
public class BigQueryFederationExpressionParser extends FederationExpressionParser
{
    private static final String quoteCharacter = "'";
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryFederationExpressionParser.class);

    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        return Joiner.on(", ").join(arguments);
    }

    public List<String> parseComplexExpressions(List<Field> columns, Constraints constraints)
    {
        if (constraints.getExpression() == null || constraints.getExpression().isEmpty()) {
            return ImmutableList.of();
        }

        List<FederationExpression> federationExpressions = constraints.getExpression();
        return federationExpressions.stream()
                .map(federationExpression -> parseFunctionCallExpression((FunctionCallExpression) federationExpression))
                .collect(Collectors.toList());
    }
    /**
     * This is a recursive function, as function calls can have arguments which, themselves, are function calls.
     * @param functionCallExpression
     * @return
     */
    public String parseFunctionCallExpression(FunctionCallExpression functionCallExpression)
    {
        FunctionName functionName = functionCallExpression.getFunctionName();
        List<FederationExpression> functionArguments = functionCallExpression.getArguments();

        List<String> arguments = functionArguments.stream()
                .map(argument -> {
                    // base cases
                    if (argument instanceof ConstantExpression) {
                        return parseConstantExpression((ConstantExpression) argument);
                    }
                    else if (argument instanceof VariableExpression) {
                        return parseVariableExpression((VariableExpression) argument);
                    }
                    // recursive case
                    else if (argument instanceof FunctionCallExpression) {
                        return parseFunctionCallExpression((FunctionCallExpression) argument);
                    }
                    throw new RuntimeException("Should not reach this case - a new subclass was introduced and is not handled.");
                }).collect(Collectors.toList());

        return mapFunctionToDataSourceSyntax(functionName, functionCallExpression.getType(), arguments);
    }

    public String parseConstantExpression(ConstantExpression constantExpression)
    {
        Block values = constantExpression.getValues();
        FieldReader fieldReader = values.getFieldReader(DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME);

        List<String> constants = new ArrayList<>();

        for (int i = 0; i < values.getRowCount(); i++) {
            fieldReader.setPosition(i);
            String strVal = BlockUtils.fieldToString(fieldReader);
            constants.add(strVal);
        }
        if (constantExpression.getType().equals(ArrowType.Utf8.INSTANCE)
                || constantExpression.getType().equals(ArrowType.LargeUtf8.INSTANCE)
                || fieldReader.getMinorType().equals(Types.MinorType.DATEDAY)) {
            constants = constants.stream()
                    .map(val -> quoteCharacter + val + quoteCharacter)
                    .collect(Collectors.toList());
        }

        return Joiner.on(",").join(constants);
    }

    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        StandardFunctions functionEnum = StandardFunctions.fromFunctionName(functionName);
        OperatorType operatorType = functionEnum.getOperatorType();

        if (arguments == null || arguments.size() == 0) {
            throw new IllegalArgumentException("Arguments cannot be null or empty.");
        }
        switch (operatorType) {
            case UNARY:
                if (arguments.size() != 1) {
                    throw new IllegalArgumentException("Unary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case BINARY:
                if (arguments.size() != 2) {
                    throw new IllegalArgumentException("Binary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case VARARG:
                break;
            default:
                throw new RuntimeException("A new operator type was introduced without adding support for it.");
        }

        String clause = "";
        switch (functionEnum) {
            case ADD_FUNCTION_NAME:
                clause = Joiner.on(" + ").join(arguments);
                break;
            case AND_FUNCTION_NAME:
                clause = Joiner.on(" AND ").join(arguments);
                break;
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME: // up to subclass
                clause = writeArrayConstructorClause(type, arguments);
                break;
            case DIVIDE_FUNCTION_NAME:
                clause = Joiner.on(" / ").join(arguments);
                break;
            case EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" = ").join(arguments);
                break;
            case GREATER_THAN_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" > ").join(arguments);
                break;
            case GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" >= ").join(arguments);
                break;
            case IN_PREDICATE_FUNCTION_NAME:
                clause = arguments.get(0) + " IN " + arguments.get(1);
                break;
            case IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME:
                String argZero = arguments.get(0);
                String argOne = arguments.get(1);
                clause = argZero + " IS DISTINCT FROM " + argOne;
                break;
            case IS_NULL_FUNCTION_NAME:
                clause = arguments.get(0) + " IS NULL";
                break;
            case LESS_THAN_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" < ").join(arguments);
                break;
            case LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" <= ").join(arguments);
                break;
            case LIKE_PATTERN_FUNCTION_NAME:
                clause = arguments.get(0) + " LIKE " + arguments.get(1);
                break;
            case MODULUS_FUNCTION_NAME:
                clause = " MOD(" + arguments.get(0) + "," + arguments.get(1) + ")";
                break;
            case MULTIPLY_FUNCTION_NAME:
                clause = Joiner.on(" * ").join(arguments);
                break;
            case NEGATE_FUNCTION_NAME:
                clause = "-" + arguments.get(0);
                break;
            case NOT_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" <> ").join(arguments);
                break;
            case NOT_FUNCTION_NAME:
                clause = " NOT " + arguments.get(0);
                break;
            case NULLIF_FUNCTION_NAME:
                clause = "NULLIF(" + arguments.get(0) + ", " + arguments.get(1) + ")";
                break;
            case OR_FUNCTION_NAME:
                clause = Joiner.on(" OR ").join(arguments);
                break;
            case SUBTRACT_FUNCTION_NAME:
                clause = Joiner.on(" - ").join(arguments);
                break;
            default:
                throw new NotImplementedException("The function " + functionName.getFunctionName() + " does not have an implementation");
        }
        if (clause == null) {
            return "";
        }
        return "(" + clause + ")";
    }
}
