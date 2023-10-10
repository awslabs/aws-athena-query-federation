/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.OperatorType;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;

public abstract class JdbcFederationExpressionParser extends FederationExpressionParser
{
    private final String quoteChar;

    public JdbcFederationExpressionParser(String quoteChar)
    {
        this.quoteChar = quoteChar;
    }

    // we pull out the functions that are not going to be universally supported by jdbc into their own methods.
    // connectors which support them can extend this class and override them.
    public abstract String writeArrayConstructorClause(ArrowType type, List<String> arguments);

    /**
     * JDBC Requires wrapping column names in a specific quote char
     */
    @Override
    public String parseVariableExpression(VariableExpression variableExpression)
    {
        return quoteChar + variableExpression.getColumnName() + quoteChar;
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
            // case CAST_FUNCTION_NAME: // up to subclass
            //     clause = writeCastClause(type, arguments);
            //     break;
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
                clause = Joiner.on(" % ").join(arguments);
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
