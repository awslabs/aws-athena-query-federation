/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.OperatorType;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

/**
 * Based on com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser class
 */
public class SqlServerFederationExpressionParser extends JdbcFederationExpressionParser
{
    private static final String quoteCharacter = "'";

    public SqlServerFederationExpressionParser(String quoteChar)
    {
        super(quoteChar);
    }

    @Override
    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        return SqlServerSqlUtils.renderTemplate("comma_separated_list_with_parentheses", Map.of("items", arguments));
    }

    @Override
    public String parseConstantExpression(ConstantExpression constantExpression, 
                                          List<TypeAndValue> parameterValues)
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

        return SqlServerSqlUtils.renderTemplate("comma_separated_list", Map.of("items", constants));
    }

    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        StandardFunctions functionEnum = StandardFunctions.fromFunctionName(functionName);
        OperatorType operatorType = functionEnum.getOperatorType();

        if (arguments == null || arguments.isEmpty()) {
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
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " + "));
                break;
            case AND_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " AND "));
                break;
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME: // up to subclass
                clause = writeArrayConstructorClause(type, arguments);
                break;
            case DIVIDE_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " / "));
                break;
            case EQUAL_OPERATOR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " = "));
                break;
            case GREATER_THAN_OPERATOR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " > "));
                break;
            case GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " >= "));
                break;
            case IN_PREDICATE_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("in_expression", Map.of("column", arguments.get(0), "values", arguments.get(1)));
                break;
            case IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("is_distinct_from", Map.of("left", arguments.get(0), "right", arguments.get(1)));
                break;
            case IS_NULL_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("null_predicate", Map.of("columnName", arguments.get(0), "isNull", true));
                break;
            case LESS_THAN_OPERATOR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " < "));
                break;
            case LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " <= "));
                break;
            case LIKE_PATTERN_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("like_expression", Map.of("column", arguments.get(0), "pattern", arguments.get(1)));
                break;
            case MODULUS_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("function_call_2args", Map.of("functionName", "MOD", "arg1", arguments.get(0), "arg2", arguments.get(1)));
                break;
            case MULTIPLY_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " * "));
                break;
            case NEGATE_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("unary_operator", Map.of("operator", "-", "operand", arguments.get(0)));
                break;
            case NOT_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " <> "));
                break;
            case NOT_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("unary_operator", Map.of("operator", "NOT ", "operand", arguments.get(0)));
                break;
            case NULLIF_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("function_call_2args", Map.of("functionName", "NULLIF", "arg1", arguments.get(0), "arg2", arguments.get(1)));
                break;
            case OR_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " OR "));
                break;
            case SUBTRACT_FUNCTION_NAME:
                clause = SqlServerSqlUtils.renderTemplate("join_expression", Map.of("items", arguments, "separator", " - "));
                break;
            default:
                throw new NotImplementedException("The function " + functionName.getFunctionName() + " does not have an implementation");
        }
        if (clause == null) {
            return "";
        }
        return clause;
    }
}
