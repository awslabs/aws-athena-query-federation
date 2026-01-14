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
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.OperatorType;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

/**
 * Template-based implementation of JdbcFederationExpressionParser using StringTemplate.
 * Connectors migrated to StringTemplate should extend this class instead of JdbcFederationExpressionParser.
 * To migrate a connector:
 * 1. Extend this class instead of JdbcFederationExpressionParser
 * 2. Implement getQueryFactory() to return the connector's query factory
 * 3. Override methods only if connector-specific behavior is needed
 */
public abstract class TemplateBasedJdbcFederationExpressionParser extends JdbcFederationExpressionParser
{
    public TemplateBasedJdbcFederationExpressionParser(String quoteChar)
    {
        super(quoteChar);
    }

    /**
     * Subclasses must provide their query factory for template rendering.
     * @return The connector-specific JdbcQueryFactory instance
     */
    protected abstract JdbcQueryFactory getQueryFactory();

    @Override
    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        return JdbcSqlUtils.renderTemplate(getQueryFactory(), "comma_separated_list_with_parentheses", Map.of("items", arguments));
    }

    @Override
    public String parseConstantExpression(ConstantExpression constantExpression, List<TypeAndValue> accumulator)
    {
        Block values = constantExpression.getValues();
        FieldReader fieldReader = values.getFieldReader(DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME);

        List<String> placeholders = new ArrayList<>();
        for (int i = 0; i < values.getRowCount(); i++) {
            fieldReader.setPosition(i);
            accumulator.add(new TypeAndValue(constantExpression.getType(), fieldReader.readObject()));
            placeholders.add("?");
        }

        return JdbcSqlUtils.renderTemplate(getQueryFactory(), "comma_separated_list", Map.of("items", placeholders));
    }

    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        StandardFunctions functionEnum = StandardFunctions.fromFunctionName(functionName);
        OperatorType operatorType = functionEnum.getOperatorType();

        if (arguments == null || arguments.isEmpty()) {
            throw new AthenaConnectorException("Arguments cannot be null or empty.",
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        switch (operatorType) {
            case UNARY:
                if (arguments.size() != 1) {
                    throw new AthenaConnectorException("Unary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.",
                            ErrorDetails.builder()
                                    .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
                }
                break;
            case BINARY:
                if (arguments.size() != 2) {
                    throw new AthenaConnectorException("Binary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.",
                            ErrorDetails.builder()
                                    .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
                }
                break;
            case VARARG:
                break;
            default:
                throw new AthenaConnectorException("A new operator type was introduced without adding support for it.",
                        ErrorDetails.builder()
                                .errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
        }

        JdbcQueryFactory factory = getQueryFactory();
        String clause;
        switch (functionEnum) {
            case ADD_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " + "));
                break;
            case AND_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " AND "));
                break;
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME:
                clause = writeArrayConstructorClause(type, arguments);
                break;
            case DIVIDE_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " / "));
                break;
            case EQUAL_OPERATOR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " = "));
                break;
            case GREATER_THAN_OPERATOR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " > "));
                break;
            case GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " >= "));
                break;
            case IN_PREDICATE_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "in_expression", Map.of("column", arguments.get(0), "values", arguments.get(1)));
                break;
            case IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "is_distinct_from", Map.of("left", arguments.get(0), "right", arguments.get(1)));
                break;
            case IS_NULL_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "null_predicate", Map.of("columnName", arguments.get(0), "isNull", true));
                break;
            case LESS_THAN_OPERATOR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " < "));
                break;
            case LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " <= "));
                break;
            case LIKE_PATTERN_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "like_expression", Map.of("column", arguments.get(0), "pattern", arguments.get(1)));
                break;
            case MODULUS_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " % "));
                break;
            case MULTIPLY_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " * "));
                break;
            case NEGATE_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "unary_operator", Map.of("operator", "-", "operand", arguments.get(0)));
                break;
            case NOT_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " <> "));
                break;
            case NOT_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "unary_operator", Map.of("operator", "NOT ", "operand", arguments.get(0)));
                break;
            case NULLIF_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "function_call_2args", Map.of("functionName", "NULLIF", "arg1", arguments.get(0), "arg2", arguments.get(1)));
                break;
            case OR_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " OR "));
                break;
            case SUBTRACT_FUNCTION_NAME:
                clause = JdbcSqlUtils.renderTemplate(factory, "join_expression", Map.of("items", arguments, "separator", " - "));
                break;
            default:
                throw new AthenaConnectorException("The function " + functionName.getFunctionName() + " does not have an implementation",
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
        }
        if (clause == null) {
            return "";
        }
        return clause;
    }
}
