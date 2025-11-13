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
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.AND_BOOL;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.EQUAL_ANY_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.GT_ANY_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.GTE_ANY_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.GTE_PTS_PTS;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.GT_PTS_PTS;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.IS_NOT_NULL_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.IS_NULL_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.LT_ANY_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.LTE_ANY_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.LTE_PTS_PTS;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.LT_PTS_PTS;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.NOT_BOOL;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.NOT_EQUAL_ANY_ANY;
import static com.amazonaws.athena.connector.substrait.model.SubstraitFunctionNames.OR_BOOL;

/**
 * Utility class for parsing Substrait function expressions into filter predicates and column predicates.
 * This parser handles scalar functions, comparison operations, logical operations (AND/OR), and literal values
 * from Substrait expressions, converting them into a more convenient predicate model.
 */
public final class SubstraitFunctionParser
{
    private SubstraitFunctionParser()
    {
        // Utility class - prevent instantiation
    }

    /**
     * Parses a Substrait expression into a map of column predicates grouped by column name.
     * This method extracts all column predicates from the expression and organizes them by the column they apply to.
     * 
     * @param extensionDeclarationList List of function extension declarations from the Substrait plan
     * @param expression The Substrait expression to parse
     * @param columnNames List of column names in the schema for field reference resolution
     * @return A map where keys are column names and values are lists of predicates for that column
     */
    public static Map<String, List<ColumnPredicate>> getColumnPredicatesMap(List<SimpleExtensionDeclaration> extensionDeclarationList,
                                                                            Expression expression,
                                                                            List<String> columnNames)
    {
        List<ColumnPredicate> columnPredicates = parseColumnPredicates(extensionDeclarationList, expression, columnNames);
        Map<String, List<ColumnPredicate>> columnPredicatesMap = new HashMap<>();
        for (ColumnPredicate columnPredicate : columnPredicates) {
            columnPredicatesMap.computeIfAbsent(columnPredicate.getColumn(), k -> new ArrayList<>()).add(columnPredicate);
        }
        return columnPredicatesMap;
    }

    /**
     * Recursively parses a Substrait expression to extract all column predicates.
     * This method flattens logical operations (AND/OR) and extracts individual column predicates.
     * 
     * @param extensionDeclarationList List of function extension declarations from the Substrait plan
     * @param expression The Substrait expression to parse
     * @param columnNames List of column names in the schema for field reference resolution
     * @return A flat list of all column predicates found in the expression
     */
    public static List<ColumnPredicate> parseColumnPredicates(List<SimpleExtensionDeclaration> extensionDeclarationList,
                                                              Expression expression,
                                                              List<String> columnNames)
    {
        List<ColumnPredicate> columnPredicates = new ArrayList<>();
        ScalarFunctionInfo functionInfo = extractScalarFunctionInfo(expression, extensionDeclarationList);
        
        if (functionInfo == null) {
            return columnPredicates;
        }

        // Handle NOT unary operator
        if (NOT_BOOL.equals(functionInfo.getFunctionName())) {
            ColumnPredicate notPredicate = handleNotOperator(functionInfo, extensionDeclarationList, columnNames);
            if (notPredicate != null) {
                columnPredicates.add(notPredicate);
            }
            return columnPredicates;
        }

        // Handle logical operators by flattening
        if (isLogicalOperator(functionInfo.getFunctionName())) {
            for (FunctionArgument argument : functionInfo.getArguments()) {
                columnPredicates.addAll(
                        parseColumnPredicates(extensionDeclarationList, argument.getValue(), columnNames));
            }
            return columnPredicates;
        }

        // Handle binary comparison operations
        if (functionInfo.getArguments().size() == 2) {
            ColumnPredicate predicate = createBinaryColumnPredicate(functionInfo, columnNames);
            columnPredicates.add(predicate);
        }

        // Handle unary operations
        if (functionInfo.getArguments().size() == 1) {
            ColumnPredicate predicate = createUnaryColumnPredicate(functionInfo, columnNames);
            columnPredicates.add(predicate);
        }
        return columnPredicates;
    }

    /**
     * Parses a Substrait expression into a logical expression tree that preserves AND/OR/NOT hierarchy.
     * This method maintains the original logical structure instead of flattening it.
     *
     * @param extensionDeclarationList List of function extension declarations from the Substrait plan
     * @param expression The Substrait expression to parse
     * @param columnNames List of column names in the schema for field reference resolution
     * @return LogicalExpression tree preserving the original logical structure
     */
    public static LogicalExpression parseLogicalExpression(List<SimpleExtensionDeclaration> extensionDeclarationList,
                                                           Expression expression,
                                                           List<String> columnNames)
    {
        if (expression == null) {
            return null;
        }
        
        // Extract function information from the Substrait expression
        ScalarFunctionInfo functionInfo = extractScalarFunctionInfo(expression, extensionDeclarationList);

        if (functionInfo == null) {
            return null;
        }

        // Handle NOT operator by delegating to handleNotOperator which converts NOT(expr)
        // to appropriate predicates (NOT_EQUAL, NAND, NOR) based on the inner expression type
        if (NOT_BOOL.equals(functionInfo.getFunctionName())) {
            ColumnPredicate notPredicate = handleNotOperator(functionInfo, extensionDeclarationList, columnNames);
            if (notPredicate != null) {
                return new LogicalExpression(notPredicate);
            }
            return null;
        }

        // Handle logical operators (AND/OR) by building tree structure instead of flattening
        // This preserves the original logical hierarchy from the SQL query
        if (isLogicalOperator(functionInfo.getFunctionName())) {
            List<LogicalExpression> childExpressions = new ArrayList<>();
            
            // Recursively parse each child argument to build the expression tree
            for (FunctionArgument argument : functionInfo.getArguments()) {
                LogicalExpression childExpr = parseLogicalExpression(extensionDeclarationList, argument.getValue(), columnNames);
                if (childExpr != null) {
                    childExpressions.add(childExpr);
                }
            }
            
            // Create logical expression node with the operator and its children
            SubstraitOperator operator = mapToOperator(functionInfo.getFunctionName());
            return new LogicalExpression(operator, childExpressions);
        }

        // Handle binary comparison operations (e.g., column = value, column > value)
        if (functionInfo.getArguments().size() == 2) {
            ColumnPredicate predicate = createBinaryColumnPredicate(functionInfo, columnNames);
            // Wrap the predicate in a leaf LogicalExpression node
            return new LogicalExpression(predicate);
        }

        // Handle unary operations (e.g., column IS NULL, column IS NOT NULL)
        if (functionInfo.getArguments().size() == 1) {
            ColumnPredicate predicate = createUnaryColumnPredicate(functionInfo, columnNames);
            // Wrap the predicate in a leaf LogicalExpression node
            return new LogicalExpression(predicate);
        }

        return null;
    }

    /**
     * Creates a mapping from function reference anchors to function names.
     * This mapping is used to resolve function references in Substrait expressions.
     * 
     * @param extensionDeclarationList List of extension declarations containing function definitions
     * @return A map from function anchor IDs to function names
     */
    private static Map<Integer, String> mapFunctionReferences(List<SimpleExtensionDeclaration> extensionDeclarationList)
    {
        Map<Integer, String> functionMap = new HashMap<>();
        for (SimpleExtensionDeclaration extension : extensionDeclarationList) {
            if (extension.hasExtensionFunction()) {
                int anchor = extension.getExtensionFunction().getFunctionAnchor();
                String name = extension.getExtensionFunction().getName();
                functionMap.put(anchor, name);
            }
        }
        return functionMap;
    }

    /**
     * Extracts the column name from a field reference expression.
     */
    private static String extractColumnName(Expression expr, List<String> schemaNames)
    {
        if (expr.hasCast()) {
            expr =  expr.getCast().getInput();
        }
        int fieldIndex = expr.getSelection().getDirectReference().getStructField().getField();

        return schemaNames.get(fieldIndex);
    }

    /**
     * Extracts a literal value from an expression, handling possible cast operations.
     * If the expression contains a cast, the underlying literal value is extracted.
     */
    private static Pair<Object, ArrowType> extractValueWithPossibleCast(Expression expr)
    {
        if (expr.hasCast()) {
            return SubstraitLiteralConverter.extractLiteralValue(expr.getCast().getInput());
        }
        return SubstraitLiteralConverter.extractLiteralValue(expr);
    }

    /**
     * Extracts scalar function information from an expression.
     */
    private static ScalarFunctionInfo extractScalarFunctionInfo(Expression expression, List<SimpleExtensionDeclaration> extensionDeclarationList)
    {
        if (!expression.hasScalarFunction()) {
            return null;
        }

        Expression.ScalarFunction scalarFunction = expression.getScalarFunction();
        Map<Integer, String> functionMap = mapFunctionReferences(extensionDeclarationList);
        String functionName = functionMap.get(scalarFunction.getFunctionReference());
        List<FunctionArgument> arguments = scalarFunction.getArgumentsList();
        
        return new ScalarFunctionInfo(functionName, arguments);
    }
    
    /**
     * Creates a column predicate for unary operations.
     */
    private static ColumnPredicate createUnaryColumnPredicate(ScalarFunctionInfo functionInfo, List<String> columnNames)
    {
        String columnName = extractColumnName(functionInfo.getArguments().get(0).getValue(), columnNames);
        SubstraitOperator substraitOperator = mapToOperator(functionInfo.getFunctionName());
        return new ColumnPredicate(columnName, substraitOperator, null, null);
    }
    
    /**
     * Creates a column predicate for binary operations.
     */
    private static ColumnPredicate createBinaryColumnPredicate(ScalarFunctionInfo functionInfo, List<String> columnNames)
    {
        String columnName = extractColumnName(functionInfo.getArguments().get(0).getValue(), columnNames);
        Pair<Object, ArrowType> value = extractValueWithPossibleCast(functionInfo.getArguments().get(1).getValue());
        SubstraitOperator substraitOperator = mapToOperator(functionInfo.getFunctionName());
        return new ColumnPredicate(columnName, substraitOperator, value.getLeft(), value.getRight());
    }
    
    /**
     * Checks if a function name represents a logical operator.
     */
    private static boolean isLogicalOperator(String functionName)
    {
        return AND_BOOL.equals(functionName) || OR_BOOL.equals(functionName);
    }

    /**
     * Maps Substrait function names to corresponding Operator enum values.
     * This method supports comparison operators, logical operators, null checks, and the NOT operator.
     * The mapping will be extended as additional operators are needed.
     * 
     * @param functionName The Substrait function name (e.g., "gt:any_any", "equal:any_any", "not:bool")
     * @return The corresponding Operator enum value
     * @throws UnsupportedOperationException if the function name is not supported
     */
    private static SubstraitOperator mapToOperator(String functionName)
    {
        return switch (functionName) {
            case GT_ANY_ANY, GT_PTS_PTS -> SubstraitOperator.GREATER_THAN;
            case GTE_ANY_ANY, GTE_PTS_PTS -> SubstraitOperator.GREATER_THAN_OR_EQUAL_TO;
            case LT_ANY_ANY, LT_PTS_PTS -> SubstraitOperator.LESS_THAN;
            case LTE_ANY_ANY, LTE_PTS_PTS -> SubstraitOperator.LESS_THAN_OR_EQUAL_TO;
            case EQUAL_ANY_ANY -> SubstraitOperator.EQUAL;
            case NOT_EQUAL_ANY_ANY -> SubstraitOperator.NOT_EQUAL;
            case IS_NULL_ANY -> SubstraitOperator.IS_NULL;
            case IS_NOT_NULL_ANY -> SubstraitOperator.IS_NOT_NULL;
            case AND_BOOL -> SubstraitOperator.AND;
            case OR_BOOL -> SubstraitOperator.OR;
            case NOT_BOOL -> SubstraitOperator.NOT;
            default -> throw new UnsupportedOperationException("Unsupported operator function: " + functionName);
        };
    }

    /**
     * Helper class to hold scalar function information.
     */
    private static final class ScalarFunctionInfo
    {
        private final String functionName;
        private final List<FunctionArgument> arguments;

        public ScalarFunctionInfo(String functionName, List<FunctionArgument> arguments)
        {
            this.functionName = functionName;
            this.arguments = arguments;
        }

        public String getFunctionName()
        {
            return functionName;
        }

        public List<FunctionArgument> getArguments()
        {
            return arguments;
        }
    }

    /**
     * Handles NOT operator expressions by analyzing the inner expression and applying appropriate negation logic.
     * Supports various NOT patterns including NOT(AND), NOT(OR), NOT IN, and simple predicate negation.
     *
     * @param notFunctionInfo The scalar function info for the NOT operation
     * @param extensionDeclarationList List of extension declarations for function mapping
     * @param columnNames List of available column names
     * @return ColumnPredicate representing the negated expression, or null if not supported
     */
    private static ColumnPredicate handleNotOperator(
            ScalarFunctionInfo notFunctionInfo,
            List<SimpleExtensionDeclaration> extensionDeclarationList,
            List<String> columnNames)
    {
        if (notFunctionInfo.getArguments().size() != 1) {
            return null;
        }
        Expression innerExpression = notFunctionInfo.getArguments().get(0).getValue();
        ScalarFunctionInfo innerFunctionInfo = extractScalarFunctionInfo(innerExpression, extensionDeclarationList);
        // Case: NOT(AND(...)) => NAND
        if (innerFunctionInfo != null && AND_BOOL.equals(innerFunctionInfo.getFunctionName())) {
            List<ColumnPredicate> childPredicates =
                    parseColumnPredicates(extensionDeclarationList, innerExpression, columnNames);
            return new ColumnPredicate(
                    null,
                    SubstraitOperator.NAND,
                    childPredicates,
                    null
            );
        }
        // Case: NOT(OR(...)) => NOR
        if (innerFunctionInfo != null && OR_BOOL.equals(innerFunctionInfo.getFunctionName())) {
            List<ColumnPredicate> childPredicates =
                    parseColumnPredicates(extensionDeclarationList, innerExpression, columnNames);
            return new ColumnPredicate(
                    null,
                    SubstraitOperator.NOR,
                    childPredicates,
                    null
            );
        }
        // NOT IN pattern detection - reserved for future use cases
        // NOTE: This handles scenarios where expressions other than direct OR operations
        // may flatten to multiple EQUAL predicates on the same column. Currently, standard
        // NOT(OR(...)) expressions are processed as NOR operations above.
        List<ColumnPredicate> innerPredicates = parseColumnPredicates(extensionDeclarationList, innerExpression, columnNames);
        if (isNotInPattern(innerPredicates)) {
            return createNotInPredicate(innerPredicates);
        }
        if (innerPredicates.size() == 1) {
            return createNegatedPredicate(innerPredicates.get(0));
        }
        return null;
    }

    /**
     * Determines if a list of predicates represents a NOT IN pattern.
     * A NOT IN pattern consists of multiple EQUAL predicates on the same column.
     *
     * @param predicates List of column predicates to analyze
     * @return true if the predicates form a NOT IN pattern, false otherwise
     */
    private static boolean isNotInPattern(List<ColumnPredicate> predicates)
    {
        if (predicates.size() <= 1) {
            return false;
        }
        String firstColumn = predicates.get(0).getColumn();
        for (ColumnPredicate predicate : predicates) {
            if (predicate.getOperator() != SubstraitOperator.EQUAL ||
                    !predicate.getColumn().equals(firstColumn)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a NOT_IN predicate from a list of EQUAL predicates on the same column.
     * Extracts all values from the EQUAL predicates and combines them into a single NOT_IN operation.
     *
     * @param equalPredicates List of EQUAL predicates on the same column
     * @return ColumnPredicate with NOT_IN operator containing all excluded values, or null if input is empty
     */
    private static ColumnPredicate createNotInPredicate(List<ColumnPredicate> equalPredicates)
    {
        if (equalPredicates.isEmpty()) {
            return null;
        }
        String column = equalPredicates.get(0).getColumn();
        List<Object> excludedValues = new ArrayList<>();
        for (ColumnPredicate predicate : equalPredicates) {
            excludedValues.add(predicate.getValue());
        }
        return new ColumnPredicate(column, SubstraitOperator.NOT_IN, excludedValues, null);
    }

    /**
     * Creates a negated version of a given predicate by applying logical negation rules.
     * Maps operators to their logical opposites (e.g., EQUAL → NOT_EQUAL, GREATER_THAN → LESS_THAN_OR_EQUAL_TO).
     * For operators that cannot be directly negated, returns a NOT operator predicate.
     *
     * @param predicate The original predicate to negate
     * @return ColumnPredicate representing the negated form of the input predicate
     */
    private static ColumnPredicate createNegatedPredicate(ColumnPredicate predicate)
    {
        return switch (predicate.getOperator()) {
            case EQUAL ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.NOT_EQUAL,
                            predicate.getValue(), predicate.getArrowType());
            case NOT_EQUAL ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.EQUAL,
                            predicate.getValue(), predicate.getArrowType());
            case GREATER_THAN ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.LESS_THAN_OR_EQUAL_TO,
                            predicate.getValue(), predicate.getArrowType());
            case GREATER_THAN_OR_EQUAL_TO ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.LESS_THAN,
                            predicate.getValue(), predicate.getArrowType());
            case LESS_THAN ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.GREATER_THAN_OR_EQUAL_TO,
                            predicate.getValue(), predicate.getArrowType());
            case LESS_THAN_OR_EQUAL_TO ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.GREATER_THAN,
                            predicate.getValue(), predicate.getArrowType());
            case IS_NULL ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.IS_NOT_NULL,
                            null, predicate.getArrowType());
            case IS_NOT_NULL ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.IS_NULL,
                            null, predicate.getArrowType());
            default ->
                    new ColumnPredicate(predicate.getColumn(), SubstraitOperator.NOT,
                            predicate.getValue(), predicate.getArrowType());
        };
    }
}
