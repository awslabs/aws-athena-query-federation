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
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        return "and:bool".equals(functionName) || "or:bool".equals(functionName);
    }

    /**
     * Maps Substrait function names to corresponding Operator enum values.
     * This method is mapping only small set of operators, and we will extend this as we need.
     * 
     * @param functionName The Substrait function name (e.g., "gt:any_any", "equal:any_any")
     * @return The corresponding Operator enum value
     * @throws UnsupportedOperationException if the function name is not supported
     */
    private static SubstraitOperator mapToOperator(String functionName)
    {
        switch (functionName) {
            case "gt:any_any":
                return SubstraitOperator.GREATER_THAN;
            case "gte:any_any":
                return SubstraitOperator.GREATER_THAN_OR_EQUAL_TO;
            case "lt:any_any":
                return SubstraitOperator.LESS_THAN;
            case "lte:any_any":
                return SubstraitOperator.LESS_THAN_OR_EQUAL_TO;
            case "equal:any_any":
                return SubstraitOperator.EQUAL;
            case "not_equal:any_any":
                return SubstraitOperator.NOT_EQUAL;
            case "is_null:any":
                return SubstraitOperator.IS_NULL;
            case "is_not_null:any":
                return SubstraitOperator.IS_NOT_NULL;
            case "and:bool":
                return SubstraitOperator.AND;
            case "or:bool":
                return SubstraitOperator.OR;
            default:
                throw new UnsupportedOperationException("Unsupported operator function: " + functionName);
        }
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
}
