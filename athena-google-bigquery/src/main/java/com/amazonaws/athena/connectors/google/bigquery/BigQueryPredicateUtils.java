/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for handling Substrait predicates in BigQuery connector.
 */
public final class BigQueryPredicateUtils
{
    private BigQueryPredicateUtils() {}

    /**
     * Builds filter predicates from a Substrait execution plan.
     * 
     * @param plan the Substrait plan containing filter conditions
     * @return map of column names to their predicates
     */
    public static Map<String, List<ColumnPredicate>> buildFilterPredicatesFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return new HashMap<>();
        }
        
        SubstraitRelModel substraitRelModel = extractSubstraitRelModel(plan);
        if (substraitRelModel.getFilterRel() == null) {
            return new HashMap<>();
        }
        
        List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);
        
        return SubstraitFunctionParser.getColumnPredicatesMap(
            extensionDeclarations, substraitRelModel.getFilterRel().getCondition(), tableColumns);
    }

    /**
     * Extracts SubstraitRelModel from a Substrait plan.
     */
    private static SubstraitRelModel extractSubstraitRelModel(Plan plan)
    {
        return SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
    }

    /**
     * Converts Substrait predicates to BigQuery SQL WHERE clause.
     * 
     * @param filterPredicates map of column predicates
     * @return SQL WHERE clause string
     */
    public static String buildBigQueryWhereClause(Map<String, List<ColumnPredicate>> filterPredicates)
    {
        if (filterPredicates == null || filterPredicates.isEmpty()) {
            return "";
        }

        return filterPredicates.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream()
                .map(BigQueryPredicateUtils::convertPredicateToBigQuerySQL))
            .filter(clause -> !clause.isEmpty())
            .collect(Collectors.joining(" AND "));
    }

    /**
     * Converts a single ColumnPredicate to BigQuery SQL.
     */
    private static String convertPredicateToBigQuerySQL(ColumnPredicate predicate)
    {
        String columnName = predicate.getColumn();
        SubstraitOperator operator = predicate.getOperator();
        Object value = predicate.getValue();
        
        switch (operator) {
            case EQUAL:
                return columnName + " = " + formatValue(value);
            case NOT_EQUAL:
                return columnName + " != " + formatValue(value);
            case LESS_THAN:
                return columnName + " < " + formatValue(value);
            case LESS_THAN_OR_EQUAL_TO:
                return columnName + " <= " + formatValue(value);
            case GREATER_THAN:
                return columnName + " > " + formatValue(value);
            case GREATER_THAN_OR_EQUAL_TO:
                return columnName + " >= " + formatValue(value);
            case IS_NULL:
                return columnName + " IS NULL";
            case IS_NOT_NULL:
                return columnName + " IS NOT NULL";
            default:
                return "";
        }
    }

    /**
     * Formats a single value for SQL.
     */
    private static String formatValue(Object value)
    {
        if (value instanceof String) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        return value.toString();
    }

    /**
     * Formats a list of values for SQL IN clause.
     */
    private static String formatValueList(List<?> values)
    {
        if (values == null || values.isEmpty()) {
            return "()";
        }
        
        String formattedValues = values.stream()
            .map(BigQueryPredicateUtils::formatValue)
            .collect(Collectors.joining(", "));
        
        return "(" + formattedValues + ")";
    }
}
