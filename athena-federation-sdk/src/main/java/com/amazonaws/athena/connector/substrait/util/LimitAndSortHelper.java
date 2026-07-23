package com.amazonaws.athena.connector.substrait.util;

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

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import io.substrait.proto.Plan;
import io.substrait.proto.SortField.SortDirection;
import io.substrait.proto.SortRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility class that extracts LIMIT and ORDER BY information from Substrait plans
 * to enable pushdown of these operations to the underlying data source.
 *
 * <p>This helper parses {@link Plan} objects to extract:
 * <ul>
 *   <li>LIMIT values from {@link FetchRel} nodes, falling back to {@link Constraints} when no plan is available</li>
 *   <li>ORDER BY sort fields from {@link SortRel} nodes, mapped to column names via table metadata</li>
 * </ul>
 *
 * <p>Connectors can use the extracted {@link GenericSortField} representations to build
 * data-source-specific sort clauses (e.g., MongoDB sort documents, SQL ORDER BY clauses).
 */
public final class LimitAndSortHelper
{
    private static final Logger logger = LoggerFactory.getLogger(LimitAndSortHelper.class);

    private LimitAndSortHelper() {}

    /**
     * Determines if a LIMIT can be applied and extracts the limit value.
     *
     * <p>When a Substrait {@link Plan} is provided, the limit is extracted from its
     * {@link FetchRel} node. Otherwise, falls back to the limit defined in {@link Constraints}.
     *
     * @param plan        the Substrait plan to extract the limit from, or {@code null} to use constraints
     * @param constraints the query constraints containing a fallback limit value
     * @return an {@link Optional} containing the limit value if one can be applied, or empty otherwise
     */
    public static Optional<Integer> getLimit(Plan plan, Constraints constraints)
    {
        SubstraitRelModel substraitRelModel = null;
        boolean useQueryPlan = false;
        if (plan != null) {
            substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
            useQueryPlan = true;
        }
        if (canApplyLimit(constraints, substraitRelModel, useQueryPlan)) {
            if (useQueryPlan) {
                int limit = getLimit(substraitRelModel);
                return Optional.of(limit);
            }
            else {
                return Optional.of((int) (constraints.getLimit()));
            }
        }
        return Optional.empty();
    }

    /**
     * Extracts sort field information from a Substrait plan for ORDER BY pushdown.
     *
     * <p>Parses the {@link SortRel} node in the plan to produce a list of {@link GenericSortField}
     * objects, each containing a column name and sort direction. If the plan is {@code null},
     * has no relations, or contains no sort relation, returns empty.
     *
     * @param plan the Substrait plan to extract sort information from, or {@code null}
     * @return an {@link Optional} containing the list of sort fields, or empty if no sort is present
     */
    public static Optional<List<GenericSortField>> getSortFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return Optional.empty();
        }
        try {
            SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                    plan.getRelations(0).getRoot().getInput());
            if (substraitRelModel.getSortRel() == null) {
                return Optional.empty();
            }
            List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);
            List<GenericSortField> sortFields = extractGenericSortFields(substraitRelModel.getSortRel(), tableColumns);
            return Optional.of(sortFields);
        }
        catch (Exception e) {
            logger.warn("Failed to extract sort from plan{}", e);
            return Optional.empty();
        }
    }

    private static boolean canApplyLimit(Constraints constraints, SubstraitRelModel substraitRelModel, boolean useQueryPlan)
    {
        if (useQueryPlan) {
            if (substraitRelModel.getFetchRel() != null) {
                return getLimit(substraitRelModel) > 0;
            }
            return false;
        }
        return constraints.hasLimit();
    }

    private static int getLimit(SubstraitRelModel substraitRelModel)
    {
        FetchRel fetchRel = substraitRelModel.getFetchRel();
        return (int) fetchRel.getCount();
    }

    private static List<GenericSortField> extractGenericSortFields(SortRel sortRel, List<String> tableColumns)
    {
        List<GenericSortField> sortFields = new ArrayList<>();
        for (io.substrait.proto.SortField sortField : sortRel.getSortsList()) {
            try {
                int fieldIndex = extractFieldIndexFromExpression(sortField.getExpr());
                if (fieldIndex >= 0 && fieldIndex < tableColumns.size()) {
                    String columnName = tableColumns.get(fieldIndex);
                    SortDirection direction = sortField.getDirection();
                    boolean ascending = (direction == SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST ||
                            direction == SortDirection.SORT_DIRECTION_ASC_NULLS_LAST);
                    sortFields.add(new GenericSortField(columnName, ascending));
                }
            }
            catch (Exception e) {
                logger.warn("Failed to extract sort field, skipping: {}", e.getMessage());
            }
        }
        return sortFields;
    }

    private static int extractFieldIndexFromExpression(Expression expression)
    {
        if (expression.hasSelection() && expression.getSelection().hasDirectReference()) {
            Expression.ReferenceSegment segment = expression.getSelection().getDirectReference();
            if (segment.hasStructField()) {
                return segment.getStructField().getField();
            }
        }
        throw new IllegalArgumentException("Cannot extract field index from expression");
    }

    /**
     * A data-source-agnostic representation of a sort field, containing the column name
     * and sort direction. Connectors use this to build native sort clauses for their
     * respective data sources.
     */
    public static class GenericSortField
    {
        private final String columnName;
        private final boolean ascending;

        /**
         * Creates a new sort field.
         *
         * @param columnName the name of the column to sort by
         * @param ascending  {@code true} for ascending order, {@code false} for descending
         */
        public GenericSortField(String columnName, boolean ascending)
        {
            this.columnName = columnName;
            this.ascending = ascending;
        }

        /**
         * @return the name of the column to sort by
         */
        public String getColumnName()
        {
            return columnName;
        }

        /**
         * @return {@code true} if the sort direction is ascending, {@code false} if descending
         */
        public boolean isAscending()
        {
            return ascending;
        }
    }
}
