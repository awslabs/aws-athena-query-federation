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
import io.substrait.proto.SortRel;
import io.substrait.proto.SortField.SortDirection;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for Substrait plan processing operations like LIMIT and SORT pushdown.
 */
public final class LimitAndSortHelper
{
    private static final Logger logger = LoggerFactory.getLogger(LimitAndSortHelper.class);

    private LimitAndSortHelper() {}

    /**
     * Determines if a LIMIT can be applied and extracts the limit value from either
     * the Substrait plan or constraints.
     */
    public static Pair<Boolean, Integer> getLimit(Plan plan, Constraints constraints)
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
                return Pair.of(true, limit);
            }
            else {
                return Pair.of(true, (int) constraints.getLimit());
            }
        }
        return Pair.of(false, -1);
    }

    /**
     * Extracts sort information from Substrait plan for ORDER BY pushdown optimization.
     */
    public static Pair<Boolean, List<GenericSortField>> getSortFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return Pair.of(false, Collections.emptyList());
        }
        try {
            SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                    plan.getRelations(0).getRoot().getInput());
            if (substraitRelModel.getSortRel() == null) {
                return Pair.of(false, Collections.emptyList());
            }
            List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);
            List<GenericSortField> sortFields = extractGenericSortFields(substraitRelModel.getSortRel(), tableColumns);
            return Pair.of(true, sortFields);
        }
        catch (Exception e) {
            logger.warn("Failed to extract sort from plan{}", e);
            return Pair.of(false, Collections.emptyList());
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
     * Generic sort field representation that connectors can use to build their specific sort formats.
     */
    public static class GenericSortField
    {
        private final String columnName;
        private final boolean ascending;

        public GenericSortField(String columnName, boolean ascending)
        {
            this.columnName = columnName;
            this.ascending = ascending;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public boolean isAscending()
        {
            return ascending;
        }
    }
}
