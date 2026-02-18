/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class has interfaces used for the generation of projections and predicates used for document search queries.
 */
class ElasticsearchQueryUtils
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryUtils.class);

    // Predicate conjunctions.
    private static final String AND_OPER = " AND ";
    private static final String OR_OPER = " OR ";
    private static final String NOT_OPER = "NOT ";
    private static final String RANGE_OPER = " TO ";
    private static final String LOWER_UNBOUNDED_RANGE = "[*";
    private static final String LOWER_INCLUSIVE_RANGE = "[";
    private static final String LOWER_EXCLUSIVE_RANGE = "{";
    private static final String UPPER_UNBOUNDED_RANGE = "*]";
    private static final String UPPER_INCLUSIVE_RANGE = "]";
    private static final String UPPER_EXCLUSIVE_RANGE = "}";
    private static final String EMPTY_PREDICATE = "";

    // Existence predicates.
    private static final String existsPredicate(boolean exists, String fieldName)
    {
        if (exists) {
            // (_exists:field)
            return "(_exists_:" + fieldName + ")";
        }
        else {
            // (NOT _exists_:field)
            return "(" + NOT_OPER + "_exists_:" + fieldName + ")";
        }
    }

    private ElasticsearchQueryUtils() {}

    /**
     * Creates a projection (using the schema) on which fields should be included in the search index request. For
     * complex type STRUCT, there is no need to include each individual nested field in the projection. Since the
     * schema contains all nested fields in the STRUCT, only the name of the STRUCT field is added to the projection
     * allowing Elasticsearch to return the entire object including all nested fields.
     * @param schema is the schema containing the requested projection.
     * @return a projection wrapped in a FetchSourceContext object.
     */
    protected static FetchSourceContext getProjection(Schema schema)
    {
        List<String> includedFields = new ArrayList<>();

        for (Field field : schema.getFields()) {
            includedFields.add(field.getName());
        }

        logger.info("Included fields: " + includedFields);

        return new FetchSourceContext(true, Strings.toStringArray(includedFields), Strings.EMPTY_ARRAY);
    }

    /**
     * Given a set of Constraints, create the query that can push predicates into the Elasticsearch data-source.
//     * @param constraintSummary is a map containing the constraints used to form the predicate for predicate push-down.
     * @return the query builder that will be injected into the query.
     */
    protected static QueryBuilder getQuery(Constraints constraints)
    {
        Map<String, ValueSet> constraintSummary = constraints.getSummary();
        List<String> predicates = new ArrayList<>();

        constraintSummary.forEach((fieldName, constraint) -> {
            String predicate = getPredicate(fieldName, constraint);
            if (!predicate.isEmpty()) {
                // predicate1, predicate2, predicate3...
                predicates.add(predicate);
            }
        });

        if (predicates.isEmpty()) {
            // No predicates formed.
            logger.info("Predicates are NOT formed.");
            return QueryBuilders.matchAllQuery();
        }

        // predicate1 AND predicate2 AND predicate3...
        String formedPredicates = Strings.collectionToDelimitedString(predicates, AND_OPER);
        logger.info("Formed Predicates: {}", formedPredicates);

        return QueryBuilders.queryStringQuery(formedPredicates).queryName(formedPredicates);
    }

    /**
     * Parses Substrait plan and extracts filter predicates per column
     */
    protected static Map<String, List<ColumnPredicate>> buildFilterPredicatesFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return new HashMap<>();
        }

        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
        if (substraitRelModel.getFilterRel() == null) {
            return new HashMap<>();
        }

        List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);

        return SubstraitFunctionParser.getColumnPredicatesMap(
                extensionDeclarations,
                substraitRelModel.getFilterRel().getCondition(),
                tableColumns);
    }

    /**
     * Enhanced query builder that converts Substrait plan to Elasticsearch QueryBuilder.
     * Handles AND/OR logical structure from SQL via the Query Plan.
     * Example: "job_title IN ('A', 'B') OR job_title < 'C'"
     * → bool query with should clauses for each OR condition
     */
    protected static QueryBuilder getQueryFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }

        // Extract Substrait relation model from the plan to access filter conditions
        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
        if (substraitRelModel.getFilterRel() == null) {
            return QueryBuilders.matchAllQuery();
        }

        final List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        final List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);

        try {
            final LogicalExpression logicalExpr = SubstraitFunctionParser.parseLogicalExpression(
                    extensionDeclarations,
                    substraitRelModel.getFilterRel().getCondition(),
                    tableColumns);

            if (logicalExpr != null) {
                // Successfully parsed expression tree - convert to Elasticsearch query
                QueryBuilder queryBuilder = convertLogicalExpressionToQuery(logicalExpr);
                if (queryBuilder != null) {
                    return queryBuilder;
                }
            }
        }
        catch (Exception e) {
            logger.warn("Tree-based parsing failed {}. Returning match_all query.", e.getMessage(), e);
        }
        return QueryBuilders.matchAllQuery();
    }

    /**
     * Converts a LogicalExpression tree to Elasticsearch QueryBuilder while preserving logical structure.
     * Example: OR(EQUAL(job_title, 'A'), EQUAL(job_title, 'B'))
     * → bool query with should clauses
     */
    private static QueryBuilder convertLogicalExpressionToQuery(LogicalExpression expression)
    {
        if (expression == null) {
            return QueryBuilders.matchAllQuery();
        }

        // Handle leaf nodes (individual predicates like job_title = 'Engineer')
        if (expression.isLeaf()) {
            ColumnPredicate predicate = expression.getLeafPredicate();
            return convertColumnPredicateToQuery(predicate.getColumn(),
                    Collections.singletonList(predicate));
        }

        // Handle logical operators (AND/OR nodes with children)
        List<QueryBuilder> childQueries = new ArrayList<>();
        for (LogicalExpression child : expression.getChildren()) {
            QueryBuilder childQuery = convertLogicalExpressionToQuery(child);
            if (childQuery != null) {
                childQueries.add(childQuery);
            }
        }

        if (childQueries.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }
        if (childQueries.size() == 1) {
            // Single child - no need for logical operator wrapper
            return childQueries.get(0);
        }

        // Apply the logical operator to combine child queries
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        switch (expression.getOperator()) {
            case AND:
                for (QueryBuilder childQuery : childQueries) {
                    boolQuery.must(childQuery);
                }
                return boolQuery;
            case OR:
                for (QueryBuilder childQuery : childQueries) {
                    boolQuery.should(childQuery);
                }
                boolQuery.minimumShouldMatch(1);
                return boolQuery;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported logical operator: " + expression.getOperator());
        }
    }

    /**
     * Converts a list of ColumnPredicates into an Elasticsearch QueryBuilder
     */
    private static QueryBuilder convertColumnPredicateToQuery(String column, List<ColumnPredicate> colPreds)
    {
        if (colPreds == null || colPreds.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }

        List<Object> equalValues = new ArrayList<>();
        List<QueryBuilder> otherQueries = new ArrayList<>();

        for (ColumnPredicate pred : colPreds) {
            Object value = convertSubstraitValue(pred);
            SubstraitOperator op = pred.getOperator();

            switch (op) {
                case IS_NULL:
                    otherQueries.add(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(column)));
                    break;
                case IS_NOT_NULL:
                    otherQueries.add(QueryBuilders.existsQuery(column));
                    break;
                case EQUAL:
                    equalValues.add(value);
                    break;
                case NOT_EQUAL:
                    BoolQueryBuilder notEqualQuery = QueryBuilders.boolQuery()
                            .must(QueryBuilders.existsQuery(column))
                            .mustNot(QueryBuilders.termQuery(column, value));
                    otherQueries.add(notEqualQuery);
                    break;
                case GREATER_THAN:
                    otherQueries.add(QueryBuilders.rangeQuery(column).gt(value));
                    break;
                case GREATER_THAN_OR_EQUAL_TO:
                    otherQueries.add(QueryBuilders.rangeQuery(column).gte(value));
                    break;
                case LESS_THAN:
                    otherQueries.add(QueryBuilders.rangeQuery(column).lt(value));
                    break;
                case LESS_THAN_OR_EQUAL_TO:
                    otherQueries.add(QueryBuilders.rangeQuery(column).lte(value));
                    break;
                case NAND:
                    // NAND(A, B, C) = NOT(A AND B AND C)
                    // Build inner AND query, then negate the entire result
                    if (value instanceof List) {
                        List<ColumnPredicate> children = (List<ColumnPredicate>) value;
                        BoolQueryBuilder innerAnd = QueryBuilders.boolQuery();

                        // Build A AND B AND C
                        for (ColumnPredicate child : children) {
                            QueryBuilder childQuery = convertColumnPredicateToQuery(
                                    child.getColumn(),
                                    Collections.singletonList(child));
                            innerAnd.must(childQuery);
                        }

                        // Negate the entire AND: NOT(A AND B AND C)
                        BoolQueryBuilder nandQuery = QueryBuilders.boolQuery().mustNot(innerAnd);
                        otherQueries.add(nandQuery);
                    }
                    break;
                case NOR:
                    // NOR(A, B, C) = NOT(A OR B OR C) = NOT(A) AND NOT(B) AND NOT(C)
                    // Apply mustNot to each child individually
                    if (value instanceof List) {
                        List<ColumnPredicate> children = (List<ColumnPredicate>) value;
                        BoolQueryBuilder norQuery = QueryBuilders.boolQuery();

                        // Build NOT(A) AND NOT(B) AND NOT(C)
                        for (ColumnPredicate child : children) {
                            QueryBuilder childQuery = convertColumnPredicateToQuery(
                                    child.getColumn(),
                                    Collections.singletonList(child));
                            norQuery.mustNot(childQuery);
                        }
                        otherQueries.add(norQuery);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + op);
            }
        }

        // Handle multiple EQUAL values -> terms query (IN clause)
        if (!equalValues.isEmpty()) {
            QueryBuilder equalQuery = equalValues.size() == 1
                    ? QueryBuilders.termQuery(column, equalValues.get(0))
                    : QueryBuilders.termsQuery(column, equalValues);
            otherQueries.add(equalQuery);
        }

        // Combine all queries with AND logic
        if (otherQueries.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }
        if (otherQueries.size() == 1) {
            return otherQueries.get(0);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (QueryBuilder query : otherQueries) {
            boolQuery.must(query);
        }
        return boolQuery;
    }

    /**
     * Converts Substrait value to appropriate Java object
     */
    private static Object convertSubstraitValue(ColumnPredicate pred)
    {
        Object value = pred.getValue();
        if (value instanceof Text) {
            return ((Text) value).toString();
        }
        // Handle datetime conversion if needed
        if (value instanceof Long && pred.getArrowType() instanceof ArrowType.Timestamp) {
            // Elasticsearch expects milliseconds for timestamp
            return ((Long) value) / 1000;
        }
        return value;
    }

    /**
     * Converts a single field constraint into a predicate to use in an Elasticsearch query.
     * @param fieldName The name of the field for the given ValueSet constraint.
     * @param constraint The constraint to apply to the given field.
     * @return A string describing the constraint for pushing down into Elasticsearch.
     */
    private static String getPredicate(String fieldName, ValueSet constraint)
    {
        if (constraint.isNone()) {
            // (NOT _exists_:field)
            return existsPredicate(false, fieldName);
        }

        if (constraint.isAll()) {
            // (_exists_:field)
            return existsPredicate(true, fieldName);
        }

        List<String> predicateParts = new ArrayList<>();

        if (!constraint.isNullAllowed()) {
            // null value should not be included in set of returned values => Include existence predicate.
            predicateParts.add(existsPredicate(true, fieldName));
        }

        if (constraint instanceof EquatableValueSet) {
            EquatableValueSet equatableValueSet = (EquatableValueSet) constraint;
            List<String> singleValues = new ArrayList<>();
            for (int pos = 0; pos < equatableValueSet.getValueBlock().getRowCount(); pos++) {
                singleValues.add(equatableValueSet.getValue(pos).toString());
            }
            if (equatableValueSet.isWhiteList()) {
                // field:(value1 OR value2 OR value3...)
                predicateParts.add(fieldName + ":(" +
                        Strings.collectionToDelimitedString(singleValues, OR_OPER) + ")");
            }
            else {
                // NOT field:(value1 OR value2 OR value3...)
                predicateParts.add(NOT_OPER + fieldName + ":(" +
                        Strings.collectionToDelimitedString(singleValues, OR_OPER) + ")");
            }
        }
        else {
            String rangedPredicate = getPredicateFromRange(fieldName, constraint);
            if (!rangedPredicate.isEmpty()) {
                predicateParts.add(rangedPredicate);
            }
        }

        return predicateParts.isEmpty() ? EMPTY_PREDICATE : Strings.collectionToDelimitedString(predicateParts, AND_OPER);
    }

    /**
     * Converts a range constraint into a predicate to use in an Elasticsearch query.
     * @param fieldName The name of the field for the given ValueSet constraint.
     * @param constraint The constraint to apply to the given field.
     * @return A string describing the constraint for pushing down into Elasticsearch.
     */
    private static String getPredicateFromRange(String fieldName, ValueSet constraint)
    {
        List<String> singleValues = new ArrayList<>();
        List<String> disjuncts = new ArrayList<>();
        for (Range range : constraint.getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                String singleValue = range.getSingleValue().toString();
                if (range.getType() instanceof ArrowType.Date) {
                    // Wrap a single date in quotes, e.g. my-birthday:("2000-11-11T06:57:44.123")
                    singleValues.add("\"" + singleValue + "\"");
                }
                else {
                    singleValues.add(singleValue);
                }
            }
            else {
                String rangeConjuncts;
                if (range.getLow().isLowerUnbounded()) {
                    rangeConjuncts = LOWER_UNBOUNDED_RANGE;
                }
                else {
                    switch (range.getLow().getBound()) {
                        case EXACTLY:
                            rangeConjuncts = LOWER_INCLUSIVE_RANGE + range.getLow().getValue().toString();
                            break;
                        case ABOVE:
                            rangeConjuncts = LOWER_EXCLUSIVE_RANGE + range.getLow().getValue().toString();
                            break;
                        case BELOW:
                            logger.warn("Low Marker should never use BELOW bound: " + range);
                            continue;
                        default:
                            logger.warn("Unhandled bound: " + range.getLow().getBound());
                            continue;
                    }
                }
                rangeConjuncts += RANGE_OPER;
                if (range.getHigh().isUpperUnbounded()) {
                    rangeConjuncts += UPPER_UNBOUNDED_RANGE;
                }
                else {
                    switch (range.getHigh().getBound()) {
                        case EXACTLY:
                            rangeConjuncts += range.getHigh().getValue().toString() + UPPER_INCLUSIVE_RANGE;
                            break;
                        case BELOW:
                            rangeConjuncts += range.getHigh().getValue().toString() + UPPER_EXCLUSIVE_RANGE;
                            break;
                        case ABOVE:
                            logger.warn("High Marker should never use ABOVE bound: " + range);
                            continue;
                        default:
                            logger.warn("Unhandled bound: " + range.getHigh().getBound());
                            continue;
                    }
                }
                disjuncts.add(rangeConjuncts);
            }
        }

        if (!singleValues.isEmpty()) {
            // value1 OR value2 OR value3...
            disjuncts.add(Strings.collectionToDelimitedString(singleValues, OR_OPER));
        }

        if (disjuncts.isEmpty()) {
            // There are no ranges stored.
            return EMPTY_PREDICATE;
        }

        // field:([value1 TO value2] OR value3 OR value4 OR value5...)
        return fieldName + ":(" + Strings.collectionToDelimitedString(disjuncts, OR_OPER) + ")";
    }
}
