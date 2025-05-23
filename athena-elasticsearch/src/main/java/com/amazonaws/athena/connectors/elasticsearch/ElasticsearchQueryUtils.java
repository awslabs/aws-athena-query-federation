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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
     * @param constraintSummary is a map containing the constraints used to form the predicate for predicate push-down.
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
