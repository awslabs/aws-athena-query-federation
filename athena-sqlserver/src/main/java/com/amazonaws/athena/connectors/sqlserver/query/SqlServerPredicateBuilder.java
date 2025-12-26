/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.sqlserver.SqlServerFederationExpressionParser;
import com.amazonaws.athena.connectors.sqlserver.SqlServerSqlUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SqlServerPredicateBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerPredicateBuilder.class);
    private static final String SQLSERVER_QUOTE_CHAR = "\"";
    
    private SqlServerPredicateBuilder() {}

    public static List<String> buildConjuncts(List<Field> columns, Constraints constraints, 
                                               List<TypeAndValue> parameterValues, Split split)
    {
        LOGGER.debug("Inside buildConjuncts(): ");
        List<String> builder = new ArrayList<>();
        
        for (Field column : columns) {
            // Skip partition columns as they are not in the table schema
            if (split.getProperties().containsKey(column.getName())) {
                continue;
            }
            
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    builder.add(toPredicate(column.getName(), valueSet, type, parameterValues));
                }
            }
        }
        
        // Add complex expressions (federation expressions)
        builder.addAll(new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHAR).parseComplexExpressions(columns, constraints, parameterValues));
        return builder;
    }

    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type, 
                                      List<TypeAndValue> parameterValues)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return SqlServerSqlUtils.renderTemplate("null_predicate", Map.of("columnName", quote(columnName), "isNull", true));
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(SqlServerSqlUtils.renderTemplate("null_predicate", Map.of("columnName", quote(columnName), "isNull", true)));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();

            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return SqlServerSqlUtils.renderTemplate("null_predicate", Map.of("columnName", quote(columnName), "isNull", false));
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                parameterValues.add(new TypeAndValue(type, range.getLow().getValue()));
                                rangeConjuncts.add(SqlServerSqlUtils.renderTemplate("comparison_predicate", Map.of("columnName", columnName, "operator", ">")));
                                break;
                            case EXACTLY:
                                parameterValues.add(new TypeAndValue(type, range.getLow().getValue()));
                                rangeConjuncts.add(SqlServerSqlUtils.renderTemplate("comparison_predicate", Map.of("columnName", columnName, "operator", ">=")));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                parameterValues.add(new TypeAndValue(type, range.getHigh().getValue()));
                                rangeConjuncts.add(SqlServerSqlUtils.renderTemplate("comparison_predicate", Map.of("columnName", columnName, "operator", "<=")));
                                break;
                            case BELOW:
                                parameterValues.add(new TypeAndValue(type, range.getHigh().getValue()));
                                rangeConjuncts.add(SqlServerSqlUtils.renderTemplate("comparison_predicate", Map.of("columnName", columnName, "operator", "<")));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add(SqlServerSqlUtils.renderTemplate("range_predicate", Map.of("conjuncts", rangeConjuncts)));
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                parameterValues.add(new TypeAndValue(type, Iterables.getOnlyElement(singleValues)));
                disjuncts.add(SqlServerSqlUtils.renderTemplate("comparison_predicate", Map.of("columnName", columnName, "operator", "=")));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    parameterValues.add(new TypeAndValue(type, value));
                }
                List<String> placeholders = Collections.nCopies(singleValues.size(), "?");
                disjuncts.add(SqlServerSqlUtils.renderTemplate("in_predicate", Map.of("columnName", columnName, "counts", placeholders)));
            }
        }

        return SqlServerSqlUtils.renderTemplate("or_predicate", Map.of("disjuncts", disjuncts));
    }

    private static String quote(String name)
    {
        if (name == null) {
            return null;
        }
        String quoted = name.replace(SQLSERVER_QUOTE_CHAR, SQLSERVER_QUOTE_CHAR + SQLSERVER_QUOTE_CHAR);
        return SQLSERVER_QUOTE_CHAR + quoted + SQLSERVER_QUOTE_CHAR;
    }
}
