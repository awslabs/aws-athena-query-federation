/*-
 * #%L
 * athena-MSK
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
package com.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
/**
 * Utilities that help with Sql operations.
 */
public class AmazonMskSqlUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskSqlUtils.class);

    private static final String QUOTE_CHAR = "'";

    private AmazonMskSqlUtils()
    {
    }
    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * BigQuery.
     *
     * @param tableName       The table name of the table we are querying.
     * @param schema          The schema of the table that we are querying.
     * @param constraints     The constraints that we want to apply to the query.
     * @param split           The split information to add as a constraint.
     * @param parameterValues Query parameter values for parameterized query.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */

    public static String buildSqlFromSplit(TableName tableName,
                                           Schema schema,
                                           Constraints constraints,
                                           Split split,
                                           List parameterValues)
    {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");
        StringJoiner sj = new StringJoiner(",");

        if (schema.getFields().isEmpty()) {
            sj.add("null");
        }
        else {
            for (Field field : schema.getFields()) {
                sj.add(field.getName());
            }
        }
        sqlBuilder.append(sj)
                .append(" from ")
                .append(tableName.getSchemaName())
                .append(".")
                .append(tableName.getTableName());

        LOGGER.debug(" STEP 5.0 " + sqlBuilder);
        LOGGER.debug(" STEP 5.1 " + sj);

        List<String> clauses = toConjuncts(schema.getFields(), constraints, split.getProperties(), parameterValues);

        LOGGER.debug(" STEP 5.0.0 " + clauses);

        if (!clauses.isEmpty()) {
            sqlBuilder.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        LOGGER.debug(" STEP 5.2 " + sqlBuilder);

        return sqlBuilder.toString();
    }

    /**
     * Builds an SQL where clause from the schema
     *
     * @param columns         List of columns on which operation is performed
     * @param constraints     Conditions in SQL Query
     * @param partitionSplit  Partition logic
     * @param parameterValues Values that are passed in where clause
     * @return Where clause of the SQL Query.
     */
    private static List<String> toConjuncts(List<Field> columns,
                                            Constraints constraints,
                                            Map<String, String> partitionSplit,
                                            List parameterValues)
    {
        LOGGER.debug(" --- STEP 5.3 : paramvalues : " + parameterValues.toString());

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                continue; // Ignore constraints on partition name as RDBMS does not contain these as columns. Presto will filter these values.
            }
            ArrowType type = column.getType();
            LOGGER.debug(" -- STEP 5.4 ArrowType: " + type);

            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    LOGGER.debug(" -- STEP 5.5 " + valueSet);
                    try {
                        builder.add(toPredicate(column.getName(), valueSet, type, parameterValues));
                    }
                    catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return builder.build();
    }

    /**
     * Integrate the predicates from the schema
     *
     * @param columnName      Names of the columns
     * @param operator        Operators that are used in the query
     * @param value           Value that is used against the predicate
     * @param type            ArrowType of the column
     * @param parameterValues Contains the list of the parameter value
     * @return conditional part of the query that have conditional operators
     */
    private static String toPredicate(String columnName, String operator, Object value, ArrowType type,
                                      List parameterValues)
    {
        Object val = null;
        try {
            val = getValueForWhereClause(columnName, value, type);
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        parameterValues.add(val);
        if (("Utf8").equalsIgnoreCase(type.getTypeID().toString())) {
            return columnName + " " + operator + quote(val.toString());
        }
        return columnName + " " + operator + val;
    }

    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List parameterValues) throws ParseException
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return String.format("(%s IS NULL)", columnName);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(String.format("(%s IS NULL)", columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return String.format("(%s IS NOT NULL)", columnName);
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
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, parameterValues));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, parameterValues));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, parameterValues));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, parameterValues));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    parameterValues.add(getValueForWhereClause(columnName, value, type));
                }
                String values = Joiner.on(",").join(Collections.nCopies(singleValues.size(), "?"));
                disjuncts.add(columnName + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    /**
     * Gets the representation of a value that can be used in a where clause, ie String values need to be quoted, numeric doesn't.
     * @param columnName
     * @param value
     * @param arrowType
     * @return
     * @throws ParseException
     */

    private static Object getValueForWhereClause(String columnName, Object value, ArrowType arrowType) throws ParseException
    {
        LOGGER.debug(" STEP 6.0 ArrowType: " + arrowType.getTypeID());
        switch (arrowType.getTypeID()) {
            case Int:
                return Integer.valueOf(value.toString());
            case FloatingPoint:
            case Decimal:
                return Double.parseDouble(value.toString());
            case Bool:
                return Boolean.valueOf(value.toString().toLowerCase());
            case Date:
            case Timestamp:
                return ArrowTypeConverter.convertToDate(String.valueOf(value));
            case Utf8:
            case NONE:
            case Interval:
            case Binary:
            case FixedSizeBinary:
                return value.toString();
            case Null:
                return null;
            case Struct:
            case List:
            case FixedSizeList:
            case Union:
            default:
                throw new IllegalArgumentException("Unknown type has been encountered during range processing: " + columnName +
                        " Field Type: " + arrowType.getTypeID().name());
        }
    }

    /**
     * Appends the single quotes on both side of the vale
     *
     * @param identifier Value of the string column
     * @return 'identifier'
     */
    private static String quote(final String identifier)
    {
        return QUOTE_CHAR + identifier + QUOTE_CHAR;
    }
}
