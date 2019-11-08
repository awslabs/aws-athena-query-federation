/*-
 * #%L
 * athena-bigquery
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

package com.amazonaws.athena.connectors.bigquery;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;
import java.util.StringJoiner;

/**
 * Utilities that help with Sql operations.
 */
class BigQuerySqlUtils
{
    private BigQuerySqlUtils()
    {
    }

    /**
     * Builds an SQL statement from the schema, table name, split and contraints that can be executable by
     * BigQuery.
     *
     * @param tableName The table name of the table we are querying.
     * @param schema The schema of the table that we are querying.
     * @param constraints The constraints that we want to apply to the query.
     * @param split The split information to add as a constraint.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */
    static String buildSqlFromSplit(TableName tableName, Schema schema, Constraints constraints, Split split)
    {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");

        StringJoiner sj = new StringJoiner(",");
        if (schema.getFields().isEmpty()) {
            sj.add("*");
        }
        else {
            for (Field field : schema.getFields()) {
                sj.add(field.getName());
            }
        }
        sqlBuilder.append(sj.toString())
            .append(" from ")
            .append(tableName.getSchemaName())
            .append(".")
            .append(tableName.getTableName());

        //Buids Where Clause
        sj = new StringJoiner(") AND (");
        for (Map.Entry<String, ValueSet> summary : constraints.getSummary().entrySet()) {
            final ValueSet value = summary.getValue();
            final String columnName = summary.getKey();
            if (value instanceof EquatableValueSet) {
                if (value.isSingleValue()) {
                    if (value.isNullAllowed()) {
                        sj.add(columnName + " is null");
                    }
                    else {
                        //Check Arrow type to see if we
                        sj.add(columnName + " = " + getValueForWhereClause(columnName, value.getSingleValue(), value.getType()));
                    }
                }
                //TODO:: process multiple values in "IN" clause.
            }
            else if (value instanceof SortedRangeSet) {
                SortedRangeSet sortedRangeSet = (SortedRangeSet) value;
                if (sortedRangeSet.isNone()) {
                    if (sortedRangeSet.isNullAllowed()) {
                        sj.add(columnName + " is null");
                    }
                    //If there is no values and null is not allowed, then that means ignore this valueset.
                    continue;
                }
                Range range = sortedRangeSet.getSpan();
                if (!sortedRangeSet.isNullAllowed() && range.getLow().isLowerUnbounded() && range.getHigh().isUpperUnbounded()) {
                    sj.add(columnName + " is not null");
                    continue;
                }
                if (!range.getLow().isLowerUnbounded() && !range.getLow().isNullValue()) {
                    final String sqlValue = getValueForWhereClause(columnName, range.getLow().getValue(), value.getType());
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            sj.add(columnName + " > " + sqlValue);
                            break;
                        case EXACTLY:
                            sj.add(columnName + " >= " + sqlValue);
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded() && !range.getHigh().isNullValue()) {
                    final String sqlValue = getValueForWhereClause(columnName, range.getHigh().getValue(), value.getType());
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            sj.add(columnName + " <= " + sqlValue);
                            break;
                        case BELOW:
                            sj.add(columnName + " < " + sqlValue);
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
            }
        }
        if (sj.length() > 0) {
            sqlBuilder.append(" WHERE (")
                .append(sj.toString())
                .append(")");
        }

        return sqlBuilder.toString();
    }

    //Gets the representation of a value that can be used in a where clause, ie String values need to be quoted, numeric doesn't.
    private static String getValueForWhereClause(String columnName, Object value, ArrowType arrowType)
    {
        switch (arrowType.getTypeID()) {
            case Int:
            case Decimal:
            case FloatingPoint:
                return value.toString();
            case Bool:
                if ((Boolean) value) {
                    return "true";
                }
                else {
                    return "false";
                }
            case Utf8:
                return "'" + value.toString() + "'";
            case Date:
            case Time:
            case Timestamp:
            case Interval:
            case Binary:
            case FixedSizeBinary:
            case Null:
            case Struct:
            case List:
            case FixedSizeList:
            case Union:
            case NONE:
                throw new UnsupportedOperationException("The Arrow type: " + arrowType.getTypeID().name() + " is currently not supported");
            default:
                throw new IllegalArgumentException("Unknown type has been encountered during range processing: " + columnName +
                    " Field Type: " + arrowType.getTypeID().name());
        }
    }
}
