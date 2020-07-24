/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.connectors.athena.vertica;

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
/**
 * Query builder for database table split.
 */
public class VerticaJdbcSplitQueryBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerticaJdbcSplitQueryBuilder.class);
    private final String quoteCharacters;

    /**
     * @param quoteCharacters database quote character for enclosing identifiers.
     */
      public VerticaJdbcSplitQueryBuilder(String quoteCharacters) {
        this.quoteCharacters = Validate.notBlank(quoteCharacters, "quoteCharacters must not be blank");
    }

    /**
     * Common logic to build Split SQL including constraints translated in where clause.
     *
     * @param s3ExportBucket S3 bucket where results of the query will be exported
     * @param schema         table schema name.
     * @param table          table name.
     * @param tableSchema    table schema (column and type information).
     * @param constraints    constraints passed by Athena to push down.
     * @param split          table split.
     * @return sqlStatement  SQL statement
     */
    public String buildSql(
            final String s3ExportBucket,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final String queryID)

    {
        ST exportSqlST = new ST("EXPORT TO PARQUET(" +
                "directory = 's3://<s3ExportBucket>/<queryID>'," +
                " Compression='snappy', fileSizeMB=1000) AS ");

        exportSqlST.add("s3ExportBucket", s3ExportBucket);
        exportSqlST.add("queryID", queryID.replace("-",""));


        StringBuilder sql = new StringBuilder();
        //get the column names to be queried
        String columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .map(this::quote)
                .collect(Collectors.joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columnNames.isEmpty()) {
            sql.append("null");
        }
        sql.append(getFromClauseWithSplit(schema, table));

        HashMap<String,TypeAndValue> accumulator = new HashMap<>();
        List<String> clauses = toConjuncts(tableSchema.getFields(), constraints, accumulator);

        if (!clauses.isEmpty())
        {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        //Using StringTemplates to fill in the values of constraints
        ST sqlTemplate = new ST(sql.toString());

        for (Map.Entry<String,TypeAndValue> entry : accumulator.entrySet())
        {
            TypeAndValue typeAndValue = entry.getValue();
            Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(typeAndValue.getType());
            String colName = entry.getKey();

            switch (minorTypeForArrowType)
            {
                case BIT:
                    int value = ((boolean) typeAndValue.getValue()) ? 1 : 0;
                    sqlTemplate.add(colName, value);
                    break;
                case TINYINT:
                    sqlTemplate.add(colName, Byte.parseByte(typeAndValue.getValue().toString()));
                    break;
                case SMALLINT:
                    sqlTemplate.add(colName,Short.parseShort(typeAndValue.getValue().toString()));
                    break;
                case INT:
                    sqlTemplate.add(colName, Integer.parseInt(typeAndValue.getValue().toString()));
                    break;
                case BIGINT:
                    sqlTemplate.add(colName,Long.parseLong(typeAndValue.getValue().toString()));
                    break;
                case FLOAT4:
                    sqlTemplate.add(colName,Float.parseFloat(typeAndValue.getValue().toString()));
                    break;
                case FLOAT8:
                    sqlTemplate.add(colName,Double.parseDouble(typeAndValue.getValue().toString()));
                    break;
                case DECIMAL:
                    sqlTemplate.add(colName, new BigDecimal(typeAndValue.getValue().toString()));
                    break;
                case DATEDAY:
                    sqlTemplate.add(colName, (int) LocalDate.parse(typeAndValue.getValue().toString()).toEpochDay());
                    break;
                case DATEMILLI:
                    sqlTemplate.add(colName, LocalDateTime.parse(typeAndValue.getValue().toString()).atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli());
                    break;
                case VARCHAR:
                    String val = "'" + typeAndValue.getValue() + "'";
                    sqlTemplate.add(colName, val);
                    break;
                case VARBINARY:
                    sqlTemplate.add(colName, typeAndValue.toString().getBytes());
                    break;

                default:
                    throw new UnsupportedOperationException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), minorTypeForArrowType));
            }
        }


        ST completeSqlST = new ST("<exportSqlST> <userSqlST>");
        completeSqlST.add("exportSqlST", exportSqlST.render());
        completeSqlST.add("userSqlST", sqlTemplate.render());
        LOGGER.info(completeSqlST.render());

        return completeSqlST.render();
    }

    protected String buildSetSessionSql(String awsAccessId, String awsSecretKey)
    {
        ST authST = new ST("ALTER SESSION SET AWSAuth='<access_key>:<secret_key>'");
        authST.add("access_key", awsAccessId);
        authST.add("secret_key", awsSecretKey);
        return authST.render();
    }

    protected String buildSetSessionSql(String awsRegion)
    {
        ST regionST=  new ST("ALTER SESSION SET AWSRegion='<defaultRegion>'") ;
        regionST.add("defaultRegion", awsRegion);
        return regionST.render();
    }

    protected String getFromClauseWithSplit(String schema, String table)
    {
        StringBuilder tableName = new StringBuilder();
        if (!Strings.isNullOrEmpty(schema))
        {
            tableName.append(quote(schema)).append('.');
        }
        tableName.append(quote(table));

        return String.format(" FROM %s ", tableName);
    }


    private List<String> toConjuncts(List<Field> columns, Constraints constraints, HashMap<String, TypeAndValue> accumulator)
    {
        List<String> conjuncts = new ArrayList<>();
        for (Field column : columns)
        {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty())
            {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null)
                {
                    conjuncts.add(toPredicate(column.getName(), valueSet, type, accumulator));
                }
            }
        }
        return conjuncts;
    }

    private String toPredicate(String columnName, ValueSet valueSet, ArrowType type, HashMap<String, TypeAndValue> accumulator)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

        if (valueSet instanceof SortedRangeSet){
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
                } else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, accumulator));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, accumulator));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, accumulator));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, accumulator));
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
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
            } else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    accumulator.put(columnName, new TypeAndValue(type, value));
                }
                String values = Joiner.on(",").join(Collections.nCopies(singleValues.size(), "<"+columnName+">"));
                disjuncts.add(quote(columnName) + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, ArrowType type,
                               HashMap<String, TypeAndValue> accumulator)
    {
        accumulator.put(columnName, new TypeAndValue(type, value));
        return quote(columnName) + " "+ operator + " <"+columnName+"> ";
    }

    protected String quote(String name)
    {
        name = name.replace(quoteCharacters, quoteCharacters + quoteCharacters);
        return quoteCharacters + name + quoteCharacters;
    }

    static class TypeAndValue
    {
        private final ArrowType type;
        private final Object value;

        TypeAndValue(ArrowType type, Object value)
        {
            this.type = Validate.notNull(type, "type is null");
            this.value = Validate.notNull(value, "value is null");
        }

        ArrowType getType()
        {
            return type;
        }

        Object getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return "TypeAndValue{" +
                    "type=" + type +
                    ", value=" + value +
                    '}';
        }
    }

}
