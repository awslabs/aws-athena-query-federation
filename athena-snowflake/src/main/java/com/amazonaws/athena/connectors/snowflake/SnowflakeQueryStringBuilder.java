/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.ALL_PARTITIONS;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.DOUBLE_QUOTE_CHAR;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.PARTITION_BUCKET_PATTERN;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.QUOTED_IDENTIFIER_LIST_PATTERN;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SINGLE_QUOTE_CHAR;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements MySql specific SQL clauses for split.
 *
 * MySql provides named partitions which can be used in a FROM clause.
 */
public class SnowflakeQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeQueryStringBuilder.class);

    public SnowflakeQueryStringBuilder(final String quoteCharacters, final FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
    }

    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        if (!Strings.isNullOrEmpty(schema)) {
            tableName.append(quote(schema)).append('.');
        }
        tableName.append(quote(table));
        return String.format(" FROM %s ", tableName);
    }

    /**
     * Turns the split's partition value back into a WHERE predicate so the split only reads its own share of
     * the table. This is the one split hook honored on every SQL generation path, including the Substrait /
     * Calcite path and queries with a pushed down LIMIT, which is why the scoping lives here rather than in
     * {@link #appendLimitOffset(Split)}.
     */
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String partitionVal = split == null ? null : split.getProperty(BLOCK_PARTITION_COLUMN_NAME);
        // Nothing to scope to: single partition tables and views, plus the S3 export and query passthrough
        // paths, all read the table in one piece.
        if (partitionVal == null || ALL_PARTITIONS.equals(partitionVal)) {
            LOGGER.info("Split execution: partitionScope=WHOLE_TABLE, partitionValue={} (no partition predicate added, "
                    + "this split reads the table in one piece)", partitionVal);
            return Collections.emptyList();
        }

        Matcher matcher = PARTITION_BUCKET_PATTERN.matcher(partitionVal);
        if (!matcher.matches()) {
            // Returning no predicate here would give every split the whole table and duplicate the result
            // set once per split, so fail the query instead of silently returning wrong data.
            throw new AthenaConnectorException("Unrecognized Snowflake partition value: " + partitionVal,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        String bucket = matcher.group(1);
        String bucketCount = matcher.group(2);
        String partitionKey = matcher.group(3);
        // The key is interpolated into SQL, so confirm it is the quoted identifier list getPrimaryKey produces.
        if (!QUOTED_IDENTIFIER_LIST_PATTERN.matcher(partitionKey).matches()) {
            throw new AthenaConnectorException("Snowflake partition key is not a quoted identifier list: " + partitionKey,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        // HASH is deterministic and defined for every input in Snowflake, NULLs included, so each row lands in
        // exactly one bucket. The buckets therefore cover the table without relying on a row order or count.
        String predicate = String.format("MOD(ABS(HASH(%s)), %s) = %s", partitionKey, bucketCount, bucket);
        // One line per split showing which slice of the table this invocation reads. Across the concurrent
        // split invocations these lines show the buckets 0..n-1 being read in parallel, and a missing bucket
        // points at the split that failed. Only the hash key column names appear, never row values.
        LOGGER.info("Split execution: partitionScope=HASH_BUCKET, bucket={}, ofBuckets={}, hashKey={}, predicate={}",
                bucket, bucketCount, partitionKey, predicate);
        return Collections.singletonList(predicate);
    }

    public String getBaseExportSQLString(
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints)
            throws SQLException
    {
        String columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(name -> !name.equalsIgnoreCase(BLOCK_PARTITION_COLUMN_NAME))
                .map(this::quote)
                .collect(Collectors.joining(", "));

        // We compute the base Export SQL String at GetSplit stage, at the time we don't have column projection hence getting everything.
        if (columnNames.isEmpty()) {
            columnNames = "*";
        }

        List<TypeAndValue> accumulator = new ArrayList<>();
        String sqlBaseString = this.buildSQLStringLiteral(catalog, schema, table, tableSchema, constraints, new Split(null, null, Map.of()), columnNames, accumulator);

        sqlBaseString = expandSql(sqlBaseString, accumulator);
        LOGGER.info("Expanded Generated SQL : {}", sqlBaseString);
        return sqlBaseString;
    }

    @VisibleForTesting
    String expandSql(String sql, List<TypeAndValue> accumulator)
    {
        if (Strings.isNullOrEmpty(sql)) {
            return null;
        }
        
        for (TypeAndValue typeAndValue : accumulator) {
            String sqlLiteral;
            
            if (typeAndValue.getValue() == null) {
                sqlLiteral = "NULL";
            }
            else {
                Types.MinorType minorType = Types.getMinorTypeForArrowType(typeAndValue.getType());
                
                switch (minorType) {
                    case DATEDAY:
                        long days = ((Number) typeAndValue.getValue()).longValue();
                        sqlLiteral = "DATE " + singleQuote(LocalDate.ofEpochDay(days).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                        break;
                    case DATEMILLI:
                        long millis = ((Number) typeAndValue.getValue()).longValue();
                        sqlLiteral = "TIMESTAMP " + singleQuote(java.time.Instant.ofEpochMilli(millis).toString());
                        break;
                    case TIMESTAMPMILLITZ:
                    case TIMESTAMPMICROTZ:
                        if (typeAndValue.getValue() instanceof java.sql.Timestamp) {
                            sqlLiteral = "TIMESTAMP " + singleQuote(typeAndValue.getValue().toString());
                        }
                        else {
                            long tsMillis = ((Number) typeAndValue.getValue()).longValue();
                            sqlLiteral = "TIMESTAMP " + singleQuote(java.time.Instant.ofEpochMilli(tsMillis).toString());
                        }
                        break;
                    case INT:
                    case SMALLINT:
                    case TINYINT:
                    case BIGINT:
                    case FLOAT4:
                    case FLOAT8:
                    case DECIMAL:
                        sqlLiteral = typeAndValue.getValue().toString();
                        break;
                    default:
                        sqlLiteral = singleQuote(typeAndValue.getValue().toString());
                        break;
                }
            }
            
            sql = sql.replaceFirst("\\?", sqlLiteral);
        }
        return sql;
    }

    protected String quote(String name)
    {
        name = name.replace(DOUBLE_QUOTE_CHAR, DOUBLE_QUOTE_CHAR + DOUBLE_QUOTE_CHAR);
        return DOUBLE_QUOTE_CHAR + name + DOUBLE_QUOTE_CHAR;
    }

    protected String singleQuote(String name)
    {
        name = name.replace(SINGLE_QUOTE_CHAR, SINGLE_QUOTE_CHAR + SINGLE_QUOTE_CHAR);
        return SINGLE_QUOTE_CHAR + name + SINGLE_QUOTE_CHAR;
    }

    @Override
    protected SqlDialect getSqlDialect()
    {
       return SnowflakeSqlDialect.DEFAULT;
    }
    
    @Override
    protected SqlDialect getSqlDialect(boolean catalogCasingFilterUpperCase)
    {
        return new SnowflakeDialect(catalogCasingFilterUpperCase);
    }
}
