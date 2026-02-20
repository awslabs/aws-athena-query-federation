/*- 
 * #%L 
 * athena-postgresql 
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements PostGreSql specific SQL clauses for split.
 *
 * PostGreSql partitions through child tables that can be used in a FROM clause.
 */
public class PostGreSqlQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    public PostGreSqlQueryStringBuilder(final String quoteCharacters, final FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
    }

    @Override
    public PreparedStatement buildSql(
            final Connection jdbcConnection,
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
            throws SQLException
    {
        List<String> charColumns = PostGreSqlMetadataHandler.getCharColumns(jdbcConnection, schema, table);

        String columnNames = tableSchema.getFields().stream()
                .map(field -> {
                    String columnName = field.getName();
                    if (!split.getProperties().containsKey(columnName)) {
                        if (charColumns.contains(columnName)) {
                            return "RTRIM(" + quote(columnName) + ") AS " + quote(columnName);
                        }
                        else {
                            return quote(columnName);
                        }
                    }
                    else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.joining(", "));
        return prepareStatementWithSql(jdbcConnection, catalog, schema, table, tableSchema, constraints, split, columnNames);
    }

    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        if (!Strings.isNullOrEmpty(catalog)) {
            tableName.append(quote(catalog)).append('.');
        }
        if (!Strings.isNullOrEmpty(schema)) {
            tableName.append(quote(schema)).append('.');
        }
        tableName.append(quote(table));

        String partitionSchemaName = split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
        String partitionName = split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME);

        if (PostGreSqlMetadataHandler.ALL_PARTITIONS.equals(partitionSchemaName) || PostGreSqlMetadataHandler.ALL_PARTITIONS.equals(partitionName)) {
            // No partitions
            return String.format(" FROM %s ", tableName);
        }

        return String.format(" FROM %s.%s ", quote(partitionSchemaName), quote(partitionName));
    }

    @Override
    protected List<String> getPartitionWhereClauses(final Split split)
    {
        if (split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME).equals("*")
                && !split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME).equals("*")) {
            return Collections.singletonList(split.getProperty(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME));
        }

        return Collections.emptyList();
    }

    @Override
    protected SqlDialect getSqlDialect()
    {
        return PostgresqlSqlDialect.DEFAULT;
    }
}
