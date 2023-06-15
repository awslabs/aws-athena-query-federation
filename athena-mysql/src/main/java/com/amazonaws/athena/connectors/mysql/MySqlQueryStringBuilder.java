/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements MySql specific SQL clauses for split.
 *
 * MySql provides named partitions which can be used in a FROM clause.
 */
public class MySqlQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    public MySqlQueryStringBuilder(final String quoteCharacters, final FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
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

        String partitionName = split.getProperty(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME);

        if (MySqlMetadataHandler.ALL_PARTITIONS.equals(partitionName)) {
            // No partitions
            return String.format(" FROM %s ", tableName);
        }

        return String.format(" FROM %s PARTITION(%s) ", tableName, partitionName);
    }

    @Override
    protected List<String> getPartitionWhereClauses(final Split split)
    {
        return Collections.emptyList();
    }

    @Override
    protected String extractOrderByClause(Constraints constraints)
    {
        List<OrderByField> orderByClause = constraints.getOrderByClause();
        if (orderByClause == null || orderByClause.size() == 0) {
            return "";
        }
        return "ORDER BY " + orderByClause.stream()
                .flatMap(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String columnSorting = String.format("%s %s", quote(orderByField.getColumnName()), ordering);
                    switch (orderByField.getDirection()) {
                        case ASC_NULLS_FIRST:
                            // In MySQL ASC implies NULLS FIRST
                        case DESC_NULLS_LAST:
                            // In MySQL DESC implies NULLS LAST
                            return Stream.of(columnSorting);
                        case ASC_NULLS_LAST:
                            return Stream.of(String.format("ISNULL(%s) ASC", quote(orderByField.getColumnName())),
                                    columnSorting);
                        case DESC_NULLS_FIRST:
                            return Stream.of(
                                    String.format("ISNULL(%s) DESC", quote(orderByField.getColumnName())),
                                    columnSorting);
                    }
                    throw new UnsupportedOperationException("Unsupported sort order: " + orderByField.getDirection());
                })
                .collect(Collectors.joining(", "));
    }
}
