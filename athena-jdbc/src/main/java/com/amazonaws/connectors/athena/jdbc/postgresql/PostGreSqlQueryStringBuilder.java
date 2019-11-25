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
package com.amazonaws.connectors.athena.jdbc.postgresql;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements PostGreSql specific SQL clauses for split.
 *
 * PostGreSql partitions through child tables that can be used in a FROM clause.
 */
public class PostGreSqlQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    PostGreSqlQueryStringBuilder(final String quoteCharacters)
    {
        super(quoteCharacters);
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
}
