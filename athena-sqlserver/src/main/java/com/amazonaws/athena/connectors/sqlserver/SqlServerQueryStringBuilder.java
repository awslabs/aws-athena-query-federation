/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;
import org.apache.calcite.sql.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.PARTITION_NUMBER;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_END;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_START;

public class SqlServerQueryStringBuilder extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerQueryStringBuilder.class);

    /**
     * Parent {@link JdbcSplitQueryBuilder} requires a quote-character string. Wrapping uses
     * {@link SqlServerConstants#SQLSERVER_QUOTE_START} and {@link SqlServerConstants#SQLSERVER_QUOTE_END}
     * in {@link #quote(String)}, not that parent field.
     */
    public SqlServerQueryStringBuilder(final FederationExpressionParser federationExpressionParser)
    {
        super("", federationExpressionParser);
    }

    /**
     * SQL Server identifiers are quoted with square brackets ({@code [name]}), which are valid
     * regardless of the session {@code QUOTED_IDENTIFIER} setting (unlike double quotes). An embedded
     * closing bracket is escaped by doubling it ({@code ]} becomes {@code ]]}). This matches the bracket
     * quoting used by {@link SqlServerDialect} on the Calcite query-plan path.
     */
    @Override
    protected String quote(String name)
    {
        return SQLSERVER_QUOTE_START + name.replace(SQLSERVER_QUOTE_END, SQLSERVER_QUOTE_END + SQLSERVER_QUOTE_END) + SQLSERVER_QUOTE_END;
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
        return String.format(" FROM %s ", tableName);
    }

    /**
     * In case of partitioned table, custom query will be formed to get specific partition
     * otherwise empty list will be returned
     * @param split
     * @return
     */
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String partitionFunction = split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION);
        String partitioningColumn = split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN);
        String partitionNumber = split.getProperty(PARTITION_NUMBER);

        // $PARTITION.func(col) uses call parentheses; identifiers are bracket-quoted.
        // example: $PARTITION.[myRangePF]([col1]) = 2
        LOGGER.debug("PARTITION_FUNCTION: {}", partitionFunction);
        LOGGER.debug("PARTITIONING_COLUMN: {}", partitioningColumn);

        if (partitionFunction != null && partitioningColumn != null && partitionNumber != null && !partitionNumber.equals("0")) {
            LOGGER.info("Fetching data using Partition");
            // Identifiers cannot be bound with JDBC ?; quote() wraps names in [] and doubles embedded ].
            return Collections.singletonList(" $PARTITION." + quote(partitionFunction) + "(" + quote(partitioningColumn) + ") = " + partitionNumber);
        }
        else {
            LOGGER.info("Fetching data without Partition");
        }
        return Collections.emptyList();
    }

    //Returning empty string as SQLServer does not support LIMIT clause
    @Override
    protected String appendLimitOffset(Split split, Constraints constraints)
    {
        return emptyString;
    }

    @Override
    protected SqlDialect getSqlDialect()
    {
        return SqlServerDialect.DEFAULT;
    }

    @Override
    protected SqlDialect getSqlDialect(boolean catalogCasingFilterUpperCase)
    {
        return new SqlServerDialect(catalogCasingFilterUpperCase);
    }
}
