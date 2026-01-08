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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.sqlserver.SqlServerSqlUtils;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;

public class SqlServerQueryBuilder extends JdbcQueryBuilder<SqlServerQueryBuilder>
{
    public SqlServerQueryBuilder(ST template)
    {
        super(template, SQLSERVER_QUOTE_CHARACTER);
    }
    
    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new SqlServerPredicateBuilder();
    }
    
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String partitionFunction = split.getProperty("PARTITION_FUNCTION");
        String partitioningColumn = split.getProperty("PARTITIONING_COLUMN");
        String partitionNumber = split.getProperty("partition_number");

        if (partitionFunction != null && partitioningColumn != null && partitionNumber != null && !partitionNumber.equals("0")) {
            // Note: SQL Server $PARTITION function doesn't use quotes around column names
            // Format: $PARTITION.pf(testCol1) = 1
            // We prepend a space because JdbcSqlUtils.renderTemplate() trims the result
            String partitionClause = JdbcSqlUtils.renderTemplate(
                    SqlServerSqlUtils.getQueryFactory(),
                    "partition_clause",
                    Map.of("partitionFunction", partitionFunction,
                            "partitioningColumn", partitioningColumn,
                            "partitionNumber", partitionNumber));
            return Collections.singletonList(" " + partitionClause);
        }
        return Collections.emptyList();
    }
    
    @Override
    public SqlServerQueryBuilder withLimitClause(Constraints constraints)
    {
        // SQL Server doesn't support LIMIT clause, return empty string
        this.limitClause = "";
        return this;
    }
}
