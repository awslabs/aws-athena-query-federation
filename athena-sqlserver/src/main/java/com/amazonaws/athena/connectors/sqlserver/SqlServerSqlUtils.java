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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.sqlserver.query.SqlServerQueryBuilder;
import com.amazonaws.athena.connectors.sqlserver.query.SqlServerQueryFactory;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

/**
 * Utilities that help with SQL operations using StringTemplate.
 */
public class SqlServerSqlUtils
{
    private static final SqlServerQueryFactory queryFactory = new SqlServerQueryFactory();
    
    private SqlServerSqlUtils()
    {
    }

    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * SQL Server using StringTemplate approach.
     *
     * @param tableName The table name of the table we are querying.
     * @param schema The schema of the table that we are querying.
     * @param constraints The constraints that we want to apply to the query.
     * @param split The split information (for partition support).
     * @param parameterValues List to store parameter values for the prepared statement.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */
    public static String buildSql(TableName tableName, Schema schema, Constraints constraints, Split split, List<TypeAndValue> parameterValues)
    {
        SqlServerQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        
        // SQL Server doesn't use catalog in FROM clause, only schema.table
        // Pass null for catalog to match old behavior
        String sql = queryBuilder
                .withCatalog(null)
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .withOrderByClause(constraints)
                .withLimitClause()
                .build();
        
        // Copy the parameter values from the builder to the provided list
        parameterValues.clear();
        parameterValues.addAll(queryBuilder.getParameterValues());
        
        return sql;
    }
    
    /**
     * Generic method to render any string template with parameters
     *
     * @param templateName The name of the template to render
     * @param params Map of parameter key-value pairs
     * @return The rendered template string
     */
    public static String renderTemplate(String templateName, Map<String, Object> params)
    {
        org.stringtemplate.v4.ST template = queryFactory.getQueryTemplate(templateName);
        
        // Add all parameters from the map
        params.forEach(template::add);
        
        return template.render().trim();
    }
}
