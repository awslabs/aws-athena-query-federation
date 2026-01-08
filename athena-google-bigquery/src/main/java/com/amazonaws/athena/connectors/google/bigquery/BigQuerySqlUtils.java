/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.google.bigquery.query.BigQueryQueryBuilder;
import com.amazonaws.athena.connectors.google.bigquery.query.BigQueryQueryFactory;
import com.google.cloud.bigquery.QueryParameterValue;
import org.apache.arrow.vector.types.pojo.Schema;
import org.stringtemplate.v4.ST;

import java.util.List;
import java.util.Map;

/**
 * Utilities that help with Sql operations using StringTemplate.
 */
public class BigQuerySqlUtils
{
    private static final BigQueryQueryFactory queryFactory = new BigQueryQueryFactory();
    
    private BigQuerySqlUtils()
    {
    }

    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * BigQuery using StringTemplate approach.
     *
     * @param tableName The table name of the table we are querying.
     * @param schema The schema of the table that we are querying.
     * @param constraints The constraints that we want to apply to the query.
     * @param parameterValues Query parameter values for parameterized query.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */
    public static String buildSql(TableName tableName, Schema schema, Constraints constraints, List<QueryParameterValue> parameterValues)
    {
        BigQueryQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        
        String sql = queryBuilder
                .withTableName(tableName)
                .withProjection(schema)
                .withConjuncts(schema, constraints)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
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
        ST template = queryFactory.getQueryTemplate(templateName);
        
        // Add all parameters from the map
        params.forEach(template::add);
        
        return template.render().trim();
    }
}
