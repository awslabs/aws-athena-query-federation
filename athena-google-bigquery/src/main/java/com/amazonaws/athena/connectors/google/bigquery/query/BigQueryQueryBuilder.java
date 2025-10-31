/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019-2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery.query;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.google.cloud.bigquery.QueryParameterValue;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryQueryBuilder
{
    private static final String TEMPLATE_NAME = "select_query";
    private static final String TEMPLATE_FIELD = "builder";
    private static final String BIGQUERY_QUOTE_CHAR = "`";
    
    private final ST query;
    private List<String> projection;
    private String schemaName;
    private String tableName;
    private List<String> conjuncts;
    private String orderByClause;
    private String limitClause;
    private List<QueryParameterValue> parameterValues;

    public BigQueryQueryBuilder(ST template)
    {
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
        this.parameterValues = new ArrayList<>();
    }

    static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    public BigQueryQueryBuilder withProjection(Schema schema)
    {
        this.projection = schema.getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toList());
        return this;
    }

    public BigQueryQueryBuilder withTableName(TableName tableName)
    {
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
        return this;
    }

    public BigQueryQueryBuilder withConjuncts(Schema schema, Constraints constraints)
    {
        this.conjuncts = BigQueryPredicateBuilder.buildConjuncts(schema.getFields(), constraints, this.parameterValues);
        return this;
    }

    public BigQueryQueryBuilder withOrderByClause(Constraints constraints)
    {
        this.orderByClause = extractOrderByClause(constraints);
        return this;
    }

    public BigQueryQueryBuilder withLimitClause(Constraints constraints)
    {
        if (constraints.getLimit() > 0) {
            this.limitClause = "LIMIT " + constraints.getLimit();
        }
        return this;
    }

    public List<QueryParameterValue> getParameterValues()
    {
        return parameterValues;
    }

    // Getter methods for StringTemplate access
    public List<String> getProjection()
    {
        return projection;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<String> getConjuncts()
    {
        return conjuncts;
    }

    public String getOrderByClause()
    {
        return orderByClause;
    }

    public String getLimitClause()
    {
        return limitClause;
    }

    public String build()
    {
        Validate.notNull(schemaName, "schemaName can not be null.");
        Validate.notNull(tableName, "tableName can not be null.");
        Validate.notNull(projection, "projection can not be null.");

        query.add(TEMPLATE_FIELD, this);
        return query.render().trim();
    }

    private static String quote(final String identifier)
    {
        return BIGQUERY_QUOTE_CHAR + identifier + BIGQUERY_QUOTE_CHAR;
    }

    private static String extractOrderByClause(Constraints constraints)
    {
        List<OrderByField> orderByClause = constraints.getOrderByClause();
        if (orderByClause == null || orderByClause.isEmpty()) {
            return "";
        }
        return "ORDER BY " + orderByClause.stream()
                .map(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String nullsHandling = orderByField.getDirection().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                    return quote(orderByField.getColumnName()) + " " + ordering + " " + nullsHandling;
                })
                .collect(Collectors.joining(", "));
    }
}
