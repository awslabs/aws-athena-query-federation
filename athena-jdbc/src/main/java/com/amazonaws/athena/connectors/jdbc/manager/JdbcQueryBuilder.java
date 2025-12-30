/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class JdbcQueryBuilder<T extends JdbcQueryBuilder<T>>
{
    protected static final String TEMPLATE_NAME = "select_query";
    protected static final String TEMPLATE_FIELD = "builder";

    protected final ST query;
    protected final String quoteChar;
    protected List<String> projection;
    protected String catalog;
    protected String schemaName;
    protected String tableName;
    protected List<String> conjuncts;
    protected String orderByClause;
    protected String limitClause;
    protected final List<TypeAndValue> parameterValues;

    public JdbcQueryBuilder(ST template, String quoteChar)
    {
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
        this.quoteChar = quoteChar;
        this.parameterValues = new ArrayList<>();
    }

    public static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    public T withCatalog(String catalog)
    {
        this.catalog = catalog;
        return (T) this;
    }

    public T withTableName(TableName tableName)
    {
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
        return (T) this;
    }

    public T withOrderByClause(Constraints constraints)
    {
        this.orderByClause = extractOrderByClause(constraints);
        return (T) this;
    }

    public T withProjection(Schema schema, Split split)
    {
        this.projection = schema.getFields().stream()
                .map(Field::getName)
                .filter(name -> !split.getProperties().containsKey(name))
                .map(this::transformColumnForProjection)
                .collect(Collectors.toList());
        return (T) this;
    }

    /**
     * Hook method for transforming column names in the projection.
     * Subclasses can override this to apply connector-specific transformations
     * (e.g., RTRIM for CHAR columns in PostgreSQL).
     *
     * @param columnName The column name to transform
     * @return The transformed column expression (default: just quoted column name)
     */
    protected String transformColumnForProjection(String columnName)
    {
        return quote(columnName);
    }

    public List<TypeAndValue> getParameterValues()
    {
        return parameterValues;
    }

    // Getter methods for StringTemplate access
    public List<String> getProjection()
    {
        return projection;
    }

    public String getCatalog()
    {
        return catalog != null ? quote(catalog) : null;
    }

    public String getSchemaName()
    {
        return schemaName != null ? quote(schemaName) : null;
    }

    public String getTableName()
    {
        return quote(tableName);
    }

    public List<String> getConjuncts()
    {
        return conjuncts;
    }

    public String getOrderByClause()
    {
        return orderByClause;
    }
    
    public T withConjuncts(Schema schema, Constraints constraints, Split split)
    {
        JdbcPredicateBuilder predicateBuilder = createPredicateBuilder();
        if (predicateBuilder != null) {
            this.conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, this.parameterValues, split);
        }
        else {
            this.conjuncts = new ArrayList<>();
        }
        
        // Add partition clauses if applicable
        List<String> partitionClauses = getPartitionWhereClauses(split);
        if (!partitionClauses.isEmpty()) {
            this.conjuncts.addAll(partitionClauses);
        }
        return (T) this;
    }
    
    protected abstract JdbcPredicateBuilder createPredicateBuilder();
    
    protected List<String> getPartitionWhereClauses(Split split)
    {
        return new ArrayList<>();
    }

    public String getLimitClause()
    {
        return limitClause;
    }
    
    public T withLimitClause(Constraints constraints)
    {
        if (constraints.getLimit() > 0) {
            this.limitClause = " LIMIT " + constraints.getLimit();
        }
        return (T) this;
    }

    public String build()
    {
        Validate.notNull(tableName, "tableName can not be null.");
        Validate.notNull(projection, "projection can not be null.");

        query.add(TEMPLATE_FIELD, this);
        return query.render().trim();
    }

    protected String quote(final String identifier)
    {
        return JdbcSqlUtils.quoteIdentifier(identifier, quoteChar);
    }

    /**
     * Extracts ORDER BY clause from constraints.
     * Subclasses can override this for database-specific syntax.
     * Default implementation uses standard SQL NULLS FIRST/NULLS LAST.
     *
     * @param constraints The query constraints
     * @return The ORDER BY clause string, or empty string if no ordering
     */
    protected String extractOrderByClause(Constraints constraints)
    {
        List<OrderByField> orderByFields = constraints.getOrderByClause();
        if (orderByFields == null || orderByFields.isEmpty()) {
            return "";
        }
        return "ORDER BY " + orderByFields.stream()
                .map(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String nullsHandling = orderByField.getDirection().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                    return quote(orderByField.getColumnName()) + " " + ordering + " " + nullsHandling;
                })
                .collect(Collectors.joining(", "));
    }
}
