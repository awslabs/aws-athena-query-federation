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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlServerQueryBuilder
{
    private static final String TEMPLATE_NAME = "select_query";
    private static final String TEMPLATE_FIELD = "builder";
    private static final String SQLSERVER_QUOTE_CHAR = "\"";
    
    private final ST query;
    private List<String> projection;
    private String catalog;
    private String schemaName;
    private String tableName;
    private List<String> conjuncts;
    private String orderByClause;
    private String limitClause;
    private List<TypeAndValue> parameterValues;

    public SqlServerQueryBuilder(ST template)
    {
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
        this.parameterValues = new ArrayList<>();
    }

    static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    public SqlServerQueryBuilder withCatalog(String catalog)
    {
        this.catalog = catalog;
        return this;
    }

    public SqlServerQueryBuilder withProjection(Schema schema, Split split)
    {
        this.projection = schema.getFields().stream()
                .map(Field::getName)
                .filter(name -> !split.getProperties().containsKey(name))
                .map(this::quote)
                .collect(Collectors.toList());
        return this;
    }

    public SqlServerQueryBuilder withTableName(TableName tableName)
    {
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
        return this;
    }

    public SqlServerQueryBuilder withConjuncts(Schema schema, Constraints constraints, Split split)
    {
        this.conjuncts = SqlServerPredicateBuilder.buildConjuncts(
                schema.getFields(), constraints, this.parameterValues, split);
        
        // Add partition clauses if applicable
        List<String> partitionClauses = getPartitionWhereClauses(split);
        if (!partitionClauses.isEmpty()) {
            this.conjuncts.addAll(partitionClauses);
        }
        
        return this;
    }
    
    private List<String> getPartitionWhereClauses(Split split)
    {
        String partitionFunction = split.getProperty("PARTITION_FUNCTION");
        String partitioningColumn = split.getProperty("PARTITIONING_COLUMN");
        String partitionNumber = split.getProperty("partition_number");

        if (partitionFunction != null && partitioningColumn != null && partitionNumber != null && !partitionNumber.equals("0")) {
            // Note: SQL Server $PARTITION function doesn't use quotes around column names
            // Format: $PARTITION.pf(testCol1) = 1 (with leading space to get " AND  $PARTITION" format)
            return java.util.Collections.singletonList(" $PARTITION." + partitionFunction + "(" + partitioningColumn + ") = " + partitionNumber);
        }
        return java.util.Collections.emptyList();
    }

    public SqlServerQueryBuilder withOrderByClause(Constraints constraints)
    {
        this.orderByClause = extractOrderByClause(constraints);
        return this;
    }

    public SqlServerQueryBuilder withLimitClause()
    {
        // SQL Server doesn't support LIMIT clause, return empty string
        this.limitClause = "";
        return this;
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

    public String getLimitClause()
    {
        return limitClause;
    }

    public String build()
    {
        Validate.notNull(tableName, "tableName can not be null.");
        Validate.notNull(projection, "projection can not be null.");

        query.add(TEMPLATE_FIELD, this);
        return query.render().trim();
    }

    private String quote(final String identifier)
    {
        if (identifier == null) {
            return null;
        }
        String name = identifier.replace(SQLSERVER_QUOTE_CHAR, SQLSERVER_QUOTE_CHAR + SQLSERVER_QUOTE_CHAR);
        return SQLSERVER_QUOTE_CHAR + name + SQLSERVER_QUOTE_CHAR;
    }

    private String extractOrderByClause(Constraints constraints)
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
