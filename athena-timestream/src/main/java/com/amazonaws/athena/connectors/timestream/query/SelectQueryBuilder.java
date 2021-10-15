/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.query;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.stringtemplate.v4.ST;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to build a Timestream query which can select data from a table in a specific database directly or via a
 * simplified view. The view is said to be simplified because it must be defined as a single 'select' statement itself
 * which this class will turn into a WITH (select) as t1 to inject the requested query.
 */
public class SelectQueryBuilder
{
    //The special table property used to include 'TimeStream view' information as part of a table's schema.
    private static final String TEMPLATE_NAME = "select_query";
    private static final String TEMPLATE_FIELD = "builder";
    private final String viewTextPropertyName;
    private final ST query;
    private List<String> projection;
    private String viewText;
    private List<String> conjucts;
    private String databaseName;
    private String tableName;

    public SelectQueryBuilder(ST template, String viewTextPropertyName)
    {
        this.viewTextPropertyName = viewTextPropertyName;
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
    }

    static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    public List<String> getProjection()
    {
        return projection;
    }

    public SelectQueryBuilder withProjection(Schema schema)
    {
        this.projection = schema.getFields().stream().map(next -> next.getName()).collect(Collectors.toList());
        this.viewText = schema.getCustomMetadata().get(viewTextPropertyName);
        return this;
    }

    public String getViewText()
    {
        return viewText;
    }

    public List<String> getConjucts()
    {
        return conjucts;
    }

    public SelectQueryBuilder withConjucts(Constraints constraints)
    {
        this.conjucts = PredicateBuilder.buildConjucts(constraints);
        return this;
    }

    public String getTableName()
    {
        return tableName;
    }

    public SelectQueryBuilder withTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public SelectQueryBuilder withDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
        return this;
    }

    public String build()
    {
        Validate.notNull(databaseName, "tableName can not be null.");
        Validate.notNull(tableName, "tableName can not be null.");
        Validate.notNull(projection, "projection can not be null.");
        Validate.notEmpty(projection, "projection can not be empty.");

        query.add(TEMPLATE_FIELD, this);
        return query.render().trim();
    }
}
