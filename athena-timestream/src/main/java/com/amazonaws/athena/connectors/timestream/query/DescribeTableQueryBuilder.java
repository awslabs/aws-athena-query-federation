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

import org.apache.commons.lang3.Validate;
import org.stringtemplate.v4.ST;

/**
 * Used to build a Timestream query which can describe a table in a specific database. This can be used
 * to obtain key schema information about the columns in this table and their respective types. This class uses
 * StringTemplate to separate formatting from parameter replacement.
 */
public class DescribeTableQueryBuilder
{
    private static final String TEMPLATE_NAME = "describe_table";
    private static final String TEMPLATE_FIELD = "builder";
    private final ST query;
    private String databaseName;
    private String tableName;

    public DescribeTableQueryBuilder(ST template)
    {
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
    }

    static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    public DescribeTableQueryBuilder withDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
        return this;
    }

    public DescribeTableQueryBuilder withTablename(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String build()
    {
        Validate.notNull(databaseName, "databaseName can not be null.");
        Validate.notEmpty(databaseName, "databaseName can not be empty.");
        Validate.notNull(tableName, "tableName can not be null.");
        Validate.notEmpty(tableName, "tableName can not be empty.");

        query.add(TEMPLATE_FIELD, this);
        return query.render();
    }
}
