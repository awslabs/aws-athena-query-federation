
/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements MySql specific SQL clauses for split.
 *
 * MySql provides named partitions which can be used in a FROM clause.
 */
public class SnowflakeQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    private static final String EMPTY_STRING = "";

    public SnowflakeQueryStringBuilder(final String quoteCharacters)
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
        return String.format(" FROM %s ", tableName);
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        return Collections.emptyList();
    }

    /**
     * logic to apply limits in query, if partition value does not contain "-", no limit and offset condition
     * to be applied, else apply limits and offset as per "p-limit-3000-offset-0" pattern.
     * @param split
     * @return
     */
    @Override
    protected String appendLimitOffset(Split split)
    {
        String xLimit = "";
        String xOffset = "";
        String partitionVal = split.getProperty(split.getProperties().keySet().iterator().next()); //p-limit-3000-offset-0
        if (!partitionVal.contains("-")) {
            return EMPTY_STRING;
        }
        else {
            String[] arr = partitionVal.split("-");
            xLimit = arr[2];
            xOffset = arr[4];
        }
        return " limit " + xLimit + " offset " + xOffset;
    }
}
