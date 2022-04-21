/*-
 * #%L
 * athena-cloudera-hive
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web services
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;

public class HiveQueryStringBuilder extends JdbcSplitQueryBuilder
{
    public HiveQueryStringBuilder(String quoteCharacters)
    {
        super(quoteCharacters);
    }

    @Override
    protected String getFromClauseWithSplit(String catalogName, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        if (!Strings.isNullOrEmpty(catalogName)) {
            tableName.append(catalogName).append('.');
        }
        if (!Strings.isNullOrEmpty(schema)) {
            tableName.append(schema).append('.');
        }
        tableName.append(table);
        return String.format(" FROM %s ", tableName);
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        if (!split.getProperty(HiveConstants.BLOCK_PARTITION_COLUMN_NAME).equals("*")) {
            return Collections.singletonList(split.getProperty(HiveConstants.BLOCK_PARTITION_COLUMN_NAME));
        }
        return Collections.emptyList();
    }
}
