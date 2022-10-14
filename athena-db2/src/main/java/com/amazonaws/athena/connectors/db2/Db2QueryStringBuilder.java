/*-
 * #%L
 * athena-db2
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.db2;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class Db2QueryStringBuilder extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2QueryStringBuilder.class);
    public Db2QueryStringBuilder(String quoteCharacters)
    {
        super(quoteCharacters);
    }

    /**
     * Formatting SQL query statement. Adding period (.) after catalog and schema name
     * and FROM before table name.
     *
     * @param catalog
     * @param schema
     * @param table
     * @param split
     * @return
     */
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

    /**
     * Creates partition framing clause by taking information from split property.
     *
     * @param split
     * @return
     */
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String column = split.getProperty(Db2MetadataHandler.PARTITIONING_COLUMN);
        if (column != null) {
            LOGGER.debug("Fetching data using Partition");
            //example query: select * from EMP_TABLE WHERE DATAPARTITIONNUM(EMP_NO) = 0
            return Collections.singletonList(" DATAPARTITIONNUM(" + column + ") = " + split.getProperty(Db2MetadataHandler.PARTITION_NUMBER));
        }
        else {
            LOGGER.debug("Fetching data without Partition");
        }
        return Collections.emptyList();
    }
}
