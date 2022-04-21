/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class SqlServerQueryStringBuilder extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerQueryStringBuilder.class);
    public SqlServerQueryStringBuilder(String quoteCharacters)
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

    /**
     * In case of partitioned table, custom query will be formed to get specific partition
     * otherwise empty list will be returned
     * @param split
     * @return
     */
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        //example query: select * from MyPartitionTable where $PARTITION.myRangePF(col1) =2
        LOGGER.debug("PARTITION_FUNCTION: {}", split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION));
        LOGGER.debug("PARTITIONING_COLUMN: {}", split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN));

        if (split.getProperty(SqlServerMetadataHandler.PARTITION_NUMBER) != null && !split.getProperty(SqlServerMetadataHandler.PARTITION_NUMBER).equals("0")) {
            LOGGER.info("Fetching data using Partition");
            return Collections.singletonList(" $PARTITION." + split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)
                    + "(" + split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN) + ") = " + split.getProperty(SqlServerMetadataHandler.PARTITION_NUMBER));
        }
        else {
            LOGGER.info("Fetching data without Partition");
        }
        return Collections.emptyList();
    }
}
