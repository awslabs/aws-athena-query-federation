/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class SynapseQueryStringBuilder extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SynapseQueryStringBuilder.class);
    public SynapseQueryStringBuilder(String quoteCharacters)
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
        String column = split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN);
        if (column != null) {
            LOGGER.debug("Fetching data using Partition");
            String from = split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM);
            String to = split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO);
            List<String> whereClause;

            LOGGER.debug("PARTITION_COLUMN: {}", column);
            LOGGER.debug("PARTITION_BOUNDARY_FROM: {}", from);
            LOGGER.debug("PARTITION_BOUNDARY_TO: {}", to);

            /*
                form where clause using partition boundaries to create specific partition as split
                example query: select * from MyPartitionTable where id >= 1 and id<= 100
             */
            if (!from.trim().isEmpty() && !to.trim().isEmpty()) {
                whereClause = Collections.singletonList(column + " > " + from + " and " + column + " <= " + to);
            }
            else if (from.trim().isEmpty() && to.trim().isEmpty()) {
                return Collections.emptyList();
            }
            else if (from.trim().isEmpty()) {
                whereClause = Collections.singletonList(column + " <= " + to);
            }
            else {
                whereClause = Collections.singletonList(column + " > " + from);
            }
            return whereClause;
        }
        else {
            LOGGER.debug("Fetching data without Partition");
        }
        return Collections.emptyList();
    }
}
