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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.google.common.base.Strings;
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SynapseQueryStringBuilder extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SynapseQueryStringBuilder.class);
    public SynapseQueryStringBuilder(String quoteCharacters, final FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
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
    protected void addPartitionWhereClauses(Split split, List<String> clauses, List<TypeAndValue> accumulator)
    {
        String column = split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN);
        if (column == null) {
            LOGGER.debug("Fetching data without Partition");
            return;
        }

        LOGGER.debug("Fetching data using Partition");
        String from = Strings.nullToEmpty(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).trim();
        String to = Strings.nullToEmpty(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).trim();
        LOGGER.debug("PARTITION_COLUMN: {}", column);
        LOGGER.debug("PARTITION_BOUNDARY_FROM: {}", from);
        LOGGER.debug("PARTITION_BOUNDARY_TO: {}", to);

        String quotedColumn = quote(column);
        /*
            form where clause using partition boundaries to create specific partition as split
            example query: select * from MyPartitionTable where id >= 1 and id<= 100
         */
        if (from.isEmpty() && to.isEmpty()) {
            return;
        }
        if (!from.isEmpty() && !to.isEmpty()) {
            clauses.add(quotedColumn + " > ? AND " + quotedColumn + " <= ?");
            accumulator.add(new TypeAndValue(Types.MinorType.VARCHAR.getType(), from));
            accumulator.add(new TypeAndValue(Types.MinorType.VARCHAR.getType(), to));
        }
        else if (from.isEmpty()) {
            clauses.add(quotedColumn + " <= ?");
            accumulator.add(new TypeAndValue(Types.MinorType.VARCHAR.getType(), to));
        }
        else {
            clauses.add(quotedColumn + " > ?");
            accumulator.add(new TypeAndValue(Types.MinorType.VARCHAR.getType(), from));
        }
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        List<String> clauses = new ArrayList<>();
        List<TypeAndValue> ignoredAccumulator = new ArrayList<>();
        addPartitionWhereClauses(split, clauses, ignoredAccumulator);
        return clauses;
    }

    //Returning empty string as Synapse does not support LIMIT clause
    @Override
    protected String appendLimitOffset(Split split, Constraints constraints)
    {
        return emptyString;
    }
}
