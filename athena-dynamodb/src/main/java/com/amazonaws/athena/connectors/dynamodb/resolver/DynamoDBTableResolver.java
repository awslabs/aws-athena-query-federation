/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.resolver;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * This class helps with resolving the differences in casing between DynamoDB and Presto. Presto expects all
 * databases, tables, and columns to be lower case. This class allows us to resolve DynamoDB tables
 * which may have captial letters in them without issue. It does so by fetching all table names and doing
 * a case insensitive search over them. It will first try to do a targeted get to reduce the penalty for
 * tables which don't have capitalization.
 *
 * TODO add caching
 */
public class DynamoDBTableResolver
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBTableResolver.class);

    private AmazonDynamoDB ddbClient;
    // used to handle Throttling events using an AIMD strategy for congestion control.
    private ThrottlingInvoker invoker;

    public DynamoDBTableResolver(ThrottlingInvoker invoker, AmazonDynamoDB ddbClient)
    {
        this.invoker = invoker;
        this.ddbClient = ddbClient;
    }

    public List<TableName> listTables()
            throws TimeoutException
    {
        List<TableName> tables = new ArrayList<>();
        String nextToken = null;
        do {
            ListTablesRequest ddbRequest = new ListTablesRequest()
                    .withExclusiveStartTableName(nextToken);
            ListTablesResult result = invoker.invoke(() -> ddbClient.listTables(ddbRequest));
            tables.addAll(result.getTableNames().stream().map(table -> new TableName(DEFAULT_SCHEMA, table)).collect(toImmutableList()));
            nextToken = result.getLastEvaluatedTableName();
        }
        while (nextToken != null);
        return tables;
    }

    public Schema getTableSchema(String tableName)
            throws TimeoutException
    {
        try {
            return DDBTableUtils.peekTableForSchema(tableName, invoker, ddbClient);
        }
        catch (ResourceNotFoundException e) {
            Optional<String> caseInsensitiveMatch = tryCaseInsensitiveSearch(tableName);
            if (caseInsensitiveMatch.isPresent()) {
                return DDBTableUtils.peekTableForSchema(caseInsensitiveMatch.get(), invoker, ddbClient);
            }
            else {
                throw e;
            }
        }
    }

    public DynamoDBTable getTableMetadata(String tableName)
            throws TimeoutException
    {
        try {
            return DDBTableUtils.getTable(tableName, invoker, ddbClient);
        }
        catch (ResourceNotFoundException e) {
            Optional<String> caseInsensitiveMatch = tryCaseInsensitiveSearch(tableName);
            if (caseInsensitiveMatch.isPresent()) {
                return DDBTableUtils.getTable(caseInsensitiveMatch.get(), invoker, ddbClient);
            }
            else {
                throw e;
            }
        }
    }

    private Optional<String> tryCaseInsensitiveSearch(String tableName)
            throws TimeoutException
    {
        logger.info("Table {} not found.  Falling back to case insensitive search.", tableName);
        Multimap<String, String> lowerCaseNameMapping = ArrayListMultimap.create();
        for (TableName nextTableName : listTables()) {
            lowerCaseNameMapping.put(nextTableName.getTableName().toLowerCase(Locale.ENGLISH), nextTableName.getTableName());
        }
        Collection<String> mappedNames = lowerCaseNameMapping.get(tableName);
        if (mappedNames.size() > 1) {
            throw new IllegalStateException(String.format("Multiple tables resolved from case insensitive name %s: %s", tableName, mappedNames));
        }
        else if (mappedNames.size() == 1) {
            return Optional.of(mappedNames.iterator().next());
        }
        else {
            return Optional.empty();
        }
    }
}
