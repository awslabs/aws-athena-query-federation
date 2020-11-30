/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.services.athena.model.Row;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the DynamoDB connector using the Integration-test Suite.
 */
public class DynamoDBInteg extends IntegrationTestBase {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBInteg.class);
    private static final String DATABASE_NAME = "default";

    private final String lambdaFunctionName;
    private final String tableName;
    private final DdbTableUtils ddbTableUtils;

    public DynamoDBInteg()
    {
        lambdaFunctionName = getLambdaFunctionName();
        tableName = String.format("dynamodbit_%s", UUID.randomUUID().toString().replace('-', '_'));
        ddbTableUtils = new DdbTableUtils(tableName);
    }


    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */
    protected PolicyDocument getConnectorAccessPolicy()
    {
        return PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(ImmutableList.of("dynamodb:DescribeTable", "dynamodb:ListSchemas",
                                "dynamodb:ListTables", "dynamodb:Query", "dynamodb:Scan"))
                        .resources(ImmutableList.of("*"))
                        .effect(Effect.ALLOW)
                        .build()))
                .build();
    }

    /**
     * Sets the environment variables for the Lambda function.
     */
    protected void setConnectorEnvironmentVars(final Map environmentVars)
    {
        // This is a no-op for this connector.
    }

    /**
     * Sets up the DDB Table's CloudFormation stack.
     * @param stack The current CloudFormation stack.
     */
    protected void setupStackData(final Stack stack)
    {
        ddbTableUtils.setupTableStack(stack);
    }

    /**
     * Insert rows into the newly created DDB table.
     */
    protected void setupData()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", tableName);
        logger.info("----------------------------------------------------");

        ddbTableUtils.addItems();
    }

    @Test
    public void listDatabasesInteg()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesInteg");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains("default"));
    }

    @Test
    public void listTablesInteg()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesInteg");
        logger.info("-----------------------------------");

        List tableNames = listTables(DATABASE_NAME);
        logger.info("Tables: {}", tableNames);
        assertTrue(String.format("Table not found: %s.", tableName), tableNames.contains(tableName));
    }

    @Test
    public void describeTableInteg()
    {
        logger.info("--------------------------------------");
        logger.info("Executing describeTableInteg");
        logger.info("--------------------------------------");

        Map schema = describeTable(DATABASE_NAME, tableName);
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 3, schema.size());
        assertTrue("Column not found: title", schema.containsKey("title"));
        assertTrue("Column not found: year", schema.containsKey("year"));
        assertTrue("Column not found: info", schema.containsKey("info"));
        assertEquals("Wrong column type.", "varchar", schema.get("title"));
        assertEquals("Wrong column type.", "decimal(38,9)", schema.get("year"));
        assertEquals("Wrong column type.",
                "struct<cast:array<varchar>,director:varchar,genre:array<varchar>>",schema.get("info"));
    }

    @Test
    public void selectColumnWithPredicateInteg()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateInteg");
        logger.info("--------------------------------------------------");

        String query = String.format("select title from %s.%s.%s where year > 2000;",
                lambdaFunctionName, DATABASE_NAME, tableName);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> titles = new ArrayList<>();
        rows.forEach(row -> titles.add(row.getData().get(0).getVarCharValue()));
        logger.info("Titles: {}", titles);
        assertEquals("Wrong number of DB records found.", 1, titles.size());
        assertTrue("Movie title not found: Interstellar.", titles.contains("Interstellar"));
    }
}
