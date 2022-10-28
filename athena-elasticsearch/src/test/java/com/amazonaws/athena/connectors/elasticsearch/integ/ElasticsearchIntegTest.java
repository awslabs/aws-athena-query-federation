/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.services.athena.model.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.elasticsearch.CapacityConfig;
import software.amazon.awscdk.services.elasticsearch.Domain;
import software.amazon.awscdk.services.elasticsearch.ElasticsearchVersion;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the Elasticsearch connector using the Integration-test module.
 */
public class ElasticsearchIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchIntegTest.class);

    private final App theApp;
    private final String dbClusterName;
    private final String lambdaFunctionName;
    private final String domainName;
    private final String index;

    private CloudFormationClient cloudFormationClient;

    public ElasticsearchIntegTest()
    {
        theApp = new App();
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json file."));
        dbClusterName = "integ-es-cluster-" + UUID.randomUUID();
        lambdaFunctionName = getLambdaFunctionName();
        domainName = (String) userSettings.get("domain_name");
        index = (String) userSettings.get("index");
    }

    /**
     * Creates an Elasticsearch Cluster used for the integration tests.
     */
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        cloudFormationClient = new CloudFormationClient(theApp, getElasticsearchStack());
        try {
            // Create the CloudFormation stack for the Elasticsearch Cluster.
            cloudFormationClient.createStack();
            // Invoke the framework's setUp().
            super.setUp();
        }
        catch (Exception e) {
            // Delete the partially formed CloudFormation stack.
            cloudFormationClient.deleteStack();
            throw e;
        }
    }

    /**
     * Deletes a CloudFormation stack for the Elasticsearch Cluster.
     */
    @AfterClass
    @Override
    protected void cleanUp()
    {
        // Invoke the framework's cleanUp().
        super.cleanUp();
        // Delete the CloudFormation stack for the Elasticsearch Cluster.
        cloudFormationClient.deleteStack();
    }

    /**
     * Gets the CloudFormation stack for the Elasticsearch Cluster.
     * @return Stack object for the Elasticsearch Cluster.
     */
    private Stack getElasticsearchStack()
    {
        Stack stack = Stack.Builder.create(theApp, dbClusterName).build();

        Domain.Builder.create(stack, "ElasticsearchCluster")
                .domainName(domainName)
                .version(ElasticsearchVersion.V7_9)
                .capacity(CapacityConfig.builder()
                        .dataNodeInstanceType("t2.medium.elasticsearch")
                        .dataNodes(1)
                        .build())
                .build();

        return stack;
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        return Optional.of(PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(ImmutableList.of("es:List*", "es:Describe*", "es:ESHttp*", "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"))
                        .resources(ImmutableList.of("*"))
                        .effect(Effect.ALLOW)
                        .build()))
                .build());
    }

    /**
     * Sets the environment variables for the Lambda function.
     */
    @Override
    protected void setConnectorEnvironmentVars(final Map environmentVars)
    {
        // No-op.
    }

    /**
     * Sets up connector-specific Cloud Formation resource.
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        // No-op.
    }

    /**
     * Sets up the DB tables used by the tests.
     */
    @Override
    protected void setUpTableData()
    {
        try (ElasticsearchIndexUtils indexUtils = new ElasticsearchIndexUtils(domainName, index))
        {
            indexUtils.addDocument(ImmutableMap.of(
                    "year", 1984,
                    "title", "Aliens",
                    "director", "James Cameron",
                    "cast", ImmutableList.of("Sigourney Weaver", "Paul Reiser", "Lance Henriksen", "Bill Paxton")));
            indexUtils.addDocument(ImmutableMap.of(
                    "year", 2014,
                    "title", "Interstellar",
                    "director", "James Cameron",
                    "cast", ImmutableList.of("Matthew McConaughey", "John Lithgow", "Ann Hathaway",
                            "David Gyasi", "Michael Caine", "Jessica Chastain", "Matt Damon", "Casey Affleck")
            ));
        }
        try (ElasticsearchIndexUtils indexUtils = new ElasticsearchIndexUtils(domainName, TEST_DATATYPES_TABLE_NAME))
        {
            Map<String, Object> item = new ImmutableMap.Builder<String, Object>()
                .put("int_type", TEST_DATATYPES_INT_VALUE)
                .put("smallint_type", TEST_DATATYPES_SHORT_VALUE)
                .put("bigint_type", TEST_DATATYPES_LONG_VALUE)
                .put("varchar_type", TEST_DATATYPES_VARCHAR_VALUE)
                .put("boolean_type", TEST_DATATYPES_BOOLEAN_VALUE)
                .put("float4_type", TEST_DATATYPES_SINGLE_PRECISION_VALUE)
                .put("float8_type", 1e-32)
//                .put("date_type", Date.parse(TEST_DATATYPES_DATE_VALUE))
                .put("timestamp_type", TEST_DATATYPES_TIMESTAMP_VALUE)
                .put("textarray_type", TEST_DATATYPES_VARCHAR_ARRAY_VALUE).build();
            indexUtils.addDocument(item);
        }

        ElasticsearchIndexUtils emptyIndex = new ElasticsearchIndexUtils(domainName, TEST_EMPTY_TABLE_NAME);
        emptyIndex.addDocument(new ImmutableMap.Builder<String, Object>().build());
        emptyIndex.close();

        try (ElasticsearchIndexUtils indexUtils = new ElasticsearchIndexUtils(domainName, TEST_NULL_TABLE_NAME))
        {
            Map<String, Object> item = new ImmutableMap.Builder<String, Object>()
                    .put("int_type", "null").build();
            indexUtils.addDocument(item);
        }

    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains(domainName));
    }
    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(domainName);
        logger.info("Tables: {}", tableNames);
        assertTrue(String.format("Table not found: %s.", index), tableNames.contains(index));
    }

    @Test
    public void describeTableIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing describeTableIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(domainName, index);
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 4, schema.size());
        assertTrue("Column not found: year", schema.containsKey("year"));
        assertTrue("Column not found: title", schema.containsKey("title"));
        assertTrue("Column not found: director", schema.containsKey("director"));
        assertTrue("Column not found: cast", schema.containsKey("cast"));
        assertEquals("Wrong column type.", "bigint", schema.get("year"));
        assertEquals("Wrong column type.", "varchar", schema.get("title"));
        assertEquals("Wrong column type.", "varchar", schema.get("director"));
        assertEquals("Wrong column type.", "varchar", schema.get("cast"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select title from %s.%s.%s where year > 2000;",
                lambdaFunctionName, domainName, index);
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

    @Test
    public void selectFloat8TypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectFloat8TypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select float8_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<Double> values = new ArrayList<>();
        rows.forEach(row -> values.add(Double.valueOf(row.getData().get(0).getVarCharValue())));
        AssertJUnit.assertEquals("Wrong number of DB records found.", 1, values.size());
        AssertJUnit.assertTrue("Float8 not found: " + 1e-32, values.contains(1e-32));
    }

    @Test
    public void selectByteArrayTypeTest()
    {
        // not supported
    }

    @Test
    public void selectDateTypeTest()
    {
        // TODO: fix this test
    }

    @Test
    public void selectNullValueTest()
    {
        // not supported
    }

    @Override
    public void selectEmptyTableTest()
    {
        // not supported
    }
}
