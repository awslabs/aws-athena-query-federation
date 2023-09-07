/*-
 * #%L
 * athena-timestream
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
package com.amazonaws.athena.connectors.timestream.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connectors.timestream.TimestreamClientBuilder;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.CreateTableRequest;
import com.amazonaws.services.timestreamwrite.model.DeleteTableRequest;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.timestream.CfnDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimestreamIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamIntegTest.class);

    private final String timestreamDbName;
    private final String timestreamTableName;
    private final String jokeProtagonist;
    private final String jokePunchline;
    private final String lambdaFunctionName;
    private final long[] timeStream;
    private final AmazonTimestreamWrite timestreamWriteClient;

    public TimestreamIntegTest()
    {
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json."));
        timestreamDbName = userSettings.get("timestream_db_name") + "_" +
                UUID.randomUUID().toString().replace('-', '_');;
        timestreamTableName = (String) userSettings.get("timestream_table_name");
        jokeProtagonist = (String) userSettings.get("joke_protagonist");
        jokePunchline = (String) userSettings.get("joke_punchline");
        lambdaFunctionName = getLambdaFunctionName();
        long currentTimeMillis = System.currentTimeMillis();
        timeStream = new long[] {currentTimeMillis, currentTimeMillis + 10_000L, currentTimeMillis + 12_000L,
                currentTimeMillis + 14_000L, currentTimeMillis + 16_000L, currentTimeMillis + 18_000L,
                currentTimeMillis + 20_000L, currentTimeMillis + 22_000L, currentTimeMillis + 24_000L,
                currentTimeMillis + 26_000L};
        timestreamWriteClient = TimestreamClientBuilder.buildWriteClient("timestream");
    }

    /**
     * Creates the Timestream database and table used for the integration tests, and insert records..
     */
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        try {
            // Invoke the framework's setUp() that also creates the Timestream database.
            super.setUp();
            // Create the Timestream table.
            createTimestreamTable();
            // Insert records into newly created table.
            insertTableRecords();
        }
        catch (Exception e) {
            // Delete the Timestream table (must be deleted before the database).
            deleteTimstreamTable();
            throw e;
        }
    }

    /**
     * Deletes a CloudFormation stack for the Timestream table, database, and Lambda.
     */
    @AfterClass
    @Override
    protected void cleanUp()
    {
        // Delete the Timestream table (must be deleted before the database).
        deleteTimstreamTable();
        // Invoke the framework's cleanUp().
        super.cleanUp();
    }

    /**
     * Creates the Timestream table. Any exceptions will be handled by calling method.
     */
    private void createTimestreamTable()
    {
        logger.info("----------------------------------------------------");
        logger.info("Creating the Timestream table: {}", timestreamTableName);
        logger.info("----------------------------------------------------");

        timestreamWriteClient.createTable(new CreateTableRequest()
                .withDatabaseName(timestreamDbName)
                .withTableName(timestreamTableName));
    }

    /**
     * Delete the Timestream table prior to Deleting the database.
     */
    private void deleteTimstreamTable()
    {
        logger.info("----------------------------------------------------");
        logger.info("Deleting the Timestream table: {}", timestreamTableName);
        logger.info("----------------------------------------------------");

        try {
            timestreamWriteClient.deleteTable(new DeleteTableRequest()
                    .withDatabaseName(timestreamDbName)
                    .withTableName(timestreamTableName));
        }
        catch (Exception e) {
            // Do not rethrow here.
            logger.error("Unable to delete Timestream table: " + e.getMessage(), e);
        }
        finally {
            timestreamWriteClient.shutdown();
        }
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. Timestream).
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        return Optional.of(PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(ImmutableList.of("timestream:*"))
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
        // This is a no-op for this connector.
    }

    /**
     * Sets up the Timestream database CloudFormation stack.
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        CfnDatabase.Builder.create(stack, "TimestreamDB")
                .databaseName(timestreamDbName)
                .build();
    }

    /**
     * Set up a table resource by creating and/or inserting data as part of the framework processing.
     */
    @Override
    protected void setUpTableData()
    {
        // No-op
    }

    /**
     * Insert records into the Timestream table.
     */
    protected void insertTableRecords()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up the Timestream table: {}", timestreamTableName);
        logger.info("----------------------------------------------------");

        int i = 0;
        timestreamWriteClient.writeRecords(new TimestreamWriteRecordRequestBuilder()
                .withDatabaseName(timestreamDbName)
                .withTableName(timestreamTableName)
                .withMeasureName("dB")
                .withMeasureValueType(MeasureValueType.DOUBLE)
                .withRecord(ImmutableMap.of("subject", "Narrator", "conversation",
                        String.format("A %s walks into a restaurant, and orders soup.", jokeProtagonist)),
                        "40", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", "Narrator", "conversation",
                        "The waiter brings the soup."),
                        "40", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", jokeProtagonist, "conversation", "Taste the soup!"),
                        "50", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", "Waiter", "conversation", "What, is it too cold?"),
                        "45", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", jokeProtagonist, "conversation", "Taste the soup!"),
                        "60", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", "Waiter", "conversation", "What, is it too salty?"),
                        "45", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", jokeProtagonist, "conversation", "Taste the soup!"),
                        "70", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", "Waiter", "conversation",
                        "Alright, alright I'll taste the soup. Where's the spoon?"),
                        "60", timeStream[Math.min(timeStream.length - 1, i++)])
                .withRecord(ImmutableMap.of("subject", jokeProtagonist, "conversation", jokePunchline),
                        "60", timeStream[Math.min(timeStream.length - 1, i)])
                .build());
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains(timestreamDbName));
    }

    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(timestreamDbName);
        logger.info("Tables: {}", tableNames);
        assertTrue(String.format("Table not found: %s.", timestreamTableName),
                tableNames.contains(timestreamTableName));
    }

    @Test
    public void listTableSchemaIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listTableSchemaIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(timestreamDbName, timestreamTableName);
        schema.remove("partition_name");
        schema.remove("partition_schema_name");
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 5, schema.size());
        assertTrue("Column not found: subject", schema.containsKey("subject"));
        assertEquals("Wrong column type for subject.", "varchar", schema.get("subject"));
        assertTrue("Column not found: conversation", schema.containsKey("conversation"));
        assertEquals("Wrong column type for conversation.", "varchar", schema.get("conversation"));
        assertTrue("Column not found: measure_name", schema.containsKey("measure_name"));
        assertEquals("Wrong column type for measure_name.", "varchar", schema.get("measure_name"));
        assertTrue("Column not found: measure_value::double", schema.containsKey("measure_value::double"));
        assertEquals("Wrong column type for measure_value::double.", "double",
                schema.get("measure_value::double"));
        assertTrue("Column not found: time", schema.containsKey("time"));
        assertEquals("Wrong column type for time.", "timestamp", schema.get("time"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select conversation from \"%s\".\"%s\".\"%s\" where subject = '%s' order by time desc limit 1;",
                lambdaFunctionName, timestreamDbName, timestreamTableName, jokeProtagonist);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> conversation = new ArrayList<>();
        rows.forEach(row -> conversation.add(row.getData().get(0).getVarCharValue()));
        logger.info("conversation: {}", conversation);
        assertEquals("Wrong number of DB records found.", 1, conversation.size());
        assertTrue("Did not find correct conversation: " + jokePunchline, conversation.contains(jokePunchline));
    }

    // timestream values are all stored as plain strings or doubles, these don't really apply (or are already tested above)
    @Override
    public void selectIntegerTypeTest()
    {
    }

    @Override
    public void selectVarcharTypeTest()
    {
    }

    @Override
    public void selectBooleanTypeTest()
    {
    }

    @Override
    public void selectSmallintTypeTest()
    {
    }

    @Override
    public void selectBigintTypeTest()
    {
    }

    @Override
    public void selectFloat4TypeTest()
    {
    }

    @Override
    public void selectFloat8TypeTest()
    {
    }

    @Override
    public void selectDateTypeTest()
    {
    }

    @Override
    public void selectTimestampTypeTest()
    {
    }

    @Override
    public void selectByteArrayTypeTest()
    {
    }

    @Override
    public void selectVarcharListTypeTest()
    {
    }

    @Override
    public void selectNullValueTest()
    {
    }

    @Override
    public void selectEmptyTableTest()
    {
    }

}
