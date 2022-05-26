/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DeleteLogGroupRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.LogStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the Cloudwatch connector using the integration-test framework.
 */
public class CloudwatchIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchIntegTest.class);

    private final String lambdaFunctionName;
    private final String logGroupName;
    private final String logStreamName;
    private final String logMessage;
    private final long currentTimeMillis;
    private final long fromTimeMillis;
    private final long toTimeMillis;

    public CloudwatchIntegTest()
    {
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json."));
        UUID uuid = UUID.randomUUID();
        logGroupName = userSettings.get("log_group_name") + "/" + uuid;
        logStreamName = userSettings.get("log_stream_name") + "/" + uuid;
        logMessage = (String) userSettings.get("log_message");
        lambdaFunctionName = getLambdaFunctionName();
        // Timestamp (milliseconds) for log messages.
        currentTimeMillis = System.currentTimeMillis(); // Current time.
        // Used in the predicate for the select with predicate test.
        fromTimeMillis = currentTimeMillis + 3000; // Current time + 3 seconds.
        // Used in the predicate for the select with predicate test.
        toTimeMillis = currentTimeMillis + 6000; // Current time + 6 seconds.
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. Cloudwatch
     * Logs).
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        return Optional.of(PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(ImmutableList.of("logs:Describe*", "logs:Get*", "logs:List*", "logs:StartQuery",
                                "logs:StopQuery", "logs:TestMetricFilter", "logs:FilterLogEvents"))
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
        // no-op
    }

    /**
     * Sets up the CloudFormation stack for specific resources (i.e. the Log Group, and Log Stream).
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        LogStream.Builder.create(stack, "CloudWatchLogStream")
                .logStreamName(logStreamName)
                .removalPolicy(RemovalPolicy.DESTROY)
                .logGroup(LogGroup.Builder.create(stack, "CloudWatchLogGroup")
                        .logGroupName(logGroupName)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build())
                .build();
    }

    /**
     * Insert log events into the Cloudwatch Log Group. Any exceptions will be handled by calling method.
     */
    @Override
    protected void setUpTableData()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up Log Group: {}, Log Stream: {}", logGroupName, logStreamName);
        logger.info("----------------------------------------------------");

        AWSLogs logsClient = AWSLogsClientBuilder.defaultClient();

        try {
            logsClient.putLogEvents(new PutLogEventsRequest()
                    .withLogGroupName(logGroupName)
                    .withLogStreamName(logStreamName)
                    .withLogEvents(
                            new InputLogEvent().withTimestamp(currentTimeMillis).withMessage("Space, the final frontier."),
                            new InputLogEvent().withTimestamp(fromTimeMillis).withMessage(logMessage),
                            new InputLogEvent().withTimestamp(toTimeMillis + 5000)
                                    .withMessage("To boldly go where no man has gone before!")));
        }
        finally {
            logsClient.shutdown();
        }
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Log Groups: {}", dbNames);
        assertTrue("Log Group not found.", dbNames.contains(logGroupName));
    }

    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(logGroupName);
        logger.info("Log Streams: {}", tableNames);
        assertTrue(String.format("Log Stream not found: %s.", logStreamName), tableNames.contains(logStreamName));
    }

    @Test
    public void describeTableIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing describeTableIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(logGroupName, logStreamName);
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 3, schema.size());
        assertTrue("Column not found: log_stream", schema.containsKey("log_stream"));
        assertTrue("Column not found: time", schema.containsKey("time"));
        assertTrue("Column not found: message", schema.containsKey("message"));
        assertEquals("Wrong column type for log_stream.", "varchar", schema.get("log_stream"));
        assertEquals("Wrong column type for time.", "bigint", schema.get("time"));
        assertEquals("Wrong column type for message.", "varchar",schema.get("message"));
    }

    // cloudwatch logs are all stored as plain strings, these don't really apply
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

    @Test
    public void selectColumnWithPredicateIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select message from %s.\"%s\".\"%s\" where time between %d and %d;",
                lambdaFunctionName, logGroupName, logStreamName, fromTimeMillis, toTimeMillis);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> messages = new ArrayList<>();
        rows.forEach(row -> messages.add(row.getData().get(0).getVarCharValue()));
        logger.info("Messages: {}", messages);
        assertEquals("Wrong number of log messages found.", 1, messages.size());
        assertTrue("Expecting log message: " + logMessage, messages.contains(logMessage));
    }
}
