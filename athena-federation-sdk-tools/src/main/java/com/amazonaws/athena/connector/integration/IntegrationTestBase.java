/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.integration;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.Capability;
import com.amazonaws.services.cloudformation.model.CreateStackRequest;
import com.amazonaws.services.cloudformation.model.CreateStackResult;
import com.amazonaws.services.cloudformation.model.DeleteStackRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackEventsRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackEventsResult;
import com.amazonaws.services.cloudformation.model.StackEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * The Integration-Tests base class from which all connector-specific integration test modules should subclass.
 */
public abstract class IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    private static final String CF_TEMPLATE_NAME = "integration-test.yaml";
    private static final String CF_CREATE_RESOURCE_IN_PROGRESS_STATUS = "CREATE_IN_PROGRESS";
    private static final String CF_CREATE_RESOURCE_FAILED_STATUS = "CREATE_FAILED";
    private static final String ATHENA_QUERY_QUEUED_STATE = "QUEUED";
    private static final String ATHENA_QUERY_RUNNING_STATE = "RUNNING";
    private static final String ATHENA_QUERY_FAILED_STATE = "FAILED";
    private static final String ATHENA_QUERY_CANCELLED_STATE = "CANCELLED";
    private static final String ATHENA_FEDERATION_WORK_GROUP = "AmazonAthenaPreviewFunctionality";
    private static final long sleepTimeMillis = 5000L;

    private final AmazonAthena athenaClient;
    private final String cloudFormationStackName;

    public IntegrationTestBase()
    {
        athenaClient = AmazonAthenaClientBuilder.defaultClient();
        cloudFormationStackName = "Integration-Test-" + this.getClass().getSimpleName() +
                "-" + UUID.randomUUID().toString();
    }

    /**
     * Must be overridden in the extending class to setup the DB table (i.e. insert rows into table, etc...)
     */
    protected abstract void setupData();

    /**
     * Creates a CloudFormation stack to build the infrastructure needed to run the integration tests (e.g., Database
     * instance, Lambda function, etc...) It then calls abstract method setupDbTable() to setup the DB table.
     * @throws InterruptedException Thread is interrupted during sleep.
     * @throws IOException The CloudFormation stack template file is not found or cannot be read.
     * @throws RuntimeException The CloudFormation stack creation failed.
     */
    @BeforeClass
    protected void createStack()
            throws InterruptedException, IOException, RuntimeException
    {
        logger.info("------------------------------------------------------");
        logger.info("Create CloudFormation stack: {}", cloudFormationStackName);
        logger.info("------------------------------------------------------");

        String stackTemplate = getTemplateFromFile();
        CreateStackRequest createStackRequest = new CreateStackRequest()
                .withStackName(cloudFormationStackName)
                .withTemplateBody(stackTemplate)
                .withDisableRollback(true)
                .withCapabilities(Capability.CAPABILITY_IAM);
        processCreateStackRequest(createStackRequest);
        setupData();
    }

    /**
     * Converts the CloudFormation stack template yaml/json file to String.
     * @return A String representation of the CloudFormation stack template.
     * @throws IOException If the CloudFormation stack template file is not found or cannot be read.
     */
    private String getTemplateFromFile()
            throws IOException
    {
        StringBuilder templateBuilder = new StringBuilder();

        Stream<String> stream = Files.lines(Paths.get(CF_TEMPLATE_NAME), StandardCharsets.UTF_8);
        stream.forEach(line -> {
            if (line.contains("S3_KEY")) {
                line = line.replace("S3_KEY", getS3KeyFromPackagedYaml());
            }
            templateBuilder.append(line).append("\n");
        });

        return templateBuilder.toString();
    }

    /**
     * Gets the S3 Key (from packaged.yaml) for the S3 Bucket containing the Connector code.
     * @return String with the S3 Key.
     */
    private String getS3KeyFromPackagedYaml()
    {
        String s3Key = "";

        try {
            for (String line : Files.readAllLines(Paths.get("packaged.yaml"), StandardCharsets.UTF_8)) {
                if (line.contains("CodeUri")) {
                    s3Key = line.substring(line.lastIndexOf('/') + 1);
                    break;
                }
            }
        }
        catch (IOException e) {
            logger.error("Unable to retrieve S3 Key", e);
        }

        logger.info("S3Key: {}", s3Key);

        return s3Key;
    }

    /**
     * Processes the creation of a CloudFormation stack including polling of the stack's status while in progress.
     * @param createStackRequest Request used to generate the CloudFormation stack.
     * @throws InterruptedException Thread is interrupted during sleep.
     * @throws RuntimeException The CloudFormation stack creation failed.
     */
    private void processCreateStackRequest(CreateStackRequest createStackRequest)
            throws InterruptedException, RuntimeException
    {
        // Create CloudFormation stack.
        AmazonCloudFormation cloudFormationClient = AmazonCloudFormationClientBuilder.defaultClient();
        CreateStackResult result = cloudFormationClient.createStack(createStackRequest);
        logger.info("Stack ID: {}", result.getStackId());

        DescribeStackEventsRequest describeStackEventsRequest = new DescribeStackEventsRequest()
                .withStackName(createStackRequest.getStackName());
        DescribeStackEventsResult describeStackEventsResult;

        // Poll status of stack until stack has been created or creation has failed
        while (true) {
            describeStackEventsResult = cloudFormationClient.describeStackEvents(describeStackEventsRequest);
            StackEvent event = describeStackEventsResult.getStackEvents().get(0);
            String resourceId = event.getLogicalResourceId();
            String resourceStatus = event.getResourceStatus();
            logger.info("Resource Id: {}, Resource status: {}", resourceId, resourceStatus);
            if (!resourceId.equals(event.getStackName()) ||
                    resourceStatus.equals(CF_CREATE_RESOURCE_IN_PROGRESS_STATUS)) {
                Thread.sleep(sleepTimeMillis);
                continue;
            }
            else if (resourceStatus.equals(CF_CREATE_RESOURCE_FAILED_STATUS)) {
                throw new RuntimeException(getCloudFormationErrorReasons(describeStackEventsResult.getStackEvents()));
            }
            break;
        }
    }

    /**
     * Provides a detailed error message when the CloudFormation stack creation fails.
     * @param stackEvents The list of CloudFormation stack events.
     * @return String containing the formatted error message.
     */
    private String getCloudFormationErrorReasons(List<StackEvent> stackEvents)
    {
        StringBuilder errorMessageBuilder =
                new StringBuilder("CloudFormation stack creation failed due to the following reason(s):\n");

        stackEvents.forEach(stackEvent -> {
            if (stackEvent.getResourceStatus().equals(CF_CREATE_RESOURCE_FAILED_STATUS)) {
                String errorMessage = String.format("Resource: %s, Reason: %s\n",
                        stackEvent.getLogicalResourceId(), stackEvent.getResourceStatusReason());
                errorMessageBuilder.append(errorMessage);
            }
        });

        return errorMessageBuilder.toString();
    }

    /**
     * Deletes a CloudFormation stack.
     */
    @AfterClass
    protected void deleteStack()
    {
        logger.info("------------------------------------------------------");
        logger.info("Delete CloudFormation stack: {}", cloudFormationStackName);
        logger.info("------------------------------------------------------");

        AmazonCloudFormation cloudFormationClient = AmazonCloudFormationClientBuilder.defaultClient();
        DeleteStackRequest request = new DeleteStackRequest().withStackName(cloudFormationStackName);
        cloudFormationClient.deleteStack(request);
    }

    /**
     * Sends a DB query via Athena and returns the query results.
     * @param query - The query string to be processed by Athena.
     * @return The query results object containing the metadata and row information.
     * @throws InterruptedException Thread is interrupted during sleep.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    protected GetQueryResultsResult startQueryExecution(String query)
            throws InterruptedException, RuntimeException
    {
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withWorkGroup(ATHENA_FEDERATION_WORK_GROUP)
                .withQueryString(query);

        String queryExecutionId = sendAthenaQuery(startQueryExecutionRequest);
        logger.info("Query: [{}], Query Id: [{}]", query, queryExecutionId);
        waitForAthenaQueryResults(queryExecutionId);
        GetQueryResultsResult getQueryResultsResult = getAthenaQueryResults(queryExecutionId);
        logger.info("Results: [{}]", getQueryResultsResult.toString());

        return getQueryResultsResult;
    }

    /**
     * Sends the DB query via the Athena API.
     * @param startQueryExecutionRequest Query execution request.
     * @return Query execution Id.
     */
    private String sendAthenaQuery(StartQueryExecutionRequest startQueryExecutionRequest)
    {
        // Send query request
        StartQueryExecutionResult executionResult = athenaClient.startQueryExecution(startQueryExecutionRequest);

        return executionResult.getQueryExecutionId();
    }

    /**
     * Wait for the Athena query request to complete while it is either queued or running.
     * @param queryExecutionId The query's Id.
     * @throws InterruptedException Thread is interrupted during sleep.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    private void waitForAthenaQueryResults(String queryExecutionId)
            throws InterruptedException, RuntimeException
    {
        // Poll the state of the query request while it is queued or running
        GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                .withQueryExecutionId(queryExecutionId);
        GetQueryExecutionResult getQueryExecutionResult;
        while (true) {
            getQueryExecutionResult = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState();
            logger.info("Query State: {}", queryState);
            if (queryState.equals(ATHENA_QUERY_QUEUED_STATE) || queryState.equals(ATHENA_QUERY_RUNNING_STATE)) {
                Thread.sleep(sleepTimeMillis);
                continue;
            }
            else if (queryState.equals(ATHENA_QUERY_FAILED_STATE) || queryState.equals(ATHENA_QUERY_CANCELLED_STATE)) {
                throw new RuntimeException(getQueryExecutionResult.
                        getQueryExecution().getStatus().getStateChangeReason());
            }
            break;
        }
    }

    /**
     * Gets the Athena query's results.
     * @param queryExecutionId The query's Id.
     * @return The query results object containing the metadata and row information.
     */
    private GetQueryResultsResult getAthenaQueryResults(String queryExecutionId)
    {
        // Get query results
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                .withQueryExecutionId(queryExecutionId);

        return athenaClient.getQueryResults(getQueryResultsRequest);
    }
}
