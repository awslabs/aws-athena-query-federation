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
import com.amazonaws.services.athena.model.CreateDataCatalogRequest;
import com.amazonaws.services.athena.model.DataCatalogType;
import com.amazonaws.services.athena.model.Database;
import com.amazonaws.services.athena.model.DeleteDataCatalogRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.ListDatabasesRequest;
import com.amazonaws.services.athena.model.ListDatabasesResult;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.Capability;
import com.amazonaws.services.cloudformation.model.CreateStackRequest;
import com.amazonaws.services.cloudformation.model.CreateStackResult;
import com.amazonaws.services.cloudformation.model.DeleteStackRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackEventsRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackEventsResult;
import com.amazonaws.services.cloudformation.model.StackEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The Integration-Tests base class from which all connector-specific integration test modules should subclass.
 */
public abstract class IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    private static final String CF_TEMPLATE_NAME = "packaged.yaml";
    private static final String LAMBDA_CODE_URI_TAG = "CodeUri:";
    private static final String LAMBDA_SPILL_BUCKET_PREFIX = "s3://";
    private static final String LAMBDA_HANDLER_TAG = "Handler:";
    private static final String LAMBDA_HANDLER_PREFIX = "Handler: ";
    private static final String CF_CREATE_RESOURCE_IN_PROGRESS_STATUS = "CREATE_IN_PROGRESS";
    private static final String CF_CREATE_RESOURCE_FAILED_STATUS = "CREATE_FAILED";
    private static final String ATHENA_QUERY_QUEUED_STATE = "QUEUED";
    private static final String ATHENA_QUERY_RUNNING_STATE = "RUNNING";
    private static final String ATHENA_QUERY_FAILED_STATE = "FAILED";
    private static final String ATHENA_QUERY_CANCELLED_STATE = "CANCELLED";
    private static final String ATHENA_FEDERATION_WORK_GROUP = "AmazonAthenaPreviewFunctionality";
    private static final long sleepTimeMillis = 5000L;

    private final String lambdaFunctionName;
    private String lambdaFunctionHandler;
    private String spillBucket;
    private String s3Key;
    private final AmazonAthena athenaClient;
    private final String cloudFormationStackName;
    private final App theApp;
    private final ObjectMapper objectMapper;

    public IntegrationTestBase()
    {
        final UUID randomUuid = UUID.randomUUID();
        this.lambdaFunctionName = this.getClass().getSimpleName().toLowerCase() + "_" +
                randomUuid.toString().replace('-', '_') ;
        this.cloudFormationStackName = "integration-" + this.getClass().getSimpleName() + "-" + randomUuid;
        this.athenaClient = AmazonAthenaClientBuilder.defaultClient();
        this.theApp = new App();
        this.objectMapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    /**
     * Gets the name of the lambda function generated by the Integration-Test Suite.
     * @return The name of the lambda function.
     */
    public String getLambdaFunctionName()
    {
        return lambdaFunctionName;
    }

    /**
     * Must be overridden in the extending class to setup the DB table (i.e. insert rows into table, etc...)
     */
    protected abstract void setupData();

    /**
     * Must be overridden in the extending class (can be a no-op) to create a connector-specific CloudFormation stack
     * resource (e.g. DB table) using AWS CDK.
     * @param stack The current CloudFormation stack.
     */
    protected abstract void setupStackData(final Stack stack);

    /**
     * Must be overridden in the extending class to get the lambda function's environment variables key-value pairs
     * (e.g. "spillbucket":"myspillbucket"). See individual connector for expected environment variables.
     * @return Map with parameter key-value pairs.
     */
    protected abstract Map<String, String> getConnectorEnvironmentVars();

    /**
     * Must be overridden in the extending class to get the lambda function's IAM access policy. The latter sets up
     * access to multiple connector-specific AWS services (e.g. DynamoDB, Elasticsearch etc...)
     * @return A policy document object.
     */
    protected abstract PolicyDocument getConnectorAccessPolicy();

    /**
     * Creates a CloudFormation stack to build the infrastructure needed to run the integration tests (e.g., Database
     * instance, Lambda function, etc...). Once the stack is created successfully, the lambda function is registered
     * with Athena.
     * @throws InterruptedException Thread is interrupted during sleep.
     * @throws RuntimeException The CloudFormation stack creation failed.
     */
    @BeforeClass
    protected void createStack()
            throws Exception
    {
        logger.info("------------------------------------------------------");
        logger.info("Create CloudFormation stack: {}", cloudFormationStackName);
        logger.info("------------------------------------------------------");

        final String cloudFormationTemplate = getCloudFormationTemplate();

        try {
            CreateStackRequest createStackRequest = new CreateStackRequest()
                    .withStackName(cloudFormationStackName)
                    .withTemplateBody(cloudFormationTemplate)
                    .withDisableRollback(true)
                    .withCapabilities(Capability.CAPABILITY_NAMED_IAM);
            processCreateStackRequest(createStackRequest);
            setupData();
            createAthenaDataCatalog();
        }
        catch (Exception e) {
            // Delete the partially formed CloudFormation stack.
            deleteStack();
            throw e;
        }
    }

    /**
     * Gets the CloudFormation template (generated programmatically using AWS CDK).
     * @return CloudFormation stack template.
     */
    private String getCloudFormationTemplate()
    {
        final Stack stack = generateStack();
        final String stackTemplate = objectMapper
                .valueToTree(theApp.synth().getStackArtifact(stack.getArtifactId()).getTemplate())
                .toPrettyString();
        logger.info("CloudFormation Template:\n{}: {}", cloudFormationStackName, stackTemplate);

        return stackTemplate;
    }

    /**
     * Generate the CloudFormation stack programmatically using AWS CDK.
     * @return CloudFormation stack object.
     */
    private Stack generateStack()
    {
        setupLambdaFunctionInfo();
        final Stack stack = new ConnectorStack(theApp, cloudFormationStackName, spillBucket, s3Key,
                lambdaFunctionName, lambdaFunctionHandler, getConnectorAccessPolicy(), getConnectorEnvironmentVars());
        // Setup connector specific stack data (e.g. DB table).
        setupStackData(stack);

        return stack;
    }

    /**
     * Sets several variables needed in the creation of the CF Stack (e.g. spillBucket, s3Key, lambdaFunctionHandler).
     */
    private void setupLambdaFunctionInfo()
    {
        try {
            for (String line : Files.readAllLines(Paths.get(CF_TEMPLATE_NAME), StandardCharsets.UTF_8)) {
                if (line.contains(LAMBDA_CODE_URI_TAG)) {
                    spillBucket = line.substring(line.indexOf(LAMBDA_SPILL_BUCKET_PREFIX) +
                            LAMBDA_SPILL_BUCKET_PREFIX.length(), line.lastIndexOf('/'));
                    s3Key = line.substring(line.lastIndexOf('/') + 1);
                }
                else if (line.contains(LAMBDA_HANDLER_TAG)) {
                    lambdaFunctionHandler = line.substring(line.indexOf(LAMBDA_HANDLER_PREFIX) +
                            LAMBDA_HANDLER_PREFIX.length());
                }
            }
        }
        catch (IOException e) {
            logger.error("Unable to retrieve Lambda Function information", e);
        }

        logger.info("Spill Bucket: [{}], S3 Key: [{}], Handler: [{}]", spillBucket, s3Key, lambdaFunctionHandler);
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
     * Creates and registers the lambda function with Athena.
     */
    private void createAthenaDataCatalog()
    {
        CreateDataCatalogRequest createDataCatalogRequest = new CreateDataCatalogRequest()
                .withName(lambdaFunctionName)
                .withType(DataCatalogType.LAMBDA)
                .withParameters(ImmutableMap.of("function", "arn:aws:lambda:function:" + lambdaFunctionName));

        athenaClient.createDataCatalog(createDataCatalogRequest);
    }

    /**
     * Deletes a CloudFormation stack, and the lambda function registered with Athena.
     */
    @AfterClass
    protected void deleteStack()
    {
        logger.info("------------------------------------------------------");
        logger.info("Delete CloudFormation stack: {}", cloudFormationStackName);
        logger.info("------------------------------------------------------");

        try {
            AmazonCloudFormation cloudFormationClient = AmazonCloudFormationClientBuilder.defaultClient();
            DeleteStackRequest request = new DeleteStackRequest().withStackName(cloudFormationStackName);
            cloudFormationClient.deleteStack(request);
            deleteAthenaDataCatalog();
        }
        catch (Exception e) {
            logger.error("Something went wrong... Manual resource cleanup may be needed!!!", e);
        }
    }

    /**
     * Deletes the lambda function registered with Athena.
     */
    private void deleteAthenaDataCatalog()
    {
        athenaClient.deleteDataCatalog(new DeleteDataCatalogRequest().withName(lambdaFunctionName));
    }

    /**
     * Uses the listDatabases Athena API to send a show databases DDL query utilizing the lambda function.
     * @return a list of database objects.
     */
    protected List<Database> listDatabases()
    {
        logger.info("Query: [show databases in {};]", lambdaFunctionName);
        ListDatabasesRequest listDatabasesRequest = new ListDatabasesRequest()
                .withCatalogName(lambdaFunctionName);
        ListDatabasesResult listDatabasesResult = athenaClient.listDatabases(listDatabasesRequest);
        logger.info("Results: [{}]", listDatabasesResult);

        return listDatabasesResult.getDatabaseList();
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
        return athenaClient.startQueryExecution(startQueryExecutionRequest).getQueryExecutionId();
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
