/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
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
package com.amazonaws.athena.connector.integ;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
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
import java.util.ArrayList;
import java.util.HashMap;
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
    private static final String LAMBDA_SPILL_BUCKET_TAG = "spill_bucket";
    private static final String LAMBDA_SPILL_PREFIX_TAG = "spill_prefix";
    private static final String LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG = "disable_spill_encryption";
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
                randomUuid.toString().replace('-', '_');
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
     * Must be overridden in the extending class to set the lambda function's environment variables key-value pairs
     * (e.g. "spill_bucket":"myspillbucket"). See individual connector for expected environment variables. This method
     * can be a no-op in the extending class since some environment variables are set by default (spill_bucket,
     * spill_prefix, and disable_spill_encryption).
     */
    protected abstract void setConnectorEnvironmentVars(final Map<String, String> environmentVars);

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
     */
    @BeforeClass
    protected void createStack()
    {
        logger.info("------------------------------------------------------");
        logger.info("Create CloudFormation stack: {}", cloudFormationStackName);
        logger.info("------------------------------------------------------");

        try {
            final String cloudFormationTemplate = getCloudFormationTemplate();

            CreateStackRequest createStackRequest = new CreateStackRequest()
                    .withStackName(cloudFormationStackName)
                    .withTemplateBody(cloudFormationTemplate)
                    .withDisableRollback(true)
                    .withCapabilities(Capability.CAPABILITY_NAMED_IAM);
            processCreateStackRequest(createStackRequest);
            setupData();
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
     * @throws RuntimeException CloudFormation template (packaged.yaml) was not found.
     */
    private void setupLambdaFunctionInfo()
            throws RuntimeException
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
            throw new RuntimeException("Lambda connector has not been packaged via `sam package` (see README).", e);
        }

        logger.info("Spill Bucket: [{}], S3 Key: [{}], Handler: [{}]", spillBucket, s3Key, lambdaFunctionHandler);
    }

    /**
     * Gets the specific connectors' environment variables.
     * @return A Map containing the environment variables key-value pairs.
     */
    private Map getConnectorEnvironmentVars()
    {
        final Map<String, String> environmentVars = new HashMap<>();
        // Have the connector set specific environment variables first.
        setConnectorEnvironmentVars(environmentVars);

        // Check for missing spill_bucket
        if (!environmentVars.containsKey(LAMBDA_SPILL_BUCKET_TAG)) {
            // Add missing spill_bucket environment variable
            environmentVars.put(LAMBDA_SPILL_BUCKET_TAG, spillBucket);
        }

        // Check for missing spill_prefix
        if (!environmentVars.containsKey(LAMBDA_SPILL_PREFIX_TAG)) {
            // Add missing spill_prefix environment variable
            environmentVars.put(LAMBDA_SPILL_PREFIX_TAG, "athena-spill");
        }

        // Check for missing disable_spill_encryption environment variable
        if (!environmentVars.containsKey(LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG)) {
            // Add missing disable_spill_encryption environment variable
            environmentVars.put(LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG, "false");
        }

        return environmentVars;
    }

    /**
     * Processes the creation of a CloudFormation stack including polling of the stack's status while in progress.
     * @param createStackRequest Request used to generate the CloudFormation stack.
     * @throws RuntimeException The CloudFormation stack creation failed.
     */
    private void processCreateStackRequest(CreateStackRequest createStackRequest)
            throws RuntimeException
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
                try {
                    Thread.sleep(sleepTimeMillis);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage());
                }
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
        }
        catch (Exception e) {
            logger.error("Something went wrong... Manual resource cleanup may be needed!!!", e);
        }
    }

    /**
     * Uses the listDatabases Athena API to list databases for the data source utilizing the lambda function.
     * @return a list of database names.
     */
    public List<String> listDatabases()
    {
        logger.info("listDatabases({})", lambdaFunctionName);
        ListDatabasesRequest listDatabasesRequest = new ListDatabasesRequest()
                .withCatalogName(lambdaFunctionName);

        ListDatabasesResult listDatabasesResult = athenaClient.listDatabases(listDatabasesRequest);
        logger.info("Results: [{}]", listDatabasesResult);

        List<String> dbNames = new ArrayList<>();
        listDatabasesResult.getDatabaseList().forEach(db -> dbNames.add(db.getName()));

        return dbNames;
    }

    /**
     * Uses the startQueryExecution Athena API to process a "show tables" query utilizing the lambda function.
     * @param databaseName The name of the database.
     * @return A list of database table names.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    public List<String> listTables(String databaseName)
            throws RuntimeException
    {
        String query = String.format("show tables in %s.%s;", lambdaFunctionName, databaseName);
        List<String> tableNames = new ArrayList<>();
        startQueryExecution(query).getResultSet().getRows()
                .forEach(row -> tableNames.add(row.getData().get(0).getVarCharValue()));

        return tableNames;
    }

    /**
     * Uses the startQueryExecution Athena API to process a "describe table" query utilizing the lambda function.
     * @param databaseName The name of the database.
     * @param tableName The name of the database table.
     * @return A Map of the table column names and their associated types.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    public Map<String, String> describeTable(String databaseName, String tableName)
            throws RuntimeException
    {
        String query = String.format("describe %s.%s.%s;", lambdaFunctionName, databaseName, tableName);
        Map<String, String> schema = new HashMap<>();
        startQueryExecution(query).getResultSet().getRows()
                .forEach(row -> {
                    String property = row.getData().get(0).getVarCharValue();
                    schema.put(property.substring(0, property.indexOf('\t')),
                            property.substring(property.indexOf('\t') + 1));
                });

        return schema;
    }

    /**
     * Sends a DB query via Athena and returns the query results.
     * @param query - The query string to be processed by Athena.
     * @return The query results object containing the metadata and row information.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    public GetQueryResultsResult startQueryExecution(String query)
            throws RuntimeException
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
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    private void waitForAthenaQueryResults(String queryExecutionId)
            throws RuntimeException
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
                try {
                    Thread.sleep(sleepTimeMillis);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage());
                }
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
