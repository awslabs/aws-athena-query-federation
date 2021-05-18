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

import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.data.SecretsManagerCredentials;
import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.amazonaws.athena.connector.integ.providers.ConnectorVpcAttributesProvider;
import com.amazonaws.athena.connector.integ.providers.SecretsManagerCredentialsProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.ListDatabasesRequest;
import com.amazonaws.services.athena.model.ListDatabasesResult;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The Integration-Tests base class from which all connector-specific integration test modules should subclass.
 */
public abstract class IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    private static final String TEST_CONFIG_WORK_GROUP = "athena_work_group";
    private static final String TEST_CONFIG_USER_SETTINGS = "user_settings";
    private static final String ATHENA_QUERY_QUEUED_STATE = "QUEUED";
    private static final String ATHENA_QUERY_RUNNING_STATE = "RUNNING";
    private static final String ATHENA_QUERY_FAILED_STATE = "FAILED";
    private static final String ATHENA_QUERY_CANCELLED_STATE = "CANCELLED";
    private static final long sleepTimeMillis = 5000L;

    private final ConnectorStackProvider connectorStackProvider;
    private final String lambdaFunctionName;
    private final AmazonAthena athenaClient;
    private final TestConfig testConfig;
    private final Optional<ConnectorVpcAttributes> vpcAttributes;
    private final Optional<SecretsManagerCredentials> secretCredentials;
    private final String athenaWorkgroup;
    private CloudFormationClient cloudFormationClient;

    public IntegrationTestBase()
    {
        testConfig = new TestConfig();
        vpcAttributes = ConnectorVpcAttributesProvider.getAttributes(testConfig);
        secretCredentials = SecretsManagerCredentialsProvider.getCredentials(testConfig);
        connectorStackProvider = new ConnectorStackProvider(this.getClass().getSimpleName(), testConfig) {
            @Override
            protected Optional<PolicyDocument> getAccessPolicy()
            {
                return getConnectorAccessPolicy();
            }

            @Override
            protected void setEnvironmentVars(final Map environmentVars)
            {
                setConnectorEnvironmentVars(environmentVars);
            }

            @Override
            protected void setSpecificResource(final Stack stack)
            {
                setUpStackData(stack);
            }
        };

        lambdaFunctionName = connectorStackProvider.getLambdaFunctionName();
        athenaClient = AmazonAthenaClientBuilder.defaultClient();
        athenaWorkgroup = getAthenaWorkgroup();
    }

    /**
     * Gets the athena_work_group from the test-config.json JSON file.
     * @return A String containing the name of the workgroup.
     * @throws RuntimeException The athena_work_group is missing from test-config.json, or its value is empty.
     */
    private String getAthenaWorkgroup()
            throws RuntimeException
    {
        String athenaWorkgroup = testConfig.getStringItem(TEST_CONFIG_WORK_GROUP).orElseThrow(() ->
                new RuntimeException(TEST_CONFIG_WORK_GROUP + " must be specified in test-config.json."));

        logger.info("Athena Workgroup: {}", athenaWorkgroup);

        return athenaWorkgroup;
    }

    /**
     * Public accessor for the framework generate lambda function name used in generating the lambda function.
     * @return The name of the lambda function.
     */
    public String getLambdaFunctionName()
    {
        return lambdaFunctionName;
    }

    /**
     * Public accessor for the VPC attributes used in generating the lambda function.
     * @return Optional VPC attributes object.
     */
    public Optional<ConnectorVpcAttributes> getVpcAttributes()
    {
        return vpcAttributes;
    }

    /**
     * Public accessor for the user_settings attribute (stored in the test-config.json file) that are customizable to
     * any user-specific purpose.
     * @return Optional Map(String, Object) containing all the user attributes as defined in the test configuration file,
     * or an empty Optional if the user_settings attribute does not exist in the file.
     */
    public Optional<Map<String, Object>> getUserSettings()
    {
        return testConfig.getMap(TEST_CONFIG_USER_SETTINGS);
    }

    /**
     * Public accessor for the SecretsManager credentials obtained using the secrets_manager_secret attribute entered
     * in the config file.
     * @return Optional SecretsManager credentials object.
     */
    public Optional<SecretsManagerCredentials> getSecretCredentials()
    {
        return secretCredentials;
    }

    /**
     * Must be overridden in the extending class to setup the DB table (i.e. insert rows into table, etc...)
     */
    protected abstract void setUpTableData();

    /**
     * Must be overridden in the extending class (can be a no-op) to create a connector-specific CloudFormation stack
     * resource (e.g. DB table) using AWS CDK.
     * @param stack The current CloudFormation stack.
     */
    protected abstract void setUpStackData(final Stack stack);

    /**
     * Must be overridden in the extending class (can be a no-op) to set the lambda function's environment variables
     * key-value pairs (e.g. "connection_string":"redshift://jdbc:redshift://..."). See individual connector for the
     * expected environment variables. This method is intended to supplement the test-config.json file environment_vars
     * attribute (see below) for cases where the environment variable cannot be hardcoded.
     */
    protected abstract void setConnectorEnvironmentVars(final Map<String, String> environmentVars);

    /**
     * Must be overridden in the extending class to get the lambda function's IAM access policy. The latter sets up
     * access to multiple connector-specific AWS services (e.g. DynamoDB, Elasticsearch etc...)
     * @return A policy document object.
     */
    protected abstract Optional<PolicyDocument> getConnectorAccessPolicy();

    /**
     * Creates a CloudFormation stack to build the infrastructure needed to run the integration tests (e.g., Database
     * instance, Lambda function, etc...). Once the stack is created successfully, the lambda function is registered
     * with Athena.
     */
    @BeforeClass
    protected void setUp()
    {
        cloudFormationClient = new CloudFormationClient(connectorStackProvider.getStack());
        try {
            cloudFormationClient.createStack();
            setUpTableData();
        }
        catch (Exception e) {
            // Delete the partially formed CloudFormation stack.
            cloudFormationClient.deleteStack();
            throw e;
        }
    }

    /**
     * Deletes a CloudFormation stack, and the lambda function registered with Athena.
     */
    @AfterClass
    protected void cleanUp()
    {
        cloudFormationClient.deleteStack();
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
        String query = String.format("show tables in `%s`.`%s`;", lambdaFunctionName, databaseName);
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
        String query = String.format("describe `%s`.`%s`.`%s`;", lambdaFunctionName, databaseName, tableName);
        Map<String, String> schema = new HashMap<>();
        startQueryExecution(query).getResultSet().getRows()
                .forEach(row -> {
                    String property = row.getData().get(0).getVarCharValue();
                    String[] columnProperties = property.split("\t");
                    if (columnProperties.length == 2) {
                        schema.put(columnProperties[0], columnProperties[1]);
                    }
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
                .withWorkGroup(athenaWorkgroup)
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
                    continue;
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage(), e);
                }
            }
            else if (queryState.equals(ATHENA_QUERY_FAILED_STATE) || queryState.equals(ATHENA_QUERY_CANCELLED_STATE)) {
                throw new RuntimeException(getQueryExecutionResult
                        .getQueryExecution().getStatus().getStateChangeReason());
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
