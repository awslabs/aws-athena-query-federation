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
import com.amazonaws.services.athena.model.Datum;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.ListDatabasesRequest;
import com.amazonaws.services.athena.model.ListDatabasesResult;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * The Integration-Tests base class from which all connector-specific integration test modules should subclass.
 */
public abstract class IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    private static final String TEST_CONFIG_WORK_GROUP = "athena_work_group";
    private static final String TEST_CONFIG_RESULT_LOCATION = "athena_result_location";
    private static final String TEST_CONFIG_USER_SETTINGS = "user_settings";
    private static final String ATHENA_QUERY_QUEUED_STATE = "QUEUED";
    private static final String ATHENA_QUERY_RUNNING_STATE = "RUNNING";
    private static final String ATHENA_QUERY_FAILED_STATE = "FAILED";
    private static final String ATHENA_QUERY_CANCELLED_STATE = "CANCELLED";

    // Data for common tests
    protected static final String INTEG_TEST_DATABASE_NAME = "datatypes";
    protected static final String TEST_NULL_TABLE_NAME = "null_table";
    protected static final String TEST_EMPTY_TABLE_NAME = "empty_table";
    protected static final String TEST_DATATYPES_TABLE_NAME = "datatypes_table";
    protected static final int TEST_DATATYPES_INT_VALUE = Integer.MIN_VALUE;
    protected static final short TEST_DATATYPES_SHORT_VALUE = Short.MIN_VALUE;
    protected static final long TEST_DATATYPES_LONG_VALUE = Long.MIN_VALUE;
    protected static final String TEST_DATATYPES_VARCHAR_VALUE = "John Doe";
    protected static final boolean TEST_DATATYPES_BOOLEAN_VALUE = true;
    protected static final float TEST_DATATYPES_SINGLE_PRECISION_VALUE = 1E-37f; // unfortunately, redshift can't handle smaller numbers
    protected static final double TEST_DATATYPES_DOUBLE_PRECISION_VALUE = 1E-307; // unfortunately, redshift can't handle smaller numbers
    protected static final String TEST_DATATYPES_DATE_VALUE = "2013-06-01";
    protected static final String TEST_DATATYPES_TIMESTAMP_VALUE = "2016-06-22T19:10:25";
    protected static final byte[] TEST_DATATYPES_BYTE_ARRAY_VALUE = new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
    protected static final String TEST_DATATYPES_VARCHAR_ARRAY_VALUE = "[(408)-589-5846, (408)-589-5555]";

    private static final long sleepTimeMillis = 5000L;

    private final ConnectorStackProvider connectorStackProvider;
    private final String lambdaFunctionName;
    private final AmazonAthena athenaClient;
    private final TestConfig testConfig;
    private final Optional<ConnectorVpcAttributes> vpcAttributes;
    private final Optional<SecretsManagerCredentials> secretCredentials;
    private final String athenaWorkgroup;
    private final String athenaResultLocation;
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
        athenaResultLocation = getAthenaResultLocation();
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

    private String getAthenaResultLocation()
            throws RuntimeException
    {
        String athenaResultLocation = "s3://" + testConfig.getStringItem(TEST_CONFIG_RESULT_LOCATION).orElseThrow(() ->
                new RuntimeException(TEST_CONFIG_RESULT_LOCATION + " must be specified in test-config.json."));

        logger.info("Athena Result Location: {}", athenaResultLocation);

        return athenaResultLocation;
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
    protected abstract void setUpTableData() throws Exception;

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
    protected void setUp() throws Exception
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
                .withQueryString(query)
                .withResultConfiguration(new ResultConfiguration().withOutputLocation(athenaResultLocation));

        String queryExecutionId = sendAthenaQuery(startQueryExecutionRequest);
        logger.info("Query: [{}], Query Id: [{}]", query, queryExecutionId);
        waitForAthenaQueryResults(queryExecutionId);
        GetQueryResultsResult getQueryResultsResult = getAthenaQueryResults(queryExecutionId);
        //logger.info("Results: [{}]", getQueryResultsResult.toString());

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

    public List<String> fetchDataSelect(String schemaName, String tablename, String lambdaFnName)
            throws RuntimeException
    {
        return processQuery(String.format("select * from   \"lambda:%s\".\"%s\".\"%s\";", lambdaFnName, schemaName, tablename));
    }
    public List<String> fetchDataSelectCountAll(String schemaName, String tablename, String lambdaFnName)
            throws RuntimeException
    {
        return processQuery(String.format("select count(*) from   \"lambda:%s\".\"%s\".\"%s\";", lambdaFnName, schemaName, tablename));
    }
    public List<String> fetchDataWhereClause(String schemaName, String tablename, String lambdaFnName, String whereClauseColumn, String whereClauseValue)
            throws RuntimeException
    {
        return processQuery(String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where \"%s\" = \'%s\' ;", lambdaFnName, schemaName, tablename, whereClauseColumn, whereClauseValue));
    }
    public List<String> fetchDataWhereClauseLIKE(String schemaName, String tablename, String lambdaFnName, String whereClauseColumn, String whereClauseValue)
            throws RuntimeException
    {
        return processQuery(String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where \"%s\" LIKE \'%s\' ;", lambdaFnName, schemaName, tablename, whereClauseColumn, whereClauseValue));
    }
    public List<String> fetchDataGroupBy(String schemaName, String tablename,  String lambdaFnName, String groupByColumn)
            throws RuntimeException
    {
        return processQuery(String.format("select count(\"%s\") from   \"lambda:%s\".\"%s\".\"%s\" group by \"%s\";",
                groupByColumn, lambdaFnName, schemaName, tablename, groupByColumn));
    }
    public List<String> fetchDataGroupByHavingClause(String schemaName, String tablename,  String lambdaFnName,
                                                     String groupByColumn, String groupByColumnValue)
            throws RuntimeException
    {
        return processQuery(String.format("select count(\'%s\') from   \"lambda:%s\".\"%s\".\"%s\" group by %s having %s = \'%s\' ;",
                groupByColumn, lambdaFnName, schemaName, tablename, groupByColumn, groupByColumn, groupByColumnValue));
    }
    public List<String> fetchDataUnion(String schemaName, String tablename1, String tablename2, String lambdaFnName1,
                                       String lambdaFnName2, String whereClauseColumn, String whereClauseValue)
            throws RuntimeException
    {
        return processQuery(String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" union all " +
                        "select * from   \"lambda:%s\".\"%s\".\"%s\" where \"%s\" = \'%s\' ;",
                lambdaFnName1, schemaName, tablename1, lambdaFnName2, schemaName, tablename2, whereClauseColumn, whereClauseValue));
    }
    public List<String> fetchDataDistinct(String lambdaFnName, String schemaName, String tablename,
                                          String distinctColumn, String whereClauseColumn, String whereClauseValue)
    {
        return processQuery(String.format("select distinct (%s) from  \"lambda:%s\".\"%s\".\"%s\" where \"%s\" = %s ;",
                distinctColumn, lambdaFnName, schemaName, tablename, whereClauseColumn, whereClauseValue));
    }
    public List<String> fetchDataJoin(String lambdaFnName1, String schemaName1, String tablename1, String lambdaFnName2,
                                      String schemaName2, String tablename2, String whereClauseColumn1, String whereClauseColumn2)
            throws RuntimeException
    {
        return processQuery(String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" t1, " +
                        " \"lambda:%s\".\"%s\".\"%s\" t2 where t1.\"%s\" = t2.\"%s\" ;",
                lambdaFnName1, schemaName1, tablename1, lambdaFnName2, schemaName2, tablename2, whereClauseColumn1, whereClauseColumn2));
    }
    public float calculateThroughput(String lambdaFnName, String schemaName, String tableName)
    {
        logger.info("Executing calculateThroughput");
        logger.info("Connector Lambda Name:" + lambdaFnName);
        logger.info("Schema Name :" + schemaName);
        logger.info("Table Name :" + tableName);
        // running the query to get total no of records
        List<String> list = fetchDataSelectCountAll(schemaName, tableName, lambdaFnName);
        long numberOfRecords = Long.valueOf(list.get(0).toString());
        logger.info("Total Record count:" + numberOfRecords);
        long startTimeInMillis = System.currentTimeMillis();
        fetchDataSelect(schemaName, tableName, lambdaFnName);
        long endTimeInMillis = System.currentTimeMillis();
        float elapsedSeconds = (float) (endTimeInMillis - startTimeInMillis) / 1000F;
        logger.info("Total time taken in seconds : " + elapsedSeconds);
        float throughput = numberOfRecords / elapsedSeconds;
        logger.info("Total throughput(Records per Second) :" + throughput);
        return throughput;
    }
    public List<String> processQuery(String query)
    {
        List<String> firstColValues = new ArrayList<>();
        skipColumnHeaderRow(startQueryExecution(query).getResultSet().getRows())
                .forEach(row -> firstColValues.add(row.getData().get(0).getVarCharValue()));
        return firstColValues;
    }
    public List<Row> skipColumnHeaderRow(List<Row> rows)
    {
        if (!rows.isEmpty()) {
            rows.remove(0);
        }
        return rows;
    }

    @Test
    public void selectIntegerTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectIntegerTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select int_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<Integer> values = new ArrayList<>();
        rows.forEach(row -> values.add(Integer.parseInt(row.getData().get(0).getVarCharValue().split("\\.")[0])));
        logger.info("Titles: {}", values);
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Integer not found: " + TEST_DATATYPES_INT_VALUE, values.contains(TEST_DATATYPES_INT_VALUE));
    }

    @Test
    public void selectVarcharTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectVarcharTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select varchar_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> values = new ArrayList<>();
        rows.forEach(row -> values.add(row.getData().get(0).getVarCharValue()));
        logger.info("Titles: {}", values);
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Varchar not found: " + TEST_DATATYPES_VARCHAR_VALUE, values.contains(TEST_DATATYPES_VARCHAR_VALUE));
    }

    @Test
    public void selectBooleanTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectBooleanTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select boolean_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<Boolean> values = new ArrayList<>();
        rows.forEach(row -> values.add(Boolean.valueOf(row.getData().get(0).getVarCharValue())));
        logger.info("Titles: {}", values);
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Boolean not found: " + TEST_DATATYPES_BOOLEAN_VALUE, values.contains(TEST_DATATYPES_BOOLEAN_VALUE));
    }

    @Test
    public void selectSmallintTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectSmallintTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select smallint_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<Short> values = new ArrayList<>();
        rows.forEach(row -> values.add(Short.valueOf(row.getData().get(0).getVarCharValue().split("\\.")[0])));
        logger.info("Titles: {}", values);
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Smallint not found: " + TEST_DATATYPES_SHORT_VALUE, values.contains(TEST_DATATYPES_SHORT_VALUE));
    }

    @Test
    public void selectBigintTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectBigintTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select bigint_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<Long> values = new ArrayList<>();
        rows.forEach(row -> values.add(Long.valueOf(row.getData().get(0).getVarCharValue().split("\\.")[0])));
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Bigint not found: " + TEST_DATATYPES_LONG_VALUE, values.contains(TEST_DATATYPES_LONG_VALUE));
    }

    @Test
    public void selectFloat4TypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectFloat4TypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select float4_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<Float> values = new ArrayList<>();
        rows.forEach(row -> values.add(Float.valueOf(row.getData().get(0).getVarCharValue())));
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Float4 not found: " + TEST_DATATYPES_SINGLE_PRECISION_VALUE, values.contains(TEST_DATATYPES_SINGLE_PRECISION_VALUE));
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
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Float8 not found: " + TEST_DATATYPES_DOUBLE_PRECISION_VALUE, values.contains(TEST_DATATYPES_DOUBLE_PRECISION_VALUE));
    }

    @Test
    public void selectDateTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectDateTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select date_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<LocalDate> values = new ArrayList<>();
        rows.forEach(row -> values.add(LocalDate.parse(row.getData().get(0).getVarCharValue())));
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Date not found: " + TEST_DATATYPES_DATE_VALUE, values.contains(LocalDate.parse(TEST_DATATYPES_DATE_VALUE)));
    }

    @Test
    public void selectTimestampTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectTimestampTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select timestamp_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<LocalDateTime> values = new ArrayList<>();
        // for some reason, timestamps lose their 'T'.
        rows.forEach(row -> values.add(LocalDateTime.parse(row.getData().get(0).getVarCharValue().replace(' ', 'T'))));
        logger.info(rows.get(0).getData().get(0).getVarCharValue());
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertTrue("Date not found: " + TEST_DATATYPES_TIMESTAMP_VALUE, values.contains(LocalDateTime.parse(TEST_DATATYPES_TIMESTAMP_VALUE)));
    }

    @Test
    public void selectByteArrayTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectByteArrayTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select byte_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> values = new ArrayList<>();
        rows.forEach(row -> values.add(row.getData().get(0).getVarCharValue()));
        Datum actual = rows.get(0).getData().get(0);
        Datum expected = new Datum();
        expected.setVarCharValue("deadbeef");
        logger.info(rows.get(0).getData().get(0).getVarCharValue());
        assertEquals("Wrong number of DB records found.", 1, values.size());
        String bytestring = actual.getVarCharValue().replace(" ", "");
        assertEquals("Byte[] not found: " + Arrays.toString(TEST_DATATYPES_BYTE_ARRAY_VALUE), expected.getVarCharValue(), bytestring);
    }

    @Test
    public void selectVarcharListTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectVarcharListTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select textarray_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> values = new ArrayList<>();
        rows.forEach(row -> values.add(row.getData().get(0).getVarCharValue()));
        Datum actual = rows.get(0).getData().get(0);
        Datum expected = new Datum();
        expected.setVarCharValue(TEST_DATATYPES_VARCHAR_ARRAY_VALUE);
        logger.info(rows.get(0).getData().get(0).getVarCharValue());
        assertEquals("Wrong number of DB records found.", 1, values.size());
        assertEquals("List not found: " + TEST_DATATYPES_VARCHAR_ARRAY_VALUE, expected, actual);
    }

    @Test
    public void selectNullValueTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectNullValueTest");
        logger.info("--------------------------------------");

        String query = String.format("select int_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_NULL_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        Datum actual = rows.get(0).getData().get(0);
        assertNull("Value not 'null'. Received: " + actual.getVarCharValue(), actual.getVarCharValue());
    }

    @Test
    public void selectEmptyTableTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectEmptyTableTest");
        logger.info("--------------------------------------");

        String query = String.format("select int_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_EMPTY_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        assertTrue("Rows should be empty: " + rows.toString(), rows.isEmpty());
    }
}
