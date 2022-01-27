/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.integ;

import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.data.SecretsManagerCredentials;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.services.athena.model.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.SecretValue;
import software.amazon.awscdk.core.Stack;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.DescribeStacksResult;
import com.amazonaws.services.cloudformation.model.DescribeStacksRequest;
import com.amazonaws.services.cloudformation.model.Output;
import software.amazon.awscdk.cloudformation.include.CfnInclude;

import software.amazon.awscdk.services.iam.PolicyDocument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the SAP HANA (JDBC) connector using the Integration-test module.
 */
public class SAPHANAIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SAPHANAIntegTest.class);

    private static final long sleepDelayMillis = 900_000L;

    private final App theApp;
    private final String username;
    private final String password;
    private final String saphanaDbName;
    private final String saphanaDbPort;
    private final String saphanaTableMovies;
    private final String saphanaTableBday;
    private final String saphanaSoftware;
    private final String saphanaInstanceRole;
    private final String saphanaAMI;
    private final String lambdaFunctionName;
    private final String clusterName;
    private String saphanahost;
    private final Map<String, String> environmentVars;

    private CloudFormationClient cloudFormationClient;

    public SAPHANAIntegTest()
    {
        theApp = new App();
        SecretsManagerCredentials secretsManagerCredentials = getSecretCredentials().orElseThrow(() ->
                new RuntimeException("secrets_manager_secret must be provided in test-config.json file."));
        username = secretsManagerCredentials.getUsername();
        password = secretsManagerCredentials.getPassword();
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json file."));
        saphanaDbName = (String) userSettings.get("saphana_db_name");
        saphanaDbPort = (String) userSettings.get("saphana_db_port");
        saphanaTableMovies = (String) userSettings.get("saphana_table_movies");
        saphanaTableBday = (String) userSettings.get("saphana_table_bday");
        saphanaSoftware = (String) userSettings.get("saphana_software_s3_bucket");
        saphanaInstanceRole = (String) userSettings.get("saphana_ec2_iam_role");
        saphanaAMI = (String) userSettings.get("saphana_ec2_ami");
        lambdaFunctionName = getLambdaFunctionName();
        clusterName = "integ-saphana-db-" + UUID.randomUUID();
        environmentVars = new HashMap<>();
    }

    /**
     * Creates a SAP HANA database (Express Edition) used for the integration tests.
     */
    @BeforeClass
    @Override
    protected void setUp()
    {
        cloudFormationClient = new CloudFormationClient(theApp, getSAPHANAStack());
        try {
            // Create the CloudFormation stack for the SAP HANA database.
            cloudFormationClient.createStack();
            
            try {
                logger.info("Allowing SAP HANA to fully warm up - Sleeping for 15 min...");
                Thread.sleep(sleepDelayMillis);
            }
            catch (InterruptedException e) {
                throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage(), e);
            }
            
            // Get private DNS of HANA instance
            getHostname();
            // Set the environment variables needed for the Lambda.
            setEnvironmentVars();
            // Create the DB schema in the newly created DB cluster used for the integration tests.
            createDbSchema();
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
     * Gets the HANA Private DNS needed for the Lambda.
     * All exceptions thrown here will be caught in the calling function.
     */
    private void getHostname()
    {
        DescribeStacksRequest describeStacksRequest = new DescribeStacksRequest();
        describeStacksRequest.withStackName(clusterName);
        DescribeStacksResult describeStacksResult = AmazonCloudFormationClientBuilder.defaultClient().describeStacks(describeStacksRequest);
        List<Output> outputs = describeStacksResult.getStacks().get(0).getOutputs();
        Optional<Output> output = outputs.stream()
            .filter(index -> index.getOutputKey().equals("DnsName"))
            .findFirst();
            
        saphanahost = output.map(Output::getOutputValue).get();
        logger.info("DNS: " + saphanahost);
    }

    /**
     * Deletes a CloudFormation stack for the SAP HANA database.
     */
    @AfterClass
    @Override
    protected void cleanUp()
    {
        // Invoke the framework's cleanUp().
        super.cleanUp();
        // Delete the CloudFormation stack for the SAP HANA database.
        cloudFormationClient.deleteStack();
    }

    /**
     * Gets the CloudFormation stack for the SAP HANA database.
     * @return Stack object for the SAP HANA database.
     */
    private Stack getSAPHANAStack()
    {
        Stack stack = Stack.Builder.create(theApp, clusterName).build();

        ConnectorVpcAttributes vpcAttributes = getVpcAttributes()
                .orElseThrow(() -> new RuntimeException("vpc_configuration must be specified in test-config.json"));
                
        CfnInclude mytemplate = CfnInclude.Builder.create(stack, "Template")
            	.templateFile("etc/hana.yaml")
            	.parameters(new HashMap<String, String>() {{
            			put("SubnetParameter", vpcAttributes.getPrivateSubnetIds().get(0));
            			put("SecurityGroupParameter", vpcAttributes.getSecurityGroupId());
            			put("SoftwareParameter", saphanaSoftware);
            			put("InstanceRoleParameter", saphanaInstanceRole);
            			put("AMIParameter", saphanaAMI);
            			put("DBPasswordParameter", password);
            	}})
            	.build();

        return stack;
    }

    /**
     * Sets the environment variables needed for the Lambda.
     * @param endpoint Contains the DB hostname and port information.
     */
    private void setEnvironmentVars()
    {
        String connectionString = String.format("saphana://jdbc:sap://%s:%s/?user=%s&password=%s",
                saphanahost, saphanaDbPort, username, password);
        String connectionStringTag = lambdaFunctionName + "_connection_string";
        environmentVars.put("default", connectionString);
        environmentVars.put(connectionStringTag, connectionString);
    }

    /**
     * Creates the DB schema used for the integration tests.
     */
    private void createDbSchema()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB Schema: {}", saphanaDbName);
        logger.info("----------------------------------------------------");

        JdbcTableUtils jdbcUtils =
                new JdbcTableUtils(lambdaFunctionName, new TableName(saphanaDbName, saphanaTableMovies),
                        environmentVars);
        jdbcUtils.createDbSchema();
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        // No connector-specific policy document needed
        return Optional.empty();
    }

    /**
     * Sets the environment variables for the Lambda function.
     */
    @Override
    protected void setConnectorEnvironmentVars(final Map environmentVars)
    {
        environmentVars.putAll(this.environmentVars);
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
        setUpMoviesTable();
        setUpBdayTable();
    }

    /**
     * Creates the 'movies' table and inserts rows.
     */
    private void setUpMoviesTable()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", saphanaTableMovies);
        logger.info("----------------------------------------------------");

        JdbcTableUtils moviesTable = new JdbcTableUtils(lambdaFunctionName,
                new TableName(saphanaDbName, saphanaTableMovies), environmentVars);
        moviesTable.createTable("year int, title varchar(100), director varchar(100), lead varchar(100)");
        moviesTable.insertRow("2014, 'Interstellar', 'Christopher Nolan', 'Matthew McConaughey'");
        moviesTable.insertRow("1986, 'Aliens', 'James Cameron', 'Sigourney Weaver'");

    }

    /**
     * Creates the 'bday' table and inserts rows.
     */
    private void setUpBdayTable()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", saphanaTableBday);
        logger.info("----------------------------------------------------");

        JdbcTableUtils bdayTable = new JdbcTableUtils(lambdaFunctionName,
                new TableName(saphanaDbName, saphanaTableBday), environmentVars);
        bdayTable.createTable("first_name varchar(100), last_name varchar(100), birthday date");
        bdayTable.insertRow("'Joe', 'Schmoe', '2002-05-05'");
        bdayTable.insertRow("'Jane', 'Doe', '2005-10-12'");
        bdayTable.insertRow("'John', 'Smith', '2006-02-10'");
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains(saphanaDbName));
    }

    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(saphanaDbName);
        logger.info("Tables: {}", tableNames);
        assertEquals("Incorrect number of tables found.", 2, tableNames.size());
        assertTrue(String.format("Table not found: %s.", saphanaTableMovies),
                tableNames.contains(saphanaTableMovies));
        assertTrue(String.format("Table not found: %s.", saphanaTableBday),
                tableNames.contains(saphanaTableBday));
    }

    @Test
    public void listTableSchemaIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listTableSchemaIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(saphanaDbName, saphanaTableMovies);
        schema.remove("partition_name");
        schema.remove("partition_schema_name");
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 5, schema.size());
        assertTrue("Column not found: year", schema.containsKey("YEAR"));
        assertEquals("Wrong column type for year.", "int", schema.get("YEAR"));
        assertTrue("Column not found: title", schema.containsKey("TITLE"));
        assertEquals("Wrong column type for title.", "varchar", schema.get("TITLE"));
        assertTrue("Column not found: director", schema.containsKey("DIRECTOR"));
        assertEquals("Wrong column type for director.", "varchar", schema.get("DIRECTOR"));
        assertTrue("Column not found: lead", schema.containsKey("LEAD"));
        assertEquals("Wrong column type for lead.", "varchar", schema.get("LEAD"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select title from %s.%s.%s where year > 2000;",
                lambdaFunctionName, saphanaDbName, saphanaTableMovies);
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
    public void selectColumnBetweenDatesIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnBetweenDatesIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format(
                "select first_name from %s.%s.%s where birthday between date('2003-1-1') and date('2005-12-31');",
                lambdaFunctionName, saphanaDbName, saphanaTableBday);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> names = new ArrayList<>();
        rows.forEach(row -> names.add(row.getData().get(0).getVarCharValue()));
        logger.info("Names: {}", names);
        assertEquals("Wrong number of DB records found.", 1, names.size());
        assertTrue("Name not found: Jane.", names.contains("Jane"));
    }
}
