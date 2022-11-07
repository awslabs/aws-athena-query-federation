/*-
 * #%L
 * athena-redshift
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
package com.amazonaws.athena.connectors.redshift.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.data.SecretsManagerCredentials;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.integ.JdbcTableUtils;
import com.amazonaws.services.athena.model.Datum;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.amazonaws.services.redshift.model.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.SecretValue;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcAttributes;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.redshift.Cluster;
import software.amazon.awscdk.services.redshift.ClusterType;
import software.amazon.awscdk.services.redshift.Login;
import software.amazon.awscdk.services.redshift.NodeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the Redshift (JDBC) connector using the Integration-test module.
 */
public class RedshiftIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftIntegTest.class);

    private static final long sleepDelayMillis = 60_000L;

    private final App theApp;
    private final String username;
    private final String password;
    private final String redshiftDbName;
    private final String redshiftDbPort;
    private final String redshiftTableMovies;
    private final String redshiftTableBday;
    private final String lambdaFunctionName;
    private final String clusterName;
    private final Map<String, String> environmentVars;
    private final DatabaseConnectionInfo databaseConnectionInfo;

    private CloudFormationClient cloudFormationClient;

    public RedshiftIntegTest()
    {
        theApp = new App();
        SecretsManagerCredentials secretsManagerCredentials = getSecretCredentials().orElseThrow(() ->
                new RuntimeException("secrets_manager_secret must be provided in test-config.json file."));
        username = secretsManagerCredentials.getUsername();
        password = secretsManagerCredentials.getPassword();
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json file."));
        redshiftDbName = (String) userSettings.get("redshift_db_name");
        redshiftDbPort = (String) userSettings.get("redshift_db_port");
        redshiftTableMovies = (String) userSettings.get("redshift_table_movies");
        redshiftTableBday = (String) userSettings.get("redshift_table_bday");
        lambdaFunctionName = getLambdaFunctionName();
        clusterName = "integ-redshift-cluster-" + UUID.randomUUID();
        environmentVars = new HashMap<>();
        databaseConnectionInfo = new DatabaseConnectionInfo(REDSHIFT_DRIVER_CLASS, REDSHIFT_DEFAULT_PORT);
    }

    /**
     * Creates a Redshift cluster used for the integration tests.
     */
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        cloudFormationClient = new CloudFormationClient(theApp, getRedshiftStack());
        try {
            // Create the CloudFormation stack for the Redshift cluster.
            cloudFormationClient.createStack();
            // Get DB cluster's host and port information and set the environment variables needed for the Lambda.
            setEnvironmentVars(getClusterData());
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
     * Deletes a CloudFormation stack for the Redshift cluster.
     */
    @AfterClass
    @Override
    protected void cleanUp()
    {
        // Invoke the framework's cleanUp().
        super.cleanUp();
        // Delete the CloudFormation stack for the Redshift cluster.
        cloudFormationClient.deleteStack();
    }

    /**
     * Gets the CloudFormation stack for the Redshift cluster.
     * @return Stack object for the Redshift cluster.
     */
    private Stack getRedshiftStack()
    {
        Stack stack = Stack.Builder.create(theApp, clusterName).build();

        ConnectorVpcAttributes vpcAttributes = getVpcAttributes()
                .orElseThrow(() -> new RuntimeException("vpc_configuration must be specified in test-config.json"));

        Cluster.Builder.create(stack, "RedshiftCluster")
                .publiclyAccessible(Boolean.TRUE)
                .removalPolicy(RemovalPolicy.DESTROY)
                .encrypted(Boolean.FALSE)
                .port(Integer.parseInt(redshiftDbPort))
                .clusterName(clusterName)
                .clusterType(ClusterType.SINGLE_NODE)
                .nodeType(NodeType.DC2_LARGE)
                .numberOfNodes(1)
                .defaultDatabaseName("public")
                .masterUser(Login.builder()
                        .masterUsername(username)
                        .masterPassword(SecretValue.plainText(password))
                        .build())
                .vpc(Vpc.fromVpcAttributes(stack, "RedshiftVpcConfig", VpcAttributes.builder()
                        .vpcId(vpcAttributes.getVpcId())
                        .privateSubnetIds(vpcAttributes.getPrivateSubnetIds())
                        .availabilityZones(vpcAttributes.getAvailabilityZones())
                        .build()))
                .securityGroups(Collections.singletonList(SecurityGroup
                        .fromSecurityGroupId(stack, "RedshiftVpcSecurityGroup", vpcAttributes.getSecurityGroupId())))
                .build();

        return stack;
    }

    /**
     * Gets the Redshift cluster endpoint information needed for the Lambda.
     * All exceptions thrown here will be caught in the calling function.
     */
    private Endpoint getClusterData()
    {
        AmazonRedshift redshiftClient = AmazonRedshiftClientBuilder.defaultClient();
        try {
            DescribeClustersResult clustersResult = redshiftClient.describeClusters(new DescribeClustersRequest()
                    .withClusterIdentifier(clusterName));
            return clustersResult.getClusters().get(0).getEndpoint();
        }
        finally {
            redshiftClient.shutdown();
        }
    }

    /**
     * Sets the environment variables needed for the Lambda.
     * @param endpoint Contains the DB hostname and port information.
     */
    private void setEnvironmentVars(Endpoint endpoint)
    {
        String connectionString = String.format("redshift://jdbc:redshift://%s:%s/public?user=%s&password=%s",
                endpoint.getAddress(), endpoint.getPort(), username, password);
        String connectionStringTag = lambdaFunctionName + "_connection_string";
        environmentVars.put("default", connectionString);
        environmentVars.put(connectionStringTag, connectionString);
    }

    /**
     * Creates the DB schema used for the integration tests.
     */
    private void createDbSchema()
            throws Exception
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB Schema: {}", redshiftDbName);
        logger.info("----------------------------------------------------");

        JdbcTableUtils jdbcUtils = new JdbcTableUtils(lambdaFunctionName, new TableName(redshiftDbName, redshiftTableMovies), environmentVars, null, REDSHIFT_NAME);
        jdbcUtils.createDbSchema(databaseConnectionInfo);

        jdbcUtils = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME), environmentVars, null, REDSHIFT_NAME);
        jdbcUtils.createDbSchema(databaseConnectionInfo);
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
            throws Exception
    {
        try {
            logger.info("Allowing Redshift cluster to fully warm up - Sleeping for 1 min...");
            Thread.sleep(sleepDelayMillis);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage(), e);
        }
        setUpMoviesTable();
        setUpBdayTable();
        setUpDatatypesTable();
        setUpNullTable();
        setUpEmptyTable();
    }

    /**
     * Creates the 'movies' table and inserts rows.
     */
    private void setUpMoviesTable()
            throws Exception
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", redshiftTableMovies);
        logger.info("----------------------------------------------------");

        JdbcTableUtils moviesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(redshiftDbName, redshiftTableMovies), environmentVars, null, REDSHIFT_NAME);
        moviesTable.createTable("year int, title varchar, director varchar, lead varchar", databaseConnectionInfo);
        moviesTable.insertRow("2014, 'Interstellar', 'Christopher Nolan', 'Matthew McConaughey'", databaseConnectionInfo);
        moviesTable.insertRow("1986, 'Aliens', 'James Cameron', 'Sigourney Weaver'", databaseConnectionInfo);

    }

    /**
     * Creates the 'bday' table and inserts rows.
     */
    private void setUpBdayTable()
            throws Exception
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", redshiftTableBday);
        logger.info("----------------------------------------------------");

        JdbcTableUtils bdayTable = new JdbcTableUtils(lambdaFunctionName,
                new TableName(redshiftDbName, redshiftTableBday), environmentVars, null, REDSHIFT_NAME);
        bdayTable.createTable("first_name varchar, last_name varchar, birthday date", databaseConnectionInfo);
        bdayTable.insertRow("'Joe', 'Schmoe', date('2002-05-05')", databaseConnectionInfo);
        bdayTable.insertRow("'Jane', 'Doe', date('2005-10-12')", databaseConnectionInfo);
        bdayTable.insertRow("'John', 'Smith', date('2006-02-10')", databaseConnectionInfo);
    }

    /**
     * Creates the 'datatypes' table and inserts rows.
     */
    protected void setUpDatatypesTable()
            throws Exception
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", TEST_DATATYPES_TABLE_NAME);
        logger.info("----------------------------------------------------");

        JdbcTableUtils datatypesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME), environmentVars, null, REDSHIFT_NAME);
        datatypesTable.createTable("int_type INTEGER, smallint_type SMALLINT, bigint_type BIGINT, varchar_type CHARACTER VARYING(255), boolean_type BOOLEAN, float4_type REAL, float8_type DOUBLE PRECISION, date_type DATE, timestamp_type TIMESTAMP, byte_type VARBYTE(4)", databaseConnectionInfo);
        String row = String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
                TEST_DATATYPES_INT_VALUE,
                TEST_DATATYPES_SHORT_VALUE,
                TEST_DATATYPES_LONG_VALUE,
                "'" + TEST_DATATYPES_VARCHAR_VALUE + "'",
                TEST_DATATYPES_BOOLEAN_VALUE,
                TEST_DATATYPES_SINGLE_PRECISION_VALUE,
                TEST_DATATYPES_DOUBLE_PRECISION_VALUE,
                "'" + TEST_DATATYPES_DATE_VALUE + "'",
                "'" + TEST_DATATYPES_TIMESTAMP_VALUE + "'",
                "from_hex('deadbeef')");
        datatypesTable.insertRow(row, databaseConnectionInfo);
    }

    @Test
    public void selectVarcharListTypeTest()
    {
        // not supported!
    }

    /**
     * Creates the 'null_table' table and inserts rows.
     */
    protected void setUpNullTable()
            throws Exception
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", TEST_NULL_TABLE_NAME);
        logger.info("----------------------------------------------------");

        JdbcTableUtils datatypesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_NULL_TABLE_NAME), environmentVars, null, REDSHIFT_NAME);
        datatypesTable.createTable("int_type INTEGER", databaseConnectionInfo);
        datatypesTable.insertRow("NULL", databaseConnectionInfo);
    }

    /**
     * Creates the 'empty_table' table and inserts rows.
     */
    protected void setUpEmptyTable()
            throws Exception
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", TEST_EMPTY_TABLE_NAME);
        logger.info("----------------------------------------------------");

        JdbcTableUtils datatypesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_EMPTY_TABLE_NAME), environmentVars, null, REDSHIFT_NAME);
        datatypesTable.createTable("int_type INTEGER", databaseConnectionInfo);
    }

    @Test
    public void listDatabasesIntegTest()
            throws Exception
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains(redshiftDbName));
    }

    @Test
    public void listTablesIntegTest()
            throws Exception
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(redshiftDbName);
        logger.info("Tables: {}", tableNames);
        assertEquals("Incorrect number of tables found.", 2, tableNames.size());
        assertTrue(String.format("Table not found: %s.", redshiftTableMovies),
                tableNames.contains(redshiftTableMovies));
        assertTrue(String.format("Table not found: %s.", redshiftTableBday),
                tableNames.contains(redshiftTableBday));
    }

    @Test
    public void listTableSchemaIntegTest()
            throws Exception
    {
        logger.info("--------------------------------------");
        logger.info("Executing listTableSchemaIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(redshiftDbName, redshiftTableMovies);
        schema.remove("partition_name");
        schema.remove("partition_schema_name");
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 4, schema.size());
        assertTrue("Column not found: year", schema.containsKey("year"));
        assertEquals("Wrong column type for year.", "int", schema.get("year"));
        assertTrue("Column not found: title", schema.containsKey("title"));
        assertEquals("Wrong column type for title.", "varchar", schema.get("title"));
        assertTrue("Column not found: director", schema.containsKey("director"));
        assertEquals("Wrong column type for director.", "varchar", schema.get("director"));
        assertTrue("Column not found: lead", schema.containsKey("lead"));
        assertEquals("Wrong column type for lead.", "varchar", schema.get("lead"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
            throws Exception
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select title from %s.%s.%s where year > 2000;",
                lambdaFunctionName, redshiftDbName, redshiftTableMovies);
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
            throws Exception
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnBetweenDatesIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format(
                "select first_name from %s.%s.%s where birthday between date('2003-1-1') and date('2005-12-31');",
                lambdaFunctionName, redshiftDbName, redshiftTableBday);
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
