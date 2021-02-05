/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.connectors.athena.jdbc.integ;

import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.stacks.ConnectorWithVpcStack;
import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.services.athena.model.Row;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the DynamoDB connector using the Integration-test module.
 */
public class RedshiftIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftIntegTest.class);

    Map<String, String> userSettings = getUserSettings().orElseThrow(() ->
            new RuntimeException("user_settings attribute must be provided in test-config.json."));
    private final String redshiftDbName = userSettings.get("redshift_db_name");
    private final String redshiftDbPort = userSettings.get("redshift_db_port");
    private final String redshiftDbUsername = userSettings.get("redshift_db_username");
    private final String redshiftDbPassword = userSettings.get("redshift_db_password");
    private final String redshiftTableMovies = userSettings.get("redshift_table_movies");
    private final String redshiftTableBday = userSettings.get("redshift_table_bday");

    private static final long sleepDelayMillis = 120_000L;

    private final String lambdaFunctionName;
    private final String clusterName;
    private final String clusterEndpoint;
    private final Map<String, String> environmentVars;

    public RedshiftIntegTest()
    {
        lambdaFunctionName = getLambdaFunctionName();
        clusterName = "redshift-" + UUID.randomUUID();
        clusterEndpoint = clusterName + userSettings.get("redshift_db_domain_suffix");
        String connectionString = String.format("redshift://jdbc:redshift://%s:%s/%s?user=%s&password=%s",
                clusterEndpoint, redshiftDbPort, redshiftDbName, redshiftDbUsername, redshiftDbPassword);
        String connectionStringTag = lambdaFunctionName + "_connection_string";
        environmentVars = ImmutableMap.of("default", connectionString, connectionStringTag, connectionString);
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
     * Sets up the Redshift Table's CloudFormation stack.
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
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
                .defaultDatabaseName(redshiftDbName)
                .masterUser(Login.builder()
                        .masterUsername(redshiftDbUsername)
                        .masterPassword(SecretValue.plainText(redshiftDbPassword))
                        .build())
                .vpc(Vpc.fromVpcAttributes(stack, "RedshiftVpcConfig", VpcAttributes.builder()
                        .vpcId(vpcAttributes.getVpcId())
                        .privateSubnetIds(vpcAttributes.getPrivateSubnetIds())
                        .availabilityZones(vpcAttributes.getAvailabilityZones())
                        .build()))
                .securityGroups(Collections.singletonList(SecurityGroup
                        .fromSecurityGroupId(stack, "RedshiftVpcSecurityGroup", vpcAttributes.getSecurityGroupId())))
                .build();
    }

    /**
     * Sets up the DB tables used by the tests.
     */
    @Override
    protected void setUpTableData()
    {
        try {
            logger.info("Allowing Redshift cluster to fully warm up - Sleeping for 2 min...");
            Thread.sleep(sleepDelayMillis);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage(), e);
        }
        setUpMoviesTable();
        setUpBdayTable();
    }

    /**
     * Creates the 'movies' table and inserts rows.
     */
    private void setUpMoviesTable()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", redshiftTableMovies);
        logger.info("----------------------------------------------------");

        JdbcTableUtils moviesTable = new JdbcTableUtils(lambdaFunctionName, redshiftTableMovies, environmentVars);
        moviesTable.createTable("year int, title varchar, director varchar, lead varchar");
        moviesTable.insertRow("2014, 'Interstellar', 'Christopher Nolan', 'Matthew McConaughey'");
        moviesTable.insertRow("1986, 'Aliens', 'James Cameron', 'Sigourney Weaver'");

    }

    /**
     * Creates the 'bday' table and inserts rows.
     */
    private void setUpBdayTable()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", redshiftTableBday);
        logger.info("----------------------------------------------------");

        JdbcTableUtils bdayTable = new JdbcTableUtils(lambdaFunctionName, redshiftTableBday, environmentVars);
        bdayTable.createTable("first_name varchar, last_name varchar, birthday date");
        bdayTable.insertRow("'Joe', 'Schmoe', date('2002-05-05')");
        bdayTable.insertRow("'Jane', 'Doe', date('2005-10-12')");
        bdayTable.insertRow("'John', 'Smith', date('2006-02-10')");
    }

    @Test
    public void listDatabasesIntegTest()
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
