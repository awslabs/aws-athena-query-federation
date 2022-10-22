/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.data.SecretsManagerCredentials;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.integ.JdbcTableUtils;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.amazonaws.services.rds.model.Endpoint;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcAttributes;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.rds.Credentials;
import software.amazon.awscdk.services.rds.DatabaseInstance;
import software.amazon.awscdk.services.rds.DatabaseInstanceEngine;
import software.amazon.awscdk.services.rds.PostgresEngineVersion;
import software.amazon.awscdk.services.rds.PostgresInstanceEngineProps;
import software.amazon.awscdk.services.rds.StorageType;
import software.amazon.awscdk.services.secretsmanager.Secret;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the PostGreSql (JDBC) connector using the Integration-test module.
 */
public class PostGreSqlIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(PostGreSqlIntegTest.class);

    private final App theApp;
    private final String secretArn;
    private final String username;
    private final String password;
    private final String postgresDbName;
    private final Number postgresDbPort;
    private final String postgresTableMovies;
    private final String postgresTableBday;
    private final String lambdaFunctionName;
    private final String dbInstanceName;
    private final Map<String, String> environmentVars;
    private final Map<String, String> jdbcProperties;
    private final DatabaseConnectionInfo databaseConnectionInfo;

    private CloudFormationClient cloudFormationClient;

    public PostGreSqlIntegTest()
    {
        theApp = new App();
        SecretsManagerCredentials secretsManagerCredentials = getSecretCredentials().orElseThrow(() ->
                new RuntimeException("secrets_manager_secret must be provided in test-config.json file."));
        secretArn = secretsManagerCredentials.getArn();
        username = secretsManagerCredentials.getUsername();
        password = secretsManagerCredentials.getPassword();
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json file."));
        postgresDbName = (String) userSettings.get("postgres_db_name");
        postgresDbPort = (Number) userSettings.get("postgres_db_port");
        postgresTableMovies = (String) userSettings.get("postgres_table_movies");
        postgresTableBday = (String) userSettings.get("postgres_table_bday");
        lambdaFunctionName = getLambdaFunctionName();
        dbInstanceName = "integ-postgres-instance-" + UUID.randomUUID();
        environmentVars = new HashMap<>();
        jdbcProperties = ImmutableMap.of("databaseTerm", "SCHEMA");
        databaseConnectionInfo = new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, (Integer) postgresDbPort);
    }

    /**
     * Creates a PostGreSql RDS Instance used for the integration tests.
     */
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        cloudFormationClient = new CloudFormationClient(theApp, getPostGreSqlStack());
        try {
            // Create the CloudFormation stack for the PostGreSql DB instance.
            cloudFormationClient.createStack();
            // Get DB instance's host and port information and set the environment variables needed for the Lambda.
            setEnvironmentVars(getInstanceData());
            // Create the DB schema in the newly created DB instance used for the integration tests.
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
     * Deletes a CloudFormation stack for the PostGreSql RDS Instance.
     */
    @AfterClass
    @Override
    protected void cleanUp()
    {
        // Invoke the framework's cleanUp().
        super.cleanUp();
        // Delete the CloudFormation stack for the PostGreSql DB instance.
        cloudFormationClient.deleteStack();
    }

    /**
     * Gets the CloudFormation stack for the PostGreSql RDS Instance.
     * @return Stack object for the PostGreSql RDS Instance.
     */
    private Stack getPostGreSqlStack()
    {
        Stack stack = Stack.Builder.create(theApp, dbInstanceName).build();

        ConnectorVpcAttributes vpcAttributes = getVpcAttributes()
                .orElseThrow(() -> new RuntimeException("vpc_configuration must be specified in test-config.json"));

        DatabaseInstance.Builder.create(stack, "PostGreSqlInstance")
                .publiclyAccessible(Boolean.TRUE)
                .removalPolicy(RemovalPolicy.DESTROY)
                .deleteAutomatedBackups(Boolean.TRUE)
                .storageEncrypted(Boolean.FALSE)
                .port(postgresDbPort)
                .instanceIdentifier(dbInstanceName)
                .engine(DatabaseInstanceEngine.postgres(PostgresInstanceEngineProps.builder()
                        .version(PostgresEngineVersion.VER_12)
                        .build()))
                .storageType(StorageType.GP2)
                .allocatedStorage(20)
                .instanceType(new InstanceType("t2.micro"))
                .credentials(Credentials.fromSecret(Secret
                        .fromSecretCompleteArn(stack, "PostGreSqlSecret", secretArn)))
                .vpc(Vpc.fromVpcAttributes(stack, "PostGreSqlVpcConfig", VpcAttributes.builder()
                        .vpcId(vpcAttributes.getVpcId())
                        .privateSubnetIds(vpcAttributes.getPrivateSubnetIds())
                        .availabilityZones(vpcAttributes.getAvailabilityZones())
                        .build()))
                .securityGroups(Collections.singletonList(SecurityGroup
                        .fromSecurityGroupId(stack, "PostGreSqlVpcSecurityGroup",
                                vpcAttributes.getSecurityGroupId())))
                .build();

        return stack;
    }

    /**
     * Gets the PostGreSql RDS Instance endpoint information and generates the environment variables needed for the
     * Lambda. All exceptions thrown here will be caught in the calling function.
     */
    private Endpoint getInstanceData()
    {
        AmazonRDS rdsClient = AmazonRDSClientBuilder.defaultClient();
        try {
            DescribeDBInstancesResult instancesResult = rdsClient.describeDBInstances(new DescribeDBInstancesRequest()
                    .withDBInstanceIdentifier(dbInstanceName));
            return instancesResult.getDBInstances().get(0).getEndpoint();
        }
        finally {
            rdsClient.shutdown();
        }
    }

    /**
     * Sets the environment variables needed for the Lambda.
     * @param endpoint Contains the DB hostname and port information.
     */
    private void setEnvironmentVars(Endpoint endpoint)
    {
        String connectionString = String.format("postgres://jdbc:postgresql://%s:%s/postgres?user=%s&password=%s",
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
        logger.info("Setting up DB Schema: {}", postgresDbName);
        logger.info("----------------------------------------------------");

        JdbcTableUtils jdbcUtils = new JdbcTableUtils(lambdaFunctionName, new TableName(postgresDbName, postgresTableMovies), environmentVars, jdbcProperties, POSTGRES_NAME);
        jdbcUtils.createDbSchema(databaseConnectionInfo);

        jdbcUtils = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME), environmentVars, jdbcProperties, POSTGRES_NAME);
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
        logger.info("Setting up DB table: {}", postgresTableMovies);
        logger.info("----------------------------------------------------");

        JdbcTableUtils moviesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(postgresDbName, postgresTableMovies), environmentVars, jdbcProperties, POSTGRES_NAME);
        moviesTable.createTable("year int, title varchar, director varchar, actors varchar[]", databaseConnectionInfo);
        moviesTable.insertRow("2014, 'Interstellar', 'Christopher Nolan', " +
                "'{Matthew McConaughey, John Lithgow, Ann Hathaway, David Gyasi, Michael Caine, " +
                "Jessica Chastain, Matt Damon, Casey Affleck}'", databaseConnectionInfo);
        moviesTable.insertRow("1986, 'Aliens', 'James Cameron', " +
                "'{Sigourney Weaver, Paul Reiser, Lance Henriksen, Bill Paxton}'", databaseConnectionInfo);

    }

    /**
     * Creates the 'bday' table and inserts rows.
     */
    private void setUpBdayTable()
          throws Exception
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", postgresTableBday);
        logger.info("----------------------------------------------------");

        JdbcTableUtils bdayTable = new JdbcTableUtils(lambdaFunctionName, new TableName(postgresDbName, postgresTableBday), environmentVars, jdbcProperties, POSTGRES_NAME);
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

        JdbcTableUtils datatypesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME), environmentVars, jdbcProperties, POSTGRES_NAME);
        datatypesTable.createTable("int_type INTEGER, smallint_type SMALLINT, bigint_type BIGINT, varchar_type CHARACTER VARYING(255), boolean_type BOOLEAN, float4_type REAL, float8_type DOUBLE PRECISION, date_type DATE, timestamp_type TIMESTAMP, byte_type BYTEA, textarray_type TEXT[]", databaseConnectionInfo);
        String row = String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
                TEST_DATATYPES_INT_VALUE,
                TEST_DATATYPES_SHORT_VALUE,
                TEST_DATATYPES_LONG_VALUE,
                "'" + TEST_DATATYPES_VARCHAR_VALUE + "'",
                TEST_DATATYPES_BOOLEAN_VALUE,
                TEST_DATATYPES_SINGLE_PRECISION_VALUE,
                TEST_DATATYPES_DOUBLE_PRECISION_VALUE,
                "'" + TEST_DATATYPES_DATE_VALUE + "'",
                "'" + TEST_DATATYPES_TIMESTAMP_VALUE + "'",
                "decode('DEADBEEF', 'hex')",
                "ARRAY ['(408)-589-5846','(408)-589-5555']");
        datatypesTable.insertRow(row, databaseConnectionInfo);
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

        JdbcTableUtils datatypesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_NULL_TABLE_NAME), environmentVars, jdbcProperties, POSTGRES_NAME);
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

        JdbcTableUtils datatypesTable = new JdbcTableUtils(lambdaFunctionName, new TableName(INTEG_TEST_DATABASE_NAME, TEST_EMPTY_TABLE_NAME), environmentVars, jdbcProperties, POSTGRES_NAME);
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
        assertTrue("DB not found.", dbNames.contains(postgresDbName));
    }

    @Test
    public void listTablesIntegTest()
          throws Exception
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(postgresDbName);
        logger.info("Tables: {}", tableNames);
        assertEquals("Incorrect number of tables found.", 2, tableNames.size());
        assertTrue(String.format("Table not found: %s.", postgresTableMovies),
                tableNames.contains(postgresTableMovies));
        assertTrue(String.format("Table not found: %s.", postgresTableBday),
                tableNames.contains(postgresTableBday));
    }

    @Test
    public void listTableSchemaIntegTest()
          throws Exception
    {
        logger.info("--------------------------------------");
        logger.info("Executing listTableSchemaIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(postgresDbName, postgresTableMovies);
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
        assertTrue("Column not found: actors", schema.containsKey("actors"));
        assertEquals("Wrong column type for actors.", "array<varchar>", schema.get("actors"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
          throws Exception
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select title from %s.%s.%s where year > 2010;",
                lambdaFunctionName, postgresDbName, postgresTableMovies);
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
                "select first_name from %s.%s.%s where birthday between date('2005-10-01') and date('2005-10-31');",
                lambdaFunctionName, postgresDbName, postgresTableBday);
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
