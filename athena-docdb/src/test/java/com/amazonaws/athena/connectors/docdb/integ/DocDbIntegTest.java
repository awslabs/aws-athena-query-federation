/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb.integ;

import com.amazonaws.athena.connector.integ.ConnectorStackFactory;
import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.athena.connector.integ.data.ConnectorPackagingAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorStackAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.data.SecretsManagerCredentials;
import com.amazonaws.athena.connector.integ.providers.ConnectorPackagingAttributesProvider;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.docdb.AmazonDocDB;
import com.amazonaws.services.docdb.AmazonDocDBClientBuilder;
import com.amazonaws.services.docdb.model.DBCluster;
import com.amazonaws.services.docdb.model.DescribeDBClustersRequest;
import com.amazonaws.services.docdb.model.DescribeDBClustersResult;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.SecretValue;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.docdb.ClusterParameterGroup;
import software.amazon.awscdk.services.docdb.DatabaseCluster;
import software.amazon.awscdk.services.docdb.Endpoint;
import software.amazon.awscdk.services.docdb.InstanceProps;
import software.amazon.awscdk.services.docdb.Login;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcAttributes;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the DocumentDB connector using the Integration-test framework.
 */
public class DocDbIntegTest extends IntegrationTestBase {
    private static final Logger logger = LoggerFactory.getLogger(DocDbIntegTest.class);

    private final App theApp;
    private final String username;
    private final String password;
    private final String docdbDbName;
    private final Number docdbDbPort;
    private final String docdbTableMovies;
    private final String lambdaFunctionName;
    private final String dbClusterName;
    private final Map<String, String> environmentVars;

    private CloudFormationClient cloudFormationClient;

    public DocDbIntegTest() {
        theApp = new App();
        SecretsManagerCredentials secretsManagerCredentials = getSecretCredentials().orElseThrow(() ->
                new RuntimeException("secrets_manager_secret must be provided in test-config.json file."));
        username = secretsManagerCredentials.getUsername();
        password = secretsManagerCredentials.getPassword();
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json file."));
        docdbDbName = (String) userSettings.get("docdb_db_name");
        docdbDbPort = (Number) userSettings.get("docdb_db_port");
        docdbTableMovies = (String) userSettings.get("docdb_table_movies");
        lambdaFunctionName = getLambdaFunctionName();
        dbClusterName = "integ-docdb-cluster-" + UUID.randomUUID();
        environmentVars = new HashMap<>();
    }

    /**
     * Creates a DocumentDB Cluster used for the integration tests.
     */
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        cloudFormationClient = new CloudFormationClient(theApp, getDocDbStack());
        try {
            // Create the CloudFormation stack for the DocumentDb cluster.
            cloudFormationClient.createStack();
            // Get DB cluster's endpoint information and set the connection string environment var for Lambda.
            setEnvironmentVars(getClusterData());
            // Invoke the framework's setUp().
            super.setUp();
        } catch (Exception e) {
            // Delete the partially formed CloudFormation stack.
            cloudFormationClient.deleteStack();
            throw e;
        }
    }

    /**
     * Deletes a CloudFormation stack for the DocumentDb Cluster.
     */
    @AfterClass
    @Override
    protected void cleanUp() {
        // Invoke the framework's cleanUp().
        super.cleanUp();
        // Delete the CloudFormation stack for the DocumentDb DB cluster.
        cloudFormationClient.deleteStack();
    }

    /**
     * Gets the CloudFormation stack for the DocumentDb Cluster.
     * @return Stack object for the DocumentDb Cluster.
     */
    private Stack getDocDbStack() {
        Stack stack = Stack.Builder.create(theApp, dbClusterName).build();

        ConnectorVpcAttributes vpcAttributes = getVpcAttributes()
                .orElseThrow(() -> new RuntimeException("vpc_configuration must be specified in test-config.json"));

        DatabaseCluster.Builder.create(stack, "DocDbCluster")
                .removalPolicy(RemovalPolicy.DESTROY)
                .dbClusterName(dbClusterName)
                .port(docdbDbPort)
                .storageEncrypted(Boolean.FALSE)
                .masterUser(Login.builder()
                        .username(username)
                        .password(SecretValue.plainText(password))
                        .build())
                .engineVersion("3.6.0")
                .parameterGroup(ClusterParameterGroup.Builder.create(stack, "DocDbClusterParamGroup")
                        .dbClusterParameterGroupName(dbClusterName)
                        .family("docdb3.6")
                        .description("Cluster parameter group for Athena Federation integration tests.")
                        .parameters(new ImmutableMap.Builder<String, String>()
                                .put("audit_logs", "disabled")
                                .put("change_stream_log_retention_duration", "10800")
                                .put("profiler", "disabled")
                                .put("profiler_sampling_rate", "1.0")
                                .put("profiler_threshold_ms", "100")
                                .put("tls", "disabled")
                                .put("ttl_monitor", "enabled")
                                .build())
                        .build())
                .instanceIdentifierBase(dbClusterName)
                .instances(1)
                .instanceProps(InstanceProps.builder()
                        .instanceType(new InstanceType("t3.medium"))
                        .vpc(Vpc.fromVpcAttributes(stack, "DocDbVpcConfig", VpcAttributes.builder()
                                .vpcId(vpcAttributes.getVpcId())
                                .privateSubnetIds(vpcAttributes.getPrivateSubnetIds())
                                .availabilityZones(vpcAttributes.getAvailabilityZones())
                                .build()))
                        .securityGroup(SecurityGroup
                                .fromSecurityGroupId(stack, "DocDbVpcSecurityGroup",
                                        vpcAttributes.getSecurityGroupId()))
                        .build())
                .build();

        return stack;
    }

    /**
     * Gets the DocumentDb Cluster endpoint information and generates the environment variables needed for the
     * Lambda. All exceptions thrown here will be caught in the calling function.
     */
    private Endpoint getClusterData() {
        AmazonDocDB docDbClient = AmazonDocDBClientBuilder.defaultClient();
        try {
            DescribeDBClustersResult dbClustersResult = docDbClient.describeDBClusters(new DescribeDBClustersRequest()
                    .withDBClusterIdentifier(dbClusterName));
            DBCluster cluster = dbClustersResult.getDBClusters().get(0);
            return new Endpoint(cluster.getEndpoint(), cluster.getPort());
        }
        finally {
            docDbClient.shutdown();
        }
    }

    /**
     * Set the connection string environment variable for the Lambda.
     * @param endpoint The DocumentDb Cluster's endpoint.
     */
    private void setEnvironmentVars(Endpoint endpoint)
    {
        String connectionString = String
                .format("mongodb://%s:%s@%s:%s/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false",
                        username, password, endpoint.getHostname(), endpoint.getPort().toString());
        environmentVars.put("default_docdb", connectionString);
        environmentVars.put("movies_database_name", docdbDbName);
        environmentVars.put("movies_table_name", docdbTableMovies);
        environmentVars.put("datatypes_database_name", INTEG_TEST_DATABASE_NAME);
        environmentVars.put("datatypes_table_name", TEST_DATATYPES_TABLE_NAME);
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     *
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
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
     *
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        // No-op.
    }

    /**
     * Create and invoke a special Lambda function that sets up the MongoDB table used by the integration tests.
     */
    @Override
    protected void setUpTableData()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up table for DB ({}): {}", docdbDbName, docdbTableMovies);
        logger.info("----------------------------------------------------");

        String mongoLambdaName = "integ-mongodb-" + UUID.randomUUID();
        AWSLambda lambdaClient = AWSLambdaClientBuilder.defaultClient();
        CloudFormationClient cloudFormationMongoClient = new CloudFormationClient(getMongoLambdaStack(mongoLambdaName));
        try {
            // Create the Lambda function.
            cloudFormationMongoClient.createStack();
            // Invoke the Lambda function.
            lambdaClient.invoke(new InvokeRequest()
                    .withFunctionName(mongoLambdaName)
                    .withInvocationType(InvocationType.RequestResponse));
        }
        finally {
            // Delete the Lambda function.
            cloudFormationMongoClient.deleteStack();
            lambdaClient.shutdown();
        }
    }

    /**
     * Generates the CloudFormation stack for the Lambda function that creates the MongoDB table and data.
     * @param mongoLambdaName The name of the Lambda function.
     * @return Stack attributes used to create the CloudFormation stack.
     */
    private Pair<App, Stack> getMongoLambdaStack(String mongoLambdaName)
    {
        String mongoStackName = "integ-mongodb-lambda-" + UUID.randomUUID();
        App mongoApp = new App();
        ConnectorPackagingAttributes packagingAttributes = ConnectorPackagingAttributesProvider.getAttributes();
        ConnectorPackagingAttributes mongoPackagingAttributes =
                new ConnectorPackagingAttributes(packagingAttributes.getS3Bucket(), packagingAttributes.getS3Key(),
                        DocDbIntegTestHandler.HANDLER);
        ConnectorStackAttributes mongoStackAttributes =
                new ConnectorStackAttributes(mongoApp, mongoStackName, mongoLambdaName, getConnectorAccessPolicy(),
                        environmentVars, mongoPackagingAttributes, getVpcAttributes());
        ConnectorStackFactory mongoStackFactory = new ConnectorStackFactory(mongoStackAttributes);

        return new Pair<>(mongoApp, mongoStackFactory.createStack());
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue(String.format("DB not found: %s.", docdbDbName), dbNames.contains(docdbDbName));
        assertTrue(String.format("DB not found: %s.", INTEG_TEST_DATABASE_NAME), dbNames.contains(INTEG_TEST_DATABASE_NAME));
    }

    @Override
    public void selectByteArrayTypeTest()
    {
        // not supported!
    }

    @Override
    public void selectEmptyTableTest()
    {
        // mongodb is schema-free, so an empty table registers as having no columns
        // maybe using glue external metadata for the test tables would solve this
    }

    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(docdbDbName);
        logger.info("Tables: {}", tableNames);
        assertTrue(String.format("Table not found: %s.", docdbTableMovies), tableNames.contains(docdbTableMovies));

        tableNames = listTables(INTEG_TEST_DATABASE_NAME);
        logger.info("Tables: {}", tableNames);
        assertTrue(String.format("Table not found: %s.", TEST_DATATYPES_TABLE_NAME), tableNames.contains(TEST_DATATYPES_TABLE_NAME));
    }

    @Test
    public void describeTableIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing describeTableIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(docdbDbName, docdbTableMovies);
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 4, schema.size());
        assertTrue("Column not found: _id", schema.containsKey("_id"));
        assertTrue("Column not found: title", schema.containsKey("title"));
        assertTrue("Column not found: year", schema.containsKey("year"));
        assertTrue("Column not found: cast", schema.containsKey("cast"));
        assertEquals("Wrong column type for title.", "varchar", schema.get("title"));
        assertEquals("Wrong column type for year.", "int", schema.get("year"));
        assertEquals("Wrong column type for cast.", "array<varchar>",schema.get("cast"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select title from %s.%s.%s where year > 2012;",
                lambdaFunctionName, docdbDbName, docdbTableMovies);
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
}
