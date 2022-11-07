/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase.integ;

import com.amazonaws.athena.connector.integ.ConnectorStackFactory;
import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.athena.connector.integ.data.ConnectorPackagingAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorStackAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.providers.ConnectorPackagingAttributesProvider;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.emr.CfnCluster;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the HBase connector using the Integration-test framework.
 */
public class HbaseIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseIntegTest.class);

    private final String hbaseDbName;
    private final String hbaseDbPort;
    private final String zookeeperPort;
    private final String hbaseTableName;
    private final String lambdaFunctionName;
    private final String dbClusterName;
    private final Map<String, String> environmentVars;

    private CloudFormationClient cloudFormationClient;

    public HbaseIntegTest()
    {
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json file."));
        hbaseDbName = (String) userSettings.get("hbase_db_name");
        hbaseDbPort = (String) userSettings.get("hbase_db_port");
        zookeeperPort = (String) userSettings.get("zookeeper_port");
        hbaseTableName = (String) userSettings.get("hbase_table_name");
        lambdaFunctionName = getLambdaFunctionName();
        dbClusterName = "integ-hbase-cluster-" + UUID.randomUUID();
        environmentVars = new HashMap<>();
    }

    /**
     * Creates a HBase EMR Cluster used for the integration tests.
     */
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        cloudFormationClient = new CloudFormationClient(getHbaseStack());
        try {
            // Create the CloudFormation stack for the HBase DB cluster.
            cloudFormationClient.createStack();
            // Get the hostname of the EMR cluster hosting the HBase database, and set the environment variables
            // needed by the Lambda.
            setEnvironmentVars(getClusterData());
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
     * Deletes a CloudFormation stack for the HBase EMR Cluster.
     */
    @AfterClass
    @Override
    protected void cleanUp()
    {
        // Invoke the framework's cleanUp().
        super.cleanUp();
        // Delete the CloudFormation stack for the HBase DB cluster.
        cloudFormationClient.deleteStack();
    }

    /**
     * Gets the CloudFormation stack for the EMR cluster hosting the HBase database.
     * @return App and Stack objects for the EMR cluster.
     */
    private Pair<App, Stack> getHbaseStack() {
        App theApp = new App();
        Stack stack = Stack.Builder.create(theApp, dbClusterName).build();

        ConnectorVpcAttributes vpcAttributes = getVpcAttributes()
                .orElseThrow(() -> new RuntimeException("vpc_configuration must be specified in test-config.json"));

        CfnCluster.Builder.create(stack, "HbaseCluster")
                .name(dbClusterName)
                .visibleToAllUsers(Boolean.TRUE)
                .applications(ImmutableList.of(
                        new Application().withName("HBase"),
                        new Application().withName("Hive"),
                        new Application().withName("Hue"),
                        new Application().withName("Phoenix")))
                .instances(CfnCluster.JobFlowInstancesConfigProperty.builder()
                        .emrManagedMasterSecurityGroup(vpcAttributes.getSecurityGroupId())
                        .emrManagedSlaveSecurityGroup(vpcAttributes.getSecurityGroupId())
                        .ec2SubnetIds(vpcAttributes.getPrivateSubnetIds())
                        .masterInstanceGroup(CfnCluster.InstanceGroupConfigProperty.builder()
                                .name("HbaseMasterInstanceGroup")
                                .instanceType("m5.xlarge")
                                .instanceCount(1)
                                .build())
                        .coreInstanceGroup(CfnCluster.InstanceGroupConfigProperty.builder()
                                .name("HbaseCoreInstanceGroup")
                                .instanceType("m5.xlarge")
                                .instanceCount(1)
                                .build())
                        .build())
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.32.0")
                .build();

        return new Pair<>(theApp, stack);
    }

    /**
     * Gets the hostname of the EMR cluster hosting the HBase database. All exceptions thrown here will be caught in
     * the calling function.
     * @return String containing the public hostname for the EMR cluster hosting the HBase database.
     */
    private String getClusterData()
    {
        AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.defaultClient();
        try {
            ListClustersResult listClustersResult;
            String marker = null;
            Optional<String> dbClusterId;
            do { // While cluster Id has not yet been found and there are more paginated results.
                // Get paginated list of EMR clusters.
                listClustersResult = emrClient.listClusters(new ListClustersRequest().withMarker(marker));
                // Get the cluster id.
                dbClusterId = getClusterId(listClustersResult);
                // Get the marker for the next paginated request.
                marker = listClustersResult.getMarker();
            } while (!dbClusterId.isPresent() && marker != null);
            // Get the cluster description using the cluster id.
            DescribeClusterResult clusterResult = emrClient.describeCluster(new DescribeClusterRequest()
                    .withClusterId(dbClusterId.orElseThrow(() ->
                            new RuntimeException("Unable to get cluster description for: " + dbClusterName))));
            return clusterResult.getCluster().getMasterPublicDnsName();
        }
        finally {
            emrClient.shutdown();
        }
    }

    /**
     * Gets the EMR Cluster Id of the cluster hosting the HBase database from the paginated list-clusters request.
     * @param listClustersResult Paginated results from the list-clusters request.
     * @return Optional String containing the cluster Id that matches the cluster name, or Optional.empty() if match
     * was not found.
     */
    private Optional<String> getClusterId(ListClustersResult listClustersResult)
    {
        for (ClusterSummary clusterSummary : listClustersResult.getClusters()) {
            if (clusterSummary.getName().equals(dbClusterName)) {
                // Found match for cluster name - return cluster id.
                String clusterId = clusterSummary.getId();
                logger.info("Found Cluster Id for {}: {}", dbClusterName, clusterId);
                return Optional.of(clusterId);
            }
        }

        return Optional.empty();
    }

    /**
     * Sets the environment variables needed for the Lambda such as the connection string
     * (e.g. ec2-000-000-000-000.compute-1.amazonaws.com:50075:2081)
     * @param hostName Contains the public hostname for the EMR cluster hosting the HBase database.
     */
    private void setEnvironmentVars(String hostName)
    {
        String connectionString = String.format("%s:%s:%s", hostName, hbaseDbPort, zookeeperPort);
        environmentVars.put("default_hbase", connectionString);
        environmentVars.put("database_name", hbaseDbName);
        environmentVars.put("table_name", hbaseTableName);
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
     * Create and invoke a special Lambda function that sets up the HBase table used by the integration tests.
     */
    @Override
    protected void setUpTableData()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up table for DB ({}): {}", hbaseDbName, hbaseTableName);
        logger.info("----------------------------------------------------");

        String hbaseLambdaName = "integ-hbase-" + UUID.randomUUID();
        AWSLambda lambdaClient = AWSLambdaClientBuilder.defaultClient();
        CloudFormationClient cloudFormationHbaseClient = new CloudFormationClient(getHbaseLambdaStack(hbaseLambdaName));
        try {
            // Create the Lambda function.
            cloudFormationHbaseClient.createStack();
            // Invoke the Lambda function.
            lambdaClient.invoke(new InvokeRequest()
                    .withFunctionName(hbaseLambdaName)
                    .withInvocationType(InvocationType.RequestResponse));
        }
        finally {
            // Delete the Lambda function.
            cloudFormationHbaseClient.deleteStack();
            lambdaClient.shutdown();
        }

    }

    /**
     * Generates the CloudFormation stack for the Lambda function that creates the HBase table and data.
     * @param hbaseLambdaName The name of the Lambda function.
     * @return Stack attributes used to create the CloudFormation stack.
     */
    private Pair<App, Stack> getHbaseLambdaStack(String hbaseLambdaName)
    {
        String hbaseStackName = "integ-hbase-lambda-" + UUID.randomUUID();
        App hbaseApp = new App();
        ConnectorPackagingAttributes packagingAttributes = ConnectorPackagingAttributesProvider.getAttributes();
        ConnectorPackagingAttributes hbasePackagingAttributes =
                new ConnectorPackagingAttributes(packagingAttributes.getS3Bucket(), packagingAttributes.getS3Key(),
                        HbaseIntegTestHandler.HANDLER);
        ConnectorStackAttributes hbaseStackAttributes =
                new ConnectorStackAttributes(hbaseApp, hbaseStackName, hbaseLambdaName, getConnectorAccessPolicy(),
                        environmentVars, hbasePackagingAttributes, getVpcAttributes());
        ConnectorStackFactory hbaseStackFactory = new ConnectorStackFactory(hbaseStackAttributes);

        return new Pair<>(hbaseApp, hbaseStackFactory.createStack());
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains(hbaseDbName));
    }

    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(hbaseDbName);
        logger.info("Tables: {}", tableNames);
        assertTrue(String.format("Table not found: %s.", hbaseTableName), tableNames.contains(hbaseTableName));
    }

    @Test
    public void listTableSchemaIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listTableSchemaIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(hbaseDbName, hbaseTableName);
        schema.remove("partition_name");
        schema.remove("partition_schema_name");
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 5, schema.size());
        assertTrue("Column not found: row", schema.containsKey("row"));
        assertEquals("Wrong column type for row.", "varchar", schema.get("row"));
        assertTrue("Column not found: movie:title", schema.containsKey("movie:title"));
        assertEquals("Wrong column type for movie:title.", "varchar", schema.get("movie:title"));
        assertTrue("Column not found: info:year", schema.containsKey("info:year"));
        assertEquals("Wrong column type for info:year.", "bigint", schema.get("info:year"));
        assertTrue("Column not found: info:director", schema.containsKey("info:director"));
        assertEquals("Wrong column type for info:director.", "varchar", schema.get("info:director"));
        assertTrue("Column not found: info:lead_actor", schema.containsKey("info:lead_actor"));
        assertEquals("Wrong column type for info:lead_actor.", "varchar", schema.get("info:lead_actor"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String
                .format("select \"info:lead_actor\" from %s.%s.%s where \"movie:title\" = 'Aliens';",
                        lambdaFunctionName, hbaseDbName, hbaseTableName);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> actors = new ArrayList<>();
        rows.forEach(row -> actors.add(row.getData().get(0).getVarCharValue()));
        logger.info("Actors: {}", actors);
        assertEquals("Wrong number of DB records found.", 1, actors.size());
        assertTrue("Actor not found: Sigourney Weaver.", actors.contains("Sigourney Weaver"));
    }

    @Test
    public void selectIntegerTypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectIntegerTypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select \"datatype:int_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:varchar_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:boolean_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:smallint_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:bigint_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:float4_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:float8_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:date_type\" from %s.%s.%s;",
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

        String query = String.format("select \"datatype:timestamp_type\" from %s.%s.%s;",
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
        // not supported
    }

    @Test
    public void selectVarcharListTypeTest()
    {
        // not supported
    }

    @Test
    public void selectNullValueTest()
    {
        // TODO: figure out how to write null values
    }

    @Test
    public void selectEmptyTableTest()
    {
        // TODO: figure out how to set schema of an empty table
    }
}
