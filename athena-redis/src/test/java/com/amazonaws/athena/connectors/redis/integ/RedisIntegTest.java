/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis.integ;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.athena.connector.integ.ConnectorStackFactory;
import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.clients.CloudFormationClient;
import com.amazonaws.athena.connector.integ.data.ConnectorPackagingAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorStackAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.data.SecretsManagerCredentials;
import com.amazonaws.athena.connector.integ.providers.ConnectorPackagingAttributesProvider;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.elasticache.AmazonElastiCache;
import com.amazonaws.services.elasticache.AmazonElastiCacheClientBuilder;
import com.amazonaws.services.elasticache.model.DescribeCacheClustersRequest;
import com.amazonaws.services.elasticache.model.DescribeCacheClustersResult;
import com.amazonaws.services.elasticache.model.DescribeReplicationGroupsRequest;
import com.amazonaws.services.elasticache.model.DescribeReplicationGroupsResult;
import com.amazonaws.services.elasticache.model.Endpoint;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.elasticache.CfnCacheCluster;
import software.amazon.awscdk.services.elasticache.CfnReplicationGroup;
import software.amazon.awscdk.services.elasticache.CfnSubnetGroup;
import software.amazon.awscdk.services.glue.Column;
import software.amazon.awscdk.services.glue.DataFormat;
import software.amazon.awscdk.services.glue.Database;
import software.amazon.awscdk.services.glue.Schema;
import software.amazon.awscdk.services.glue.Table;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;

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
 * Integration-tests for the Redis connector using the Integration-test module.
 */
@Test(singleThreaded = true)
public class RedisIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedisIntegTest.class);
    private static final String STANDALONE_KEY = "standalone";
    private static final String CLUSTER_KEY = "cluster";
    private static final int GLUE_TIMEOUT = 250;
    private static final String STANDALONE_REDIS_DB_NUMBER = "10";

    private final App theApp;
    private final String redisPassword;
    private final String redisStandaloneName;
    private final String redisClusterName;
    private final String redisPort;
    private final String redisDbName;
    private final String redisTableNamePrefix;
    private final String lambdaFunctionName;
    private final AWSGlue glue;
    private final String redisStackName;
    private final Map<String, String> environmentVars;

    private CloudFormationClient cloudFormationClient;
    private final Map<String, String> redisEndpoints = new HashMap<>();

    public RedisIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        SecretsManagerCredentials secretsManagerCredentials = getSecretCredentials().orElseThrow(() ->
                new RuntimeException("secrets_manager_secret must be provided in test-config.json file."));
        redisPassword = secretsManagerCredentials.getPassword();
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json file."));
        redisStandaloneName = userSettings.get("redis_instance_prefix") + "-standalone-" + UUID.randomUUID().toString().substring(0, 6);
        redisClusterName = userSettings.get("redis_instance_prefix") + "-cluster-" + UUID.randomUUID().toString().substring(0, 6);
        redisPort = (String) userSettings.get("redis_port");
        redisDbName = (String) userSettings.get("redis_db_name");
        redisTableNamePrefix = (String) userSettings.get("redis_table_name_prefix");
        lambdaFunctionName = getLambdaFunctionName();
        glue = AWSGlueClientBuilder.standard()
                .withClientConfiguration(new ClientConfiguration().withConnectionTimeout(GLUE_TIMEOUT))
                .build();
        redisStackName = "integ-redis-instance-" + UUID.randomUUID();
        environmentVars = new HashMap<>();
    }

    /**
     * Creates a Redis cluster used for the integration tests.
     */
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        cloudFormationClient = new CloudFormationClient(theApp, getRedisStack());
        try {
            // Create the CloudFormation stack for the Redis instances.
            cloudFormationClient.createStack();

            // Get host and port information
            Endpoint standaloneEndpoint = getRedisInstanceData(redisStandaloneName, false);
            logger.info("Got Endpoint: " + standaloneEndpoint.toString());
            redisEndpoints.put(STANDALONE_KEY, String.format("%s:%s",
                    standaloneEndpoint.getAddress(), standaloneEndpoint.getPort()));

            Endpoint clusterEndpoint = getRedisInstanceData(redisClusterName, true);
            logger.info("Got Endpoint: " + clusterEndpoint.toString());
            redisEndpoints.put(CLUSTER_KEY, String.format("%s:%s:%s",
                    clusterEndpoint.getAddress(), clusterEndpoint.getPort(), redisPassword));

            // Get endpoint information and set the connection string environment var for Lambda.
            environmentVars.put("standalone_connection", redisEndpoints.get(STANDALONE_KEY));
            environmentVars.put("cluster_connection", redisEndpoints.get(CLUSTER_KEY));

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
     * Deletes a CloudFormation stack for Redis.
     */
    @AfterClass
    @Override
    protected void cleanUp()
    {
        // Invoke the framework's cleanUp().
        super.cleanUp();
        // Delete the CloudFormation stack for Redis.
        cloudFormationClient.deleteStack();
        // close glue client
        glue.shutdown();
    }

    /**
     * Create and invoke a special Lambda function that sets up the Redis instances used by the integration tests.
     */
    @Override
    protected void setUpTableData()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up data for Redis Instances");
        logger.info("----------------------------------------------------");

        String redisLambdaName = "integ-redis-helper-" + UUID.randomUUID();
        AWSLambda lambdaClient = AWSLambdaClientBuilder.defaultClient();
        CloudFormationClient cloudFormationRedisClient = new CloudFormationClient(getRedisLambdaStack(redisLambdaName));
        try {
            // Create the Lambda function.
            cloudFormationRedisClient.createStack();
            // Invoke the Lambda function.
            lambdaClient.invoke(new InvokeRequest()
                    .withFunctionName(redisLambdaName)
                    .withInvocationType(InvocationType.RequestResponse));
        }
        finally {
            // Delete the Lambda function.
            cloudFormationRedisClient.deleteStack();
            lambdaClient.shutdown();
        }
    }

    /**
     * Generates the CloudFormation stack for the Lambda function that inserts the Redis Keys.
     * @param redisLambdaName The name of the Lambda function.
     * @return Stack attributes used to create the CloudFormation stack.
     */
    private Pair<App, Stack> getRedisLambdaStack(String redisLambdaName)
    {
        String redisStackName = "integ-redis-helper-lambda-" + UUID.randomUUID();
        App redisApp = new App();
        ConnectorPackagingAttributes packagingAttributes = ConnectorPackagingAttributesProvider.getAttributes();
        ConnectorPackagingAttributes redisPackagingAttributes =
                new ConnectorPackagingAttributes(packagingAttributes.getS3Bucket(), packagingAttributes.getS3Key(),
                        RedisIntegTestHandler.HANDLER);
        ConnectorStackAttributes redisStackAttributes =
                new ConnectorStackAttributes(redisApp, redisStackName, redisLambdaName, getConnectorAccessPolicy(),
                        environmentVars, redisPackagingAttributes, getVpcAttributes());
        ConnectorStackFactory redisStackFactory = new ConnectorStackFactory(redisStackAttributes);

        return new Pair<>(redisApp, redisStackFactory.createStack());
    }

    /**
     * Sets the environment variables for the Lambda function.
     *
     * @param environmentVars
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
    protected void setUpStackData(Stack stack)
    {
        //No-op
    }

    /**
     * Must be overridden in the extending class to get the lambda function's IAM access policy. The latter sets up
     * access to multiple connector-specific AWS services (e.g. DynamoDB, Elasticsearch etc...)
     *
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        return Optional.empty();
    }

    /**
     * Gets the CloudFormation stack for Redis.
     * @return Stack object for Redis.
     */
    private Stack getRedisStack()
    {
        Stack stack = Stack.Builder.create(theApp, redisStackName).build();
        ConnectorVpcAttributes vpcAttributes = getVpcAttributes()
                .orElseThrow(() -> new RuntimeException("vpc_configuration must be specified in test-config.json"));

        CfnSubnetGroup redisSubnetGroup = CfnSubnetGroup.Builder.create(stack, "RedisSubnetGroup")
                .cacheSubnetGroupName("RedisSubnetGroup")
                .subnetIds(vpcAttributes.getPrivateSubnetIds())
                .description("RedisSubnetGroup")
                .build();

        CfnCacheCluster redisStandalone = CfnCacheCluster.Builder.create(stack, "RedisStandalone")
                .clusterName(redisStandaloneName)
                .cacheNodeType("cache.t3.micro")
                .cacheSubnetGroupName(redisSubnetGroup.getCacheSubnetGroupName())
                .engine("redis")
                .numCacheNodes(1)
                .port(Integer.parseInt(redisPort))
                .vpcSecurityGroupIds(Collections.singletonList(vpcAttributes.getSecurityGroupId()))
                .build();
        redisStandalone.addDependsOn(redisSubnetGroup);

        CfnReplicationGroup redisCluster = CfnReplicationGroup.Builder.create(stack, "RedisCluster")
                .replicationGroupId(redisClusterName)
                .replicationGroupDescription("RedisCluster")
                .cacheNodeType("cache.t3.micro")
                .cacheSubnetGroupName(redisSubnetGroup.getCacheSubnetGroupName())
                .engine("redis")
                .replicasPerNodeGroup(1)
                .numNodeGroups(3)
                .automaticFailoverEnabled(true)
                .port(Integer.parseInt(redisPort))
                .transitEncryptionEnabled(true)
                .authToken(redisPassword)
                .build();
        redisCluster.addDependsOn(redisSubnetGroup);

        IBucket glueTableBucket = Bucket.fromBucketName(stack, "RedisBucket", "fake-bucket");
        Database redisDb = Database.Builder.create(stack, "RedisDB")
                .databaseName(redisDbName)
                .locationUri("s3://fake-bucket?redis-db-flag=redis-db-flag")
                .build();

        // This Table will be used for hashmap keys
        List<Column> hashColumns = new ArrayList<>();
        hashColumns.add(Column.builder().name("custkey").type(Schema.BIG_INT).build());
        hashColumns.add(Column.builder().name("name").type(Schema.STRING).build());
        hashColumns.add(Column.builder().name("acctbal").type(Schema.DOUBLE).build());

        Table.Builder.create(stack, "RedisTable1")
                .database(redisDb)
                .tableName(redisTableNamePrefix + "_1")
                .columns(hashColumns)
                .dataFormat(DataFormat.AVRO)
                .bucket(glueTableBucket)
                .build();

        // This Table will be used for Literal and Zset Keys (single column)
        List<Column> zsetAndLiteralColumns = new ArrayList<>();
        zsetAndLiteralColumns.add(Column.builder().name("name").type(Schema.STRING).build());

        Table.Builder.create(stack, "RedisTable2")
                .database(redisDb)
                .tableName(redisTableNamePrefix + "_2")
                .columns(zsetAndLiteralColumns)
                .dataFormat(DataFormat.AVRO)
                .bucket(glueTableBucket)
                .build();

        return stack;
    }

    /**
     * Gets the Redis server endpoint information.
     * All exceptions thrown here will be caught in the calling function.
     */
    private Endpoint getRedisInstanceData(String redisName, boolean isCluster)
    {
        AmazonElastiCache elastiCacheClient = AmazonElastiCacheClientBuilder.defaultClient();
        try {
            if (isCluster) {
                DescribeReplicationGroupsResult describeResult = elastiCacheClient.describeReplicationGroups(new DescribeReplicationGroupsRequest()
                        .withReplicationGroupId(redisName));
                return describeResult.getReplicationGroups().get(0).getConfigurationEndpoint();
            }
            else {
                DescribeCacheClustersResult describeResult = elastiCacheClient.describeCacheClusters(new DescribeCacheClustersRequest()
                        .withCacheClusterId(redisName).withShowCacheNodeInfo(true));
                return describeResult.getCacheClusters().get(0).getCacheNodes().get(0).getEndpoint();
            }
        }
        finally {
            elastiCacheClient.shutdown();
        }
    }

    /**
     * This method gets a Table using the given name from Glue Data Catalog.
     *
     * @param databaseName
     * @param tableName
     * @return Table
     */
    private com.amazonaws.services.glue.model.Table getGlueTable(String databaseName, String tableName)
    {
        com.amazonaws.services.glue.model.Table table;
        GetTableRequest getTableRequest = new GetTableRequest();
        getTableRequest.setDatabaseName(databaseName);
        getTableRequest.setName(tableName);
        try {
            GetTableResult tableResult = glue.getTable(getTableRequest);
            table = tableResult.getTable();
        } catch (EntityNotFoundException e) {
            throw e;
        }
        return table;
    }

    /**
     * This method creates a TableInput object using Table object
     *
     * @param table
     * @return TableInput
     */
    private TableInput createTableInput(com.amazonaws.services.glue.model.Table table) {
        TableInput tableInput = new TableInput();
        tableInput.setDescription(table.getDescription());
        tableInput.setLastAccessTime(table.getLastAccessTime());
        tableInput.setOwner(table.getOwner());
        tableInput.setName(table.getName());
        if (Optional.ofNullable(table.getStorageDescriptor()).isPresent()) {
            tableInput.setStorageDescriptor(table.getStorageDescriptor());
            if (Optional.ofNullable(table.getStorageDescriptor().getParameters()).isPresent())
                tableInput.setParameters(table.getStorageDescriptor().getParameters());
        }
        tableInput.setPartitionKeys(table.getPartitionKeys());
        tableInput.setTableType(table.getTableType());
        tableInput.setViewExpandedText(table.getViewExpandedText());
        tableInput.setViewOriginalText(table.getViewOriginalText());
        tableInput.setParameters(table.getParameters());
        return tableInput;
    }

    private void selectHashValue()
    {
        String query = String.format("select * from \"%s\".\"%s\".\"%s\";",
                lambdaFunctionName, redisDbName, redisTableNamePrefix + "_1");
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> names = new ArrayList<>();
        rows.forEach(row -> {
            names.add(row.getData().get(1).getVarCharValue());
            // redis key is added as an extra col by the connector. so expected #cols is #glue cols + 1
            assertEquals("Wrong number of columns found", 4, row.getData().size());
        });
        logger.info("names: {}", names);
        assertEquals("Wrong number of DB records found.", 3, names.size());
        assertTrue("name not found: Jon Snow.", names.contains("Jon Snow"));
        assertTrue("name not found: Robb Stark.", names.contains("Robb Stark"));
        assertTrue("name not found: Eddard Stark.", names.contains("Eddard Stark"));
    }

    private void selectZsetValue()
    {
        String query = String.format("select * from \"%s\".\"%s\".\"%s\";",
                lambdaFunctionName, redisDbName, redisTableNamePrefix + "_2");
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> names = new ArrayList<>();
        rows.forEach(row -> {
            names.add(row.getData().get(0).getVarCharValue());
            assertEquals("Wrong number of columns found", 2, row.getData().size());
        });
        logger.info("names: {}", names);
        assertEquals("Wrong number of DB records found.", 3, names.size());
        assertTrue("name not found: customer-hm:1.", names.contains("customer-hm:1"));
        assertTrue("name not found: customer-hm:2.", names.contains("customer-hm:2"));
        assertTrue("name not found: customer-hm:3.", names.contains("customer-hm:3"));
    }

    private void selectLiteralValue()
    {
        String query = String.format("select * from \"%s\".\"%s\".\"%s\";",
                lambdaFunctionName, redisDbName, redisTableNamePrefix + "_2");
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> names = new ArrayList<>();
        rows.forEach(row -> {
            names.add(row.getData().get(0).getVarCharValue());
            assertEquals("Wrong number of columns found", 2, row.getData().size());
        });
        logger.info("names: {}", names);
        assertEquals("Wrong number of DB records found.", 3, names.size());
        assertTrue("name not found: Sansa Stark.", names.contains("Sansa Stark"));
        assertTrue("name not found: Daenerys Targaryen.", names.contains("Daenerys Targaryen"));
        assertTrue("name not found: Arya Stark.", names.contains("Arya Stark"));
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains(redisDbName));
    }

    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(redisDbName);
        logger.info("Tables: {}", tableNames);
        assertEquals("Incorrect number of tables found.", 2, tableNames.size());
        assertTrue(String.format("Table not found: %s.", redisTableNamePrefix + "_1"),
                tableNames.contains(redisTableNamePrefix + "_1"));
        assertTrue(String.format("Table not found: %s.", redisTableNamePrefix + "_2"),
                tableNames.contains(redisTableNamePrefix + "_2"));
    }

    @Test
    public void listTableSchemaIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listTableSchemaIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(redisDbName, redisTableNamePrefix + "_1");
        schema.remove("partition_name");
        schema.remove("partition_schema_name");
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 4, schema.size());

        assertTrue("Column not found: custkey", schema.containsKey("custkey"));
        assertEquals("Wrong column type for custkey.", "bigint", schema.get("custkey"));
        assertTrue("Column not found: name", schema.containsKey("name"));
        assertEquals("Wrong column type for name.", "varchar", schema.get("name"));
        assertTrue("Column not found: acctbal", schema.containsKey("acctbal"));
        assertEquals("Wrong column type for acctbal.", "double", schema.get("acctbal"));
        assertTrue("Column not found: _key_", schema.containsKey("_key_"));
        assertEquals("Wrong column type for _key_.", "varchar", schema.get("_key_"));
    }

    @Test
    public void standaloneSelectPrefixWithHashValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing standaloneSelectPrefixWithHashValue");
        logger.info("--------------------------------------------------");

        // Setup Table 1
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(STANDALONE_KEY));
        tableParams.put("redis-key-prefix", "customer-hm:*"); // prefix
        tableParams.put("redis-value-type", "hash"); // hash
        tableParams.put("redis-cluster-flag", "false");
        tableParams.put("redis-ssl-flag", "false");
        tableParams.put("redis-db-number", STANDALONE_REDIS_DB_NUMBER);
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_1")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectHashValue();
    }

    @Test
    public void standaloneSelectZsetWithHashValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing standaloneSelectZsetWithHashValue");
        logger.info("--------------------------------------------------");

        // Setup Table 1
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(STANDALONE_KEY));
        tableParams.put("redis-keys-zset", "customer-hm-zset"); // zset
        tableParams.put("redis-value-type", "hash"); // hash
        tableParams.put("redis-cluster-flag", "false");
        tableParams.put("redis-ssl-flag", "false");
        tableParams.put("redis-db-number", STANDALONE_REDIS_DB_NUMBER);
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_1")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectHashValue();
    }

    @Test
    public void clusterSelectPrefixWithHashValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing clusterSelectPrefixWithHashValue");
        logger.info("--------------------------------------------------");

        // Setup Table 1
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(CLUSTER_KEY));
        tableParams.put("redis-key-prefix", "customer-hm:*"); // prefix
        tableParams.put("redis-value-type", "hash"); // hash
        tableParams.put("redis-cluster-flag", "true");
        tableParams.put("redis-ssl-flag", "true");
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_1")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectHashValue();
    }

    @Test
    public void clusterSelectZsetWithHashValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing clusterSelectZsetWithHashValue");
        logger.info("--------------------------------------------------");

        // Setup Table 1
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(CLUSTER_KEY));
        tableParams.put("redis-keys-zset", "customer-hm-zset"); // zset
        tableParams.put("redis-value-type", "hash"); // hash
        tableParams.put("redis-cluster-flag", "true");
        tableParams.put("redis-ssl-flag", "true");
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_1")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectHashValue();
    }

    @Test
    public void standaloneSelectPrefixWithZsetValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing standaloneSelectPrefixWithZsetValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(STANDALONE_KEY));
        tableParams.put("redis-key-prefix", "customer-hm-zset*"); // prefix
        tableParams.put("redis-value-type", "zset"); // zset
        tableParams.put("redis-cluster-flag", "false");
        tableParams.put("redis-ssl-flag", "false");
        tableParams.put("redis-db-number", STANDALONE_REDIS_DB_NUMBER);
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectZsetValue();
    }

    @Test
    public void standaloneSelectZsetWithZsetValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing standaloneSelectZsetWithZsetValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(STANDALONE_KEY));
        tableParams.put("redis-keys-zset", "key-hm-zset"); // zset
        tableParams.put("redis-value-type", "zset"); // zset
        tableParams.put("redis-cluster-flag", "false");
        tableParams.put("redis-ssl-flag", "false");
        tableParams.put("redis-db-number", STANDALONE_REDIS_DB_NUMBER);
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectZsetValue();
    }

    @Test
    public void clusterSelectPrefixWithZsetValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing clusterSelectPrefixWithZsetValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(CLUSTER_KEY));
        tableParams.put("redis-key-prefix", "customer-hm-zset*"); // prefix
        tableParams.put("redis-value-type", "zset"); // zset
        tableParams.put("redis-cluster-flag", "true");
        tableParams.put("redis-ssl-flag", "true");
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectZsetValue();
    }

    @Test
    public void clusterSelectZsetWithZsetValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing clusterSelectZsetWithZsetValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(CLUSTER_KEY));
        tableParams.put("redis-keys-zset", "key-hm-zset"); // zset
        tableParams.put("redis-value-type", "zset"); // zset
        tableParams.put("redis-cluster-flag", "true");
        tableParams.put("redis-ssl-flag", "true");
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectZsetValue();
    }

    @Test
    public void standaloneSelectPrefixWithLiteralValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing standaloneSelectPrefixWithLiteralValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(STANDALONE_KEY));
        tableParams.put("redis-key-prefix", "customer-literal:*"); // prefix
        tableParams.put("redis-value-type", "literal"); // literal
        tableParams.put("redis-cluster-flag", "false");
        tableParams.put("redis-ssl-flag", "false");
        tableParams.put("redis-db-number", STANDALONE_REDIS_DB_NUMBER);
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectLiteralValue();
    }

    @Test
    public void standaloneSelectZsetWithLiteralValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing standaloneSelectZsetWithLiteralValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(STANDALONE_KEY));
        tableParams.put("redis-keys-zset", "key-literal-zset"); // zset
        tableParams.put("redis-value-type", "literal"); // literal
        tableParams.put("redis-cluster-flag", "false");
        tableParams.put("redis-ssl-flag", "false");
        tableParams.put("redis-db-number", STANDALONE_REDIS_DB_NUMBER);
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectLiteralValue();
    }

    @Test
    public void clusterSelectPrefixWithLiteralValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing clusterSelectPrefixWithLiteralValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(CLUSTER_KEY));
        tableParams.put("redis-key-prefix", "customer-literal:*"); // prefix
        tableParams.put("redis-value-type", "literal"); // literal
        tableParams.put("redis-cluster-flag", "true");
        tableParams.put("redis-ssl-flag", "true");
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectLiteralValue();
    }

    @Test
    public void clusterSelectZsetWithLiteralValue()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing clusterSelectZsetWithLiteralValue");
        logger.info("--------------------------------------------------");

        // Setup Table 2
        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("redis-endpoint", redisEndpoints.get(CLUSTER_KEY));
        tableParams.put("redis-keys-zset", "key-literal-zset"); // zset
        tableParams.put("redis-value-type", "literal"); // literal
        tableParams.put("redis-cluster-flag", "true");
        tableParams.put("redis-ssl-flag", "true");
        TableInput tableInput = createTableInput(getGlueTable(redisDbName, redisTableNamePrefix + "_2")).withParameters(tableParams);
        glue.updateTable(new UpdateTableRequest().withDatabaseName(redisDbName).withTableInput(tableInput));

        selectLiteralValue();
    }

    // redis keys/values are all stored as plain strings, these don't really apply
    @Override
    public void selectIntegerTypeTest()
    {
    }

    @Override
    public void selectVarcharTypeTest()
    {
    }

    @Override
    public void selectBooleanTypeTest()
    {
    }

    @Override
    public void selectSmallintTypeTest()
    {
    }

    @Override
    public void selectBigintTypeTest()
    {
    }

    @Override
    public void selectFloat4TypeTest()
    {
    }

    @Override
    public void selectFloat8TypeTest()
    {
    }

    @Override
    public void selectDateTypeTest()
    {
    }

    @Override
    public void selectTimestampTypeTest()
    {
    }

    @Override
    public void selectByteArrayTypeTest()
    {
    }

    @Override
    public void selectVarcharListTypeTest()
    {
    }

    @Override
    public void selectNullValueTest()
    {
    }

    @Override
    public void selectEmptyTableTest()
    {
    }
}
