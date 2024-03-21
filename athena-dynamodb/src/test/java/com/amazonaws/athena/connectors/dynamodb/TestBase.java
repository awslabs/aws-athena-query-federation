/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.throttling.DynamoDBExceptionFilter.EXCEPTION_FILTER;

public class TestBase
{
    protected FederatedIdentity TEST_IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    protected static final String TEST_QUERY_ID = "queryId";
    protected static final String TEST_CATALOG_NAME = "default";
    protected static final String TEST_TABLE = "test_table";
    protected static final String TEST_TABLE2 = "Test_table2";
    protected static final String TEST_TABLE3 = "test_table3";
    protected static final String TEST_TABLE4 = "test_table4";
    protected static final String TEST_TABLE5 = "test_table5";
    protected static final String TEST_TABLE6 = "test_table6";
    protected static final String TEST_TABLE7 = "test_table7";
    protected static final String TEST_TABLE8 = "test_table8";
    protected static final TableName TEST_TABLE_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE);
    protected static final TableName TEST_TABLE_2_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE2);
    protected static final TableName TEST_TABLE_3_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE3);
    protected static final TableName TEST_TABLE_4_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE4);
    protected static final TableName TEST_TABLE_5_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE5);
    protected static final TableName TEST_TABLE_6_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE6);
    protected static final TableName TEST_TABLE_7_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE7);
    protected static final TableName TEST_TABLE_8_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE8);

    protected static DynamoDbClient ddbClient;
    protected static Schema schema;
    protected static DynamoDBProxyServer server;

    @BeforeClass
    public static void setupOnce() throws Exception
    {
        ddbClient = setupLocalDDB();
        ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, com.google.common.collect.ImmutableMap.of()).build();
        schema = DDBTableUtils.peekTableForSchema(TEST_TABLE, invoker, ddbClient);
    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
        server.stop();
    }

    private static String waitForTableToBecomeActive(DynamoDbClient ddb, DynamoDbWaiter dbWaiter, CreateTableResponse createTableResponse, String tableName) {
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Entered: waitForTableToBecomeActive: for: " + tableName);

        try {
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            // Wait until the Amazon DynamoDB table is created
            dbWaiter.waitUntilTableExists(tableRequest);
            String name = createTableResponse.tableDescription().tableName();
            System.out.println("Table :[ " + name + " ] is active");
            System.out.println("------------------------------------------------------------------------");
            return name;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return tableName;
    }

    public static void waitForGsiToBecomeActive(DynamoDbClient ddb, String tableName, String indexName) {
        System.out.println("------------------------------------------------------------------------");
        System.out.println("Waiting for GSI to become active: " + indexName + " in table: " + tableName);

        boolean isIndexActive = false;

        while (!isIndexActive) {
            try {
                //This is a local Dynamo DB, so it should be reasonably fast.
                Thread.sleep(5_000); // 5-second wait between checks

                DescribeTableResponse response = ddb.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
                isIndexActive = response.table().globalSecondaryIndexes().stream()
                        .anyMatch(gsi -> gsi.indexName().equals(indexName) && gsi.indexStatus().equals(IndexStatus.ACTIVE));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for the GSI to become active.", e);
            }
        }

        System.out.println("GSI " + indexName + " is now ACTIVE in table " + tableName);
        System.out.println("------------------------------------------------------------------------");
    }

    //Adapted from: https://github.com/awslabs/amazon-dynamodb-local-samples/blob/main/DynamoDBLocal
    private static DynamoDbClient setupLocalDDB() throws Exception
    {
        DynamoDbWaiter dbWaiter;
        DynamoDbClient ddbClient;
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.ERROR);

        try {
            String port = "8000";
            String uri = "http://localhost:" + port;
            // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP
            final String[] localArgs = {"-inMemory", "-port", port};
            System.out.println("Starting DynamoDB Local...");
            server = ServerRunner.createServerFromCommandLineArgs(localArgs);
            server.start();
            System.out.println("Started DynamoDB Local...");


            //  Create a client and connect to DynamoDB Local
            //  Note: This is a dummy key and secret and AWS_ACCESS_KEY_ID can contain only letters (A–Z, a–z) and numbers (0–9).
            ddbClient = DynamoDbClient.builder()
                    .endpointOverride(URI.create(uri))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .region(Region.US_WEST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummyKey", "dummySecret")))
                    .build();

            dbWaiter = ddbClient.waiter();

            // Create ProvisionedThroughput using the builder pattern
            ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder()
                    .readCapacityUnits(5L)
                    .writeCapacityUnits(6L)
                    .build();

            setUpTable1And2(provisionedThroughput, ddbClient, dbWaiter);
            setUpTable3(provisionedThroughput, ddbClient, dbWaiter);
            setUpTable4(provisionedThroughput, ddbClient, dbWaiter);
            setUpTable5(provisionedThroughput, ddbClient, dbWaiter);
            setUpTable6(provisionedThroughput, ddbClient, dbWaiter);
            setUpTable7(provisionedThroughput, ddbClient, dbWaiter);
            setUpTable8(provisionedThroughput, ddbClient, dbWaiter);

            return ddbClient;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setUpTable1And2(
            ProvisionedThroughput provisionedThroughput,
            DynamoDbClient ddb, DynamoDbWaiter dbWaiter) throws InterruptedException {

        // For AttributeDefinitions
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("col_0")
                .attributeType("S")
                .build());
        attributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("col_1")
                .attributeType("N")
                .build());

        // For KeySchema
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder()
                .attributeName("col_0")
                .keyType(KeyType.HASH)
                .build());
        keySchema.add(KeySchemaElement.builder()
                .attributeName("col_1")
                .keyType(KeyType.RANGE)
                .build());

        // Create CreateTableRequest using the builder pattern
        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(TEST_TABLE)
                .keySchema(keySchema)
                .attributeDefinitions(attributeDefinitions)
                .provisionedThroughput(provisionedThroughput)
                .build();

        CreateTableResponse table1CreateTableResponse = ddb.createTable(createTableRequest);
        String newTableName = waitForTableToBecomeActive(ddb, dbWaiter, table1CreateTableResponse, TEST_TABLE);
        System.out.println("New Table Name: " + newTableName);

        int len = 1000;
        LocalDateTime dateTime = LocalDateTime.of(2019, 9, 23, 11, 18, 37);
        List<WriteRequest> writeRequests = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            // Populating the item map
            item.put("col_0", AttributeValue.builder().s("test_str_" + (i - i % 3)).build());
            item.put("col_1", AttributeValue.builder().n(String.valueOf(i)).build());
            double doubleVal = 200000.0 + i / 2.0;
            if (Math.floor(doubleVal) != doubleVal) {
                item.put("col_2", AttributeValue.builder().n(String.valueOf(doubleVal)).build());
            }
            // Assuming you have appropriate methods to create Map and List AttributeValues
            item.put("col_3", DDBTypeUtils.toAttributeValue(ImmutableMap.of("modulo", i % 2, "nextModulos", ImmutableList.of((i + 1) % 2, ((i + 2) % 2)))));
            item.put("col_4", AttributeValue.builder().n(String.valueOf(dateTime.toLocalDate().toEpochDay())).build());
            item.put("col_5", AttributeValue.builder().n(String.valueOf(Timestamp.valueOf(dateTime).toInstant().toEpochMilli())).build());
            item.put("col_6", i % 128 == 0 ? AttributeValue.builder().nul(true).build() : AttributeValue.builder().n(String.valueOf(i % 128)).build());
            item.put("col_7", DDBTypeUtils.toAttributeValue(ImmutableList.of(-i,String.valueOf(i))));
            item.put("col_8", DDBTypeUtils.toAttributeValue((ImmutableList.of(ImmutableMap.of("mostlyEmptyMap",
                    i % 128 == 0 ? ImmutableMap.of("subtractions", ImmutableSet.of(i - 100, i - 200)) : ImmutableMap.of())))));
            item.put("col_9", AttributeValue.builder().n(String.valueOf(100.0f + i)).build());
            item.put("col_10", DDBTypeUtils.toAttributeValue(ImmutableList.of(ImmutableList.of(1 * i, 2 * i, 3 * i),
                    ImmutableList.of(4 * i, 5 * i), ImmutableList.of(6 * i, 7 * i, 8 * i))));

            byte[] sampleBytes = new byte[2];
            sampleBytes[0] = (byte) (i * len);
            sampleBytes[1] = (byte) (len - i);
            item.put("col_11", AttributeValue.builder().bs(SdkBytes.fromByteBuffer(ByteBuffer.wrap(sampleBytes))).build());

            writeRequests.add(WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build())
                    .build());

            if (writeRequests.size() == 25) {
                BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                        .requestItems(ImmutableMap.of(TEST_TABLE, writeRequests))
                        .build();

                ddb.batchWriteItem(batchWriteItemRequest);
                // Handle response, check for unprocessed items, etc.
                writeRequests.clear(); // Clear the list for the next batch
            }
            dateTime = dateTime.plusHours(26);
        }


        CreateGlobalSecondaryIndexAction createIndexRequest = CreateGlobalSecondaryIndexAction.builder()
                .indexName("test_index")
                .keySchema(KeySchemaElement.builder()
                                .keyType(KeyType.HASH)
                                .attributeName("col_4")
                                .build(),
                        KeySchemaElement.builder()
                                .keyType(KeyType.RANGE)
                                .attributeName("col_5")
                                .build())
                .projection(Projection.builder()
                        .projectionType(ProjectionType.ALL)
                        .build())
                .provisionedThroughput(provisionedThroughput)
                .build();

        UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                .tableName(TEST_TABLE)
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName("col_4")
                                .attributeType(ScalarAttributeType.N)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("col_5")
                                .attributeType(ScalarAttributeType.N)
                                .build())
                .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                        .create(createIndexRequest)
                        .build())
                .build();

        ddb.updateTable(updateTableRequest);
        dbWaiter.waitUntilTableExists(DescribeTableRequest.builder().tableName(TEST_TABLE).build());
        waitForGsiToBecomeActive(ddb, TEST_TABLE, "test_index");


        // for case sensitivity testing
        CreateTableRequest table2Request = CreateTableRequest.builder()
                .tableName(TEST_TABLE2)
                .keySchema(keySchema)
                .attributeDefinitions(attributeDefinitions)
                .provisionedThroughput(provisionedThroughput)
                .build();


        waitForTableToBecomeActive(ddb, dbWaiter, ddb.createTable(table2Request), TEST_TABLE2);

    }
    private static void setUpTable3(
            ProvisionedThroughput provisionedThroughput,
            DynamoDbClient ddb, DynamoDbWaiter dbWaiter)  {
        ///Glue Table;
        List<AttributeDefinition> attributeDefinitionsGlue = new ArrayList<>();
        attributeDefinitionsGlue.add(AttributeDefinition.builder()
                .attributeName("Col0")
                .attributeType("S")
                .build());
        attributeDefinitionsGlue.add(AttributeDefinition.builder()
                .attributeName("Col1")
                .attributeType("S")
                .build());

        List<KeySchemaElement> keySchemaGlue = new ArrayList<>();
        keySchemaGlue.add(KeySchemaElement.builder()
                .attributeName("Col0")
                .keyType(KeyType.HASH)
                .build());
        keySchemaGlue.add(KeySchemaElement.builder()
                .attributeName("Col1")
                .keyType(KeyType.RANGE)
                .build());
        CreateTableRequest table3Request = CreateTableRequest.builder()
                .tableName(TEST_TABLE3)
                .keySchema(keySchemaGlue)
                .attributeDefinitions(attributeDefinitionsGlue)
                .provisionedThroughput(provisionedThroughput)
                .build();

        waitForTableToBecomeActive(ddb, dbWaiter, ddb.createTable(table3Request), TEST_TABLE3);

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", DDBTypeUtils.toAttributeValue("hashVal"));
        item.put("Col1", DDBTypeUtils.toAttributeValue("20200227S091227"));
        item.put("Col2", DDBTypeUtils.toAttributeValue("2020-02-27T09:12:27Z"));
        item.put("Col3", DDBTypeUtils.toAttributeValue("27/02/2020"));
        item.put("Col4", DDBTypeUtils.toAttributeValue("2020-02-27"));
        item.put("Col5", DDBTypeUtils.toAttributeValue("2015-12-21T17:42:34-05:00"));
        item.put("Col6", DDBTypeUtils.toAttributeValue("2015-12-21T17:42:34Z"));
        item.put("Col7", DDBTypeUtils.toAttributeValue("2015-12-21T17:42:34"));

        List<WriteRequest> table3WriteRequest = new ArrayList<>();
        table3WriteRequest.add(WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build());

        BatchWriteItemRequest table3BatchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(TEST_TABLE3, table3WriteRequest))
                .build();
        ddb.batchWriteItem(table3BatchWriteItemRequest);

    }
    private static void setUpTable4(
            ProvisionedThroughput provisionedThroughput,
            DynamoDbClient ddb, DynamoDbWaiter dbWaiter) {

        List<AttributeDefinition> table4AttributeDefinitions = new ArrayList<>();
        table4AttributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("Col0")
                .attributeType(ScalarAttributeType.S)
                .build());

        // Define key schema for Table 4
        List<KeySchemaElement> table4KeySchema = new ArrayList<>();
        table4KeySchema.add(KeySchemaElement.builder()
                .attributeName("Col0")
                .keyType(KeyType.HASH)
                .build());

        // Create Table Request for Table 4
        CreateTableRequest table4TableRequest = CreateTableRequest.builder()
                .tableName(TEST_TABLE4)
                .keySchema(table4KeySchema)
                .attributeDefinitions(table4AttributeDefinitions)
                .provisionedThroughput(ProvisionedThroughput.builder() // Assuming 'provisionedThroughput' is defined
                        .readCapacityUnits(5L)  // Example capacity units
                        .writeCapacityUnits(5L)
                        .build())
                .build();

        waitForTableToBecomeActive(ddb, dbWaiter, ddb.createTable(table4TableRequest), TEST_TABLE4);

        Map<String, AttributeValue> col1 = new HashMap<>();
        col1.put("field1", AttributeValue.builder().s("someField1").build());
        col1.put("field2", AttributeValue.builder().nul(true).build()); // Representing null

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", AttributeValue.builder().s("hashVal").build());
        item.put("Col1", AttributeValue.builder().m(col1).build());

        WriteRequest writeRequest = WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build();

        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(TEST_TABLE4, ImmutableList.of(writeRequest)))
                .build();

        ddb.batchWriteItem(batchWriteItemRequest);
    }

    private static void setUpTable5(
            ProvisionedThroughput provisionedThroughput,
            DynamoDbClient ddb, DynamoDbWaiter dbWaiter) {

        List<AttributeDefinition> table5AttributeDefinitions = new ArrayList<>();
        table5AttributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("Col0")
                .attributeType(ScalarAttributeType.S)
                .build());

        List<KeySchemaElement> table5KeySchema = new ArrayList<>();
        table5KeySchema.add(KeySchemaElement.builder()
                .attributeName("Col0")
                .keyType(KeyType.HASH)
                .build());

        CreateTableRequest table5CreateTableRequest = CreateTableRequest.builder()
                .tableName(TEST_TABLE5)
                .keySchema(table5KeySchema)
                .attributeDefinitions(table5AttributeDefinitions)
                .provisionedThroughput(provisionedThroughput)
                .build();

        waitForTableToBecomeActive(ddb, dbWaiter, ddb.createTable(table5CreateTableRequest), TEST_TABLE5);

        // Nested collection
        Map<String, AttributeValue> nestedCol = new HashMap<>();
        ArrayList<String> value = new ArrayList<>();
        value.add("list1");
        value.add("list2");
        nestedCol.put("list", AttributeValue.builder()
                .l(value.stream().map(s -> AttributeValue.builder().s(s).build()).collect(Collectors.toList()))
                .build());

        // Structured collection
        Map<String, AttributeValue> listStructCol = new HashMap<>();
        Map<String, AttributeValue> structVal = new HashMap<>();
        structVal.put("key1", AttributeValue.builder().s("str1").build());
        structVal.put("key2", AttributeValue.builder().s("str2").build());
        listStructCol.put("structKey", AttributeValue.builder().m(structVal).build());

        // Item to write
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", AttributeValue.builder().s("hashVal").build());
        item.put("outermap", AttributeValue.builder().m(nestedCol).build());
        item.put("structcol", AttributeValue.builder().m(listStructCol).build());

        // Batch write request
        WriteRequest writeRequest = WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build();
        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(TEST_TABLE5, ImmutableList.of(writeRequest)))
                .build();

        // Perform the batch write operation
        ddb.batchWriteItem(batchWriteItemRequest);
    }

    private static void setUpTable6(
            ProvisionedThroughput provisionedThroughput,
            DynamoDbClient ddb, DynamoDbWaiter dbWaiter)
            throws InterruptedException {

        // Define attribute definitions for Table 6
        List<AttributeDefinition> table6AttributeDefinitions = new ArrayList<>();
        table6AttributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("Col0")
                .attributeType(ScalarAttributeType.S)
                .build());

        // Define key schema for Table 6
        List<KeySchemaElement> table6KeySchema = new ArrayList<>();
        table6KeySchema.add(KeySchemaElement.builder()
                .attributeName("Col0")
                .keyType(KeyType.HASH)
                .build());

        // Create Table Request for Table 6
        CreateTableRequest table6CreateTableRequest = CreateTableRequest.builder()
                .tableName(TEST_TABLE6)
                .keySchema(table6KeySchema)
                .attributeDefinitions(table6AttributeDefinitions)
                .provisionedThroughput(provisionedThroughput)
                .build();


        waitForTableToBecomeActive(ddb, dbWaiter, ddb.createTable(table6CreateTableRequest), TEST_TABLE6); // Use the same waitForTableToBecomeActive method

        // Prepare nested collections for batch write
        ArrayList<String> value = new ArrayList<>();
        value.add("list1");
        value.add("list2");
        Map<String, ArrayList<String>> nestedCol = new HashMap<>();
        nestedCol.put("list", value);

        Map<String, String> structVal = new HashMap<>();
        structVal.put("key1", "str1");
        structVal.put("key2", "str2");
        Map<String, Map<String, String>> listStructCol = new HashMap<>();
        listStructCol.put("structKey", structVal);

        // Batch writes items to Table 6
        Map<String, AttributeValue> table6Item = new HashMap<>();
        // Assuming DDBTypeUtils.toAttributeValue is properly defined and imported
        table6Item.put("Col0", DDBTypeUtils.toAttributeValue("hashVal"));
        table6Item.put("outermap", DDBTypeUtils.toAttributeValue(nestedCol));
        table6Item.put("structcol", DDBTypeUtils.toAttributeValue(listStructCol));

        WriteRequest table6WriteRequest = WriteRequest.builder()
                .putRequest(PutRequest.builder().item(table6Item).build())
                .build();

        List<WriteRequest> table6WriteRequests = new ArrayList<>();
        table6WriteRequests.add(table6WriteRequest);

        BatchWriteItemRequest table6BatchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(TEST_TABLE6, table6WriteRequests))
                .build();

        ddb.batchWriteItem(table6BatchWriteItemRequest);
    }

    private static void setUpTable7(
            ProvisionedThroughput provisionedThroughput,
            DynamoDbClient ddb, DynamoDbWaiter dbWaiter)
            throws InterruptedException {

        List<AttributeDefinition> table7AttributeDefinitions = new ArrayList<>();
        table7AttributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("Col0")
                .attributeType(ScalarAttributeType.S)
                .build());

        List<KeySchemaElement> table7KeySchema = new ArrayList<>();
        table7KeySchema.add(KeySchemaElement.builder()
                .attributeName("Col0")
                .keyType(KeyType.HASH)
                .build());

        CreateTableRequest table7CreateTableRequest = CreateTableRequest.builder()
                .tableName(TEST_TABLE7)
                .keySchema(table7KeySchema)
                .attributeDefinitions(table7AttributeDefinitions)
                .provisionedThroughput(provisionedThroughput)
                .build();

        CreateTableResponse table7CreateTableResponse = ddb.createTable(table7CreateTableRequest);
        waitForTableToBecomeActive(ddb, dbWaiter, table7CreateTableResponse, TEST_TABLE7);

        // Prepare data for batch write
        ArrayList<String> stringList = new ArrayList<>();
        stringList.add("list1");
        stringList.add("list2");

        ArrayList<Integer> intList = new ArrayList<>();
        intList.add(0);
        intList.add(1);
        intList.add(2);

        ArrayList<Map<String, String>> listStructCol = new ArrayList<>();
        Map<String, String> structVal = new HashMap<>();
        structVal.put("key1", "str1");
        structVal.put("key2", "str2");
        Map<String, String> structVal2 = new HashMap<>();
        structVal2.put("key1", "str11");
        structVal2.put("key2", "str22");
        listStructCol.add(structVal);
        listStructCol.add(structVal2);

        // Batch writes items to Table 7
        Map<String, AttributeValue> table7Item = new HashMap<>();
        table7Item.put("Col0", DDBTypeUtils.toAttributeValue("hashVal"));
        table7Item.put("stringList", DDBTypeUtils.toAttributeValue(stringList));
        table7Item.put("intList", DDBTypeUtils.toAttributeValue(intList));
        table7Item.put("listStructCol", DDBTypeUtils.toAttributeValue(listStructCol));

        WriteRequest table7WriteRequest = WriteRequest.builder()
                .putRequest(PutRequest.builder().item(table7Item).build())
                .build();

        List<WriteRequest> table7WriteRequests = new ArrayList<>();
        table7WriteRequests.add(table7WriteRequest);

        BatchWriteItemRequest table7BatchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(TEST_TABLE7, table7WriteRequests))
                .build();

        ddb.batchWriteItem(table7BatchWriteItemRequest);
    }

    private static void setUpTable8(
            ProvisionedThroughput provisionedThroughput,
            DynamoDbClient ddb, DynamoDbWaiter dbWaiter)
            throws InterruptedException {

        List<AttributeDefinition> table8AttributeDefinitions = new ArrayList<>();
        table8AttributeDefinitions.add(AttributeDefinition.builder()
                .attributeName("Col0")
                .attributeType(ScalarAttributeType.S)
                .build());

        List<KeySchemaElement> table8KeySchema = new ArrayList<>();
        table8KeySchema.add(KeySchemaElement.builder()
                .attributeName("Col0")
                .keyType(KeyType.HASH)
                .build());

        CreateTableRequest table8CreateTableRequest = CreateTableRequest.builder()
                .tableName(TEST_TABLE8)
                .keySchema(table8KeySchema)
                .attributeDefinitions(table8AttributeDefinitions)
                .provisionedThroughput(provisionedThroughput)
                .build();

        CreateTableResponse table8CreateTableResponse = ddb.createTable(table8CreateTableRequest);
        waitForTableToBecomeActive(ddb, dbWaiter, table8CreateTableResponse, TEST_TABLE8);

        // Prepare data for batch write
        Map<String, Integer> numMap = new HashMap<>();
        numMap.put("key1", 1);
        numMap.put("key2", 2);

        // Batch writes items to Table 8
        Map<String, AttributeValue> table8Item = new HashMap<>();
        table8Item.put("Col0", DDBTypeUtils.toAttributeValue("hashVal"));
        table8Item.put("nummap", DDBTypeUtils.toAttributeValue(numMap));

        WriteRequest table8WriteRequest = WriteRequest.builder()
                .putRequest(PutRequest.builder().item(table8Item).build())
                .build();

        List<WriteRequest> table8WriteRequests = new ArrayList<>();
        table8WriteRequests.add(table8WriteRequest);

        BatchWriteItemRequest table8BatchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(TEST_TABLE8, table8WriteRequests))
                .build();

        ddb.batchWriteItem(table8BatchWriteItemRequest);
    }
}
