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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.throttling.DynamoDBExceptionFilter.EXCEPTION_FILTER;
import static com.amazonaws.services.dynamodbv2.document.ItemUtils.toAttributeValue;
import static com.amazonaws.services.dynamodbv2.document.ItemUtils.toItem;

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

    protected static AmazonDynamoDB ddbClient;
    protected static Schema schema;
    protected static Table tableDdbNoGlue;

    @BeforeClass
    public static void setupOnce() throws Exception
    {
        ddbClient = setupDatabase();
        ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER).build();
        schema = DDBTableUtils.peekTableForSchema(TEST_TABLE, invoker, ddbClient);
    }

    @AfterClass
    public static void tearDownOnce()
    {
        ddbClient.shutdown();
    }

    private static AmazonDynamoDB setupDatabase() throws InterruptedException
    {
        System.setProperty("sqlite4java.library.path", "native-libs");
        AmazonDynamoDB client = DynamoDBEmbedded.create().amazonDynamoDB();
        DynamoDB ddb = new DynamoDB(client);

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("col_0").withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("col_1").withAttributeType("N"));

        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("col_0").withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName("col_1").withKeyType(KeyType.RANGE));

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withReadCapacityUnits(5L)
                .withWriteCapacityUnits(6L);
        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput);

        Table table = ddb.createTable(createTableRequest);

        table.waitForActive();

        TableWriteItems tableWriteItems = new TableWriteItems(TEST_TABLE);
        int len = 1000;
        LocalDateTime dateTime = LocalDateTime.of(2019, 9, 23, 11, 18, 37);
        for (int i = 0; i < len; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("col_0", toAttributeValue("test_str_" + (i - i % 3)));
            item.put("col_1", toAttributeValue(i));
            double doubleVal = 200000.0 + i / 2.0;
            if (Math.floor(doubleVal) != doubleVal) {
                item.put("col_2", toAttributeValue(200000.0 + i / 2.0));
            }
            item.put("col_3", toAttributeValue(ImmutableMap.of("modulo", i % 2, "nextModulos", ImmutableList.of((i + 1) % 2, ((i + 2) % 2)))));
            item.put("col_4", toAttributeValue(dateTime.toLocalDate().toEpochDay()));
            item.put("col_5", toAttributeValue(Timestamp.valueOf(dateTime).toInstant().toEpochMilli()));
            item.put("col_6", toAttributeValue(i % 128 == 0 ? null : i % 128));
            item.put("col_7", toAttributeValue(ImmutableList.of(-i, String.valueOf(i))));
            item.put("col_8", toAttributeValue(ImmutableList.of(ImmutableMap.of("mostlyEmptyMap",
                    i % 128 == 0 ? ImmutableMap.of("subtractions", ImmutableSet.of(i - 100, i - 200)) : ImmutableMap.of()))));
            item.put("col_9", toAttributeValue(100.0f + i));
            item.put("col_10", toAttributeValue(ImmutableList.of(ImmutableList.of(1 * i, 2 * i, 3 * i),
                    ImmutableList.of(4 * i, 5 * i), ImmutableList.of(6 * i, 7 * i, 8 * i))));
            tableWriteItems.addItemToPut(toItem(item));

            if (tableWriteItems.getItemsToPut().size() == 25) {
                ddb.batchWriteItem(tableWriteItems);
                tableWriteItems = new TableWriteItems(TEST_TABLE);
            }

            dateTime = dateTime.plusHours(26);
        }

        CreateGlobalSecondaryIndexAction createIndexRequest = new CreateGlobalSecondaryIndexAction()
                .withIndexName("test_index")
                .withKeySchema(
                        new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName("col_4"),
                        new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName("col_5"))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(provisionedThroughput);
        Index gsi = table.createGSI(createIndexRequest,
                new AttributeDefinition().withAttributeName("col_4").withAttributeType(ScalarAttributeType.N),
                new AttributeDefinition().withAttributeName("col_5").withAttributeType(ScalarAttributeType.N));
        gsi.waitForActive();

        // for case sensitivity testing
        createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE2)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput);
        table = ddb.createTable(createTableRequest);
        table.waitForActive();

        ArrayList<AttributeDefinition> attributeDefinitionsGlue = new ArrayList<>();
        attributeDefinitionsGlue.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));
        attributeDefinitionsGlue.add(new AttributeDefinition().withAttributeName("Col1").withAttributeType("S"));

        ArrayList<KeySchemaElement> keySchemaGlue = new ArrayList<>();
        keySchemaGlue.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(KeyType.HASH));
        keySchemaGlue.add(new KeySchemaElement().withAttributeName("Col1").withKeyType(KeyType.RANGE));

        CreateTableRequest createTableRequestGlue = new CreateTableRequest()
                .withTableName(TEST_TABLE3)
                .withKeySchema(keySchemaGlue)
                .withAttributeDefinitions(attributeDefinitionsGlue)
                .withProvisionedThroughput(provisionedThroughput);

        Table tableGlue = ddb.createTable(createTableRequestGlue);
        tableGlue.waitForActive();

        tableWriteItems = new TableWriteItems(TEST_TABLE3);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("Col1", toAttributeValue("20200227S091227"));
        item.put("Col2", toAttributeValue("2020-02-27T09:12:27Z"));
        item.put("Col3", toAttributeValue("27/02/2020"));
        item.put("Col4", toAttributeValue("2020-02-27"));
        // below three columns are testing timestamp with timezone
        // col5 with non-utc timezone, col6 with utc timezone, and c7 without timezone that will fall back to   default
        item.put("Col5", toAttributeValue("2015-12-21T17:42:34-05:00"));
        item.put("Col6", toAttributeValue("2015-12-21T17:42:34Z"));
        item.put("Col7", toAttributeValue("2015-12-21T17:42:34"));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);

        // Table 4
        attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));

        keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(KeyType.HASH));

        createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE4)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput);

        tableDdbNoGlue = ddb.createTable(createTableRequest);
        tableDdbNoGlue.waitForActive();

        Map<String, String> col1 = new HashMap<>();
        col1.put("field1", "someField1");
        col1.put("field2", null);

        tableWriteItems = new TableWriteItems(TEST_TABLE4);
        item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("Col1", toAttributeValue(col1));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);

        // Table5
        attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));

        keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(KeyType.HASH));

        createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE5)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput);

        tableDdbNoGlue = ddb.createTable(createTableRequest);
        tableDdbNoGlue.waitForActive();

        Map<String, ArrayList<String>> nestedCol = new HashMap<>();
        ArrayList<String> value = new ArrayList();
        value.add("list1");
        value.add("list2");
        nestedCol.put("list", value);

        Map<String, Map<String, String>> listStructCol = new HashMap<>();
        Map<String, String> structVal = new HashMap<>();
        structVal.put("key1", "str1");
        structVal.put("key2", "str2");
        listStructCol.put("structKey", structVal);

        tableWriteItems = new TableWriteItems(TEST_TABLE5);
        item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("outermap", toAttributeValue(nestedCol));
        item.put("structcol", toAttributeValue(listStructCol));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);

        setUpTable6(provisionedThroughput, ddb);
        setUpTable7(provisionedThroughput, ddb);
        setUpTable8(provisionedThroughput, ddb);
        return client;
    }

    private static void setUpTable6(
            ProvisionedThroughput provisionedThroughput,
            DynamoDB ddb)
            throws InterruptedException
    {

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));

        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(KeyType.HASH));

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE6)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput);

        tableDdbNoGlue = ddb.createTable(createTableRequest);
        tableDdbNoGlue.waitForActive();

        Map<String, ArrayList<String>> nestedCol = new HashMap<>();
        ArrayList<String> value = new ArrayList();
        value.add("list1");
        value.add("list2");
        nestedCol.put("list", value);

        Map<String, Map<String, String>> listStructCol = new HashMap<>();
        Map<String, String> structVal = new HashMap<>();
        structVal.put("key1", "str1");
        structVal.put("key2", "str2");
        listStructCol.put("structKey", structVal);

        TableWriteItems tableWriteItems = new TableWriteItems(TEST_TABLE6);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("outermap", toAttributeValue(nestedCol));
        item.put("structcol", toAttributeValue(listStructCol));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);
    }

    private static void setUpTable7(
            ProvisionedThroughput provisionedThroughput,
            DynamoDB ddb)
            throws InterruptedException
    {

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));

        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(KeyType.HASH));

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE7)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput);

        tableDdbNoGlue = ddb.createTable(createTableRequest);
        tableDdbNoGlue.waitForActive();

        ArrayList<String> stringList = new ArrayList();
        stringList.add("list1");
        stringList.add("list2");

        ArrayList<Integer> intList = new ArrayList();
        intList.add(0);
        intList.add(1);
        intList.add(2);

        ArrayList<Map<String, String>> listStructCol = new ArrayList();
        Map<String, String> structVal = new HashMap<>();
        structVal.put("key1", "str1");
        structVal.put("key2", "str2");
        Map<String, String> structVal2 = new HashMap<>();
        structVal2.put("key1", "str11");
        structVal2.put("key2", "str22");
        listStructCol.add(structVal);
        listStructCol.add(structVal2);

        TableWriteItems tableWriteItems = new TableWriteItems(TEST_TABLE7);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("stringList", toAttributeValue(stringList));
        item.put("intList", toAttributeValue(intList));
        item.put("listStructCol", toAttributeValue(listStructCol));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);
    }

    private static void setUpTable8(
            ProvisionedThroughput provisionedThroughput,
            DynamoDB ddb)
            throws InterruptedException
    {

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));

        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(KeyType.HASH));

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE8)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput);

        tableDdbNoGlue = ddb.createTable(createTableRequest);
        tableDdbNoGlue.waitForActive();

        Map<String, Integer> numMap = new HashMap<>();
        numMap.put("key1", 1);
        numMap.put("key2", 2);

        TableWriteItems tableWriteItems = new TableWriteItems(TEST_TABLE8);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("nummap", toAttributeValue(numMap));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);
    }
}
