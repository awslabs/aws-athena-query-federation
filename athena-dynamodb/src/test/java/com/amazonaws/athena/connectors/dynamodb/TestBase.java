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
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler.DEFAULT_SCHEMA;
import static com.amazonaws.services.dynamodbv2.document.ItemUtils.toAttributeValue;
import static com.amazonaws.services.dynamodbv2.document.ItemUtils.toItem;

public class TestBase
{
    protected FederatedIdentity TEST_IDENTITY = new FederatedIdentity("id", "principal", "account");
    protected static final String TEST_QUERY_ID = "queryId";
    protected static final String TEST_CATALOG_NAME = "default";
    protected static final String TEST_TABLE = "test_table";
    protected static final TableName TEST_TABLE_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE);

    protected static AmazonDynamoDB ddbClient;
    protected static Schema schema;

    @BeforeClass
    public static void setupOnce() throws Exception
    {
        ddbClient = setupDatabase();
        schema = DDBTableUtils.peekTableForSchema(TEST_TABLE, ddbClient);
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
            item.put("col_0", toAttributeValue("test_str_" + i));
            item.put("col_1", toAttributeValue(i));
            item.put("col_2", toAttributeValue(200000.0 + i / 2.0));
            item.put("col_3", toAttributeValue(ImmutableMap.of("modulo", i % 2 == 0, "nextModulos", ImmutableList.of((i + 1) % 2 == 0, ((i + 2) % 2 == 0)))));
            item.put("col_4", toAttributeValue(dateTime.toLocalDate().toEpochDay()));
            item.put("col_5", toAttributeValue(Timestamp.valueOf(dateTime).toInstant().toEpochMilli()));
            item.put("col_6", toAttributeValue(i % 128));
            item.put("col_7", toAttributeValue(-i));
            item.put("col_8", toAttributeValue(ImmutableSet.of(i - 100, i - 200)));
            item.put("col_9", toAttributeValue(100.0f + i));
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

        return client;
    }
}
