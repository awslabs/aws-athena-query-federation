/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Used in combination with an Integration-test class to create a CloudFormation stack for a DDB table, and insert
 * rows prior to running the tests.
 */
public class DdbTableUtils {
    private static final Logger logger = LoggerFactory.getLogger(DdbTableUtils.class);

    private static final int MAX_TRIES = 3;
    private static final int SLEEP_TIME_MS = 10000;
    private static final String CF_STACK_ID = "DdbTable";
    private static final long READ_CAPACITY_UNITS = 10L;
    private static final long WRITE_CAPACITY_UNITS = 10L;
    private static final String DDB_PARTITION_KEY = "title";
    private static final String DDB_SORT_KEY = "year";

    private final String tableName;
    private final AmazonDynamoDB client;

    public DdbTableUtils(String tableName) {
        this.tableName = tableName;
        client = AmazonDynamoDBClientBuilder.defaultClient();
    }

    /**
     * Sets up the DDB Table's CloudFormation stack.
     * @param stack The current CloudFormation stack.
     */
    protected void setupTableStack(final Stack stack)
    {
        Table.Builder.create(stack, CF_STACK_ID)
                .tableName(tableName)
                .billingMode(BillingMode.PROVISIONED)
                .removalPolicy(RemovalPolicy.DESTROY)
                .readCapacity(Long.valueOf(READ_CAPACITY_UNITS))
                .writeCapacity(Long.valueOf(WRITE_CAPACITY_UNITS))
                .partitionKey(Attribute.builder().name(DDB_PARTITION_KEY).type(AttributeType.STRING).build())
                .sortKey(Attribute.builder().name(DDB_SORT_KEY).type(AttributeType.NUMBER).build())
                .build();
    }

    /**
     * Insert rows into the newly created DDB table.
     */
    protected void addItems()
    {
        addItem("2014", "Interstellar", "Christopher Nolan",
                ImmutableList.of("Matthew McConaughey", "John Lithgow", "Ann Hathaway",
                        "David Gyasi", "Michael Caine", "Jessica Chastain",
                        "Matt Damon", "Casey Affleck"),
                ImmutableList.of("Adventure", "Drama", "Sci-Fi", "Thriller"));
        addItem("1986", "Aliens", "James Cameron",
                ImmutableList.of("Sigourney Weaver", "Paul Reiser", "Lance Henriksen",
                        "Bill Paxton"),
                ImmutableList.of("Adventure", "Action", "Sci-Fi", "Thriller"));
    }

    /**
     * Creates a Map containing the item's attributes and adds the item to the table.
     * @param year Year attribute.
     * @param title Title attribute.
     * @param director Director attribute.
     * @param cast List of cast members.
     * @param genre List of movie generes.
     */
    private void addItem(String year, String title, String director,
                         List<String> cast, List<String> genre)
    {
        logger.info("Add item: year=[{}], title=[{}], director=[{}], cast={}, genre={}",
                year, title, director, cast, genre);

        List<AttributeValue> castAttrib = new ArrayList<>();
        cast.forEach(value -> castAttrib.add(new AttributeValue(value)));

        List<AttributeValue> genreAttrib = new ArrayList<>();
        genre.forEach(value -> genreAttrib.add(new AttributeValue(value)));

        Map<String, AttributeValue> item = ImmutableMap.of(
                "year", new AttributeValue().withN(year),
                "title", new AttributeValue(title),
                "info", new AttributeValue().withM(ImmutableMap.of(
                        "director", new AttributeValue(director),
                        "cast", new AttributeValue().withL(castAttrib),
                        "genre", new AttributeValue().withL(genreAttrib))));

        putItem(item);
    }

    /**
     * Adds item to the table.
     * @param item Map of table attributes.
     */
    protected void putItem(Map<String, AttributeValue> item)
    {
        // Table takes a while to initialize, and may require several attempts to insert
        // the first record.
        for (int attempt = 1; attempt <= MAX_TRIES; ++attempt) {
            try {
                // Add record to table in DynamoDB service.
                logger.info("Add item attempt: {}", attempt);
                client.putItem(tableName, item);
                logger.info("Added item in {} attempt(s).", attempt);
                break;
            } catch (ResourceNotFoundException e) {
                logger.info(e.getErrorMessage());
                if (attempt < MAX_TRIES) {
                    // Sleep for 10 seconds and try again.
                    logger.info("Sleeping for 10 seconds...");
                    try {
                        Thread.sleep(SLEEP_TIME_MS);
                    }
                    catch (InterruptedException re) {
                        throw new RuntimeException("Thread.sleep interrupted: " + re.getMessage());
                    }
                }
                continue;
            } catch (AmazonServiceException e) {
                String errorMsg = String.format("Unable to add item to DynamoDB table (%s): %s",
                        tableName, e.getErrorMessage());
                throw new AmazonServiceException(errorMsg, e);
            }
        }
    }
}
