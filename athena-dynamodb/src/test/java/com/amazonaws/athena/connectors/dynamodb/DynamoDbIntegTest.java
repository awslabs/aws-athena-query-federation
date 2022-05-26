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

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.services.athena.model.Datum;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;

/**
 * Integration-tests for the DynamoDB connector using the Integration-test module.
 */
public class DynamoDbIntegTest extends IntegrationTestBase {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDbIntegTest.class);

    private final String dynamodbDbName;
    private final String lambdaFunctionName;
    private final String movieTableName;
    private final DdbTableUtils tableUtils;

    private static final String DDB_PARTITION_KEY = "title";
    private static final String DDB_SORT_KEY = "year";

    public DynamoDbIntegTest()
    {
        Map<String, Object> userSettings = getUserSettings().orElseThrow(() ->
                new RuntimeException("user_settings attribute must be provided in test-config.json."));
        dynamodbDbName = (String) userSettings.get("dynamodb_db_name");
        lambdaFunctionName = getLambdaFunctionName();
        movieTableName = String.format("dynamodbit_%s", UUID.randomUUID().toString().replace('-', '_'));
        tableUtils = new DdbTableUtils();
    }

    /**
     * Insert rows into the newly created DDB table.
     */
    protected void addMovieItems()
    {
        tableUtils.putItem(movieTableName, createMovieItem("2014", "Interstellar", "Christopher Nolan",
                ImmutableList.of("Matthew McConaughey", "John Lithgow", "Ann Hathaway",
                        "David Gyasi", "Michael Caine", "Jessica Chastain",
                        "Matt Damon", "Casey Affleck"),
                ImmutableList.of("Adventure", "Drama", "Sci-Fi", "Thriller")));
        tableUtils.putItem(movieTableName, createMovieItem("1986", "Aliens", "James Cameron",
                ImmutableList.of("Sigourney Weaver", "Paul Reiser", "Lance Henriksen",
                        "Bill Paxton"),
                ImmutableList.of("Adventure", "Action", "Sci-Fi", "Thriller")));
    }

    private void addDatatypeItems()
    {
        Map<String, AttributeValue> item = new ImmutableMap.Builder<String, AttributeValue>()
            .put("int_type", new AttributeValue().withN(String.valueOf(TEST_DATATYPES_INT_VALUE)))
            .put("smallint_type", new AttributeValue().withN(String.valueOf(TEST_DATATYPES_SHORT_VALUE)))
            .put("bigint_type", new AttributeValue().withN(String.valueOf(TEST_DATATYPES_LONG_VALUE)))
            .put("varchar_type", new AttributeValue(TEST_DATATYPES_VARCHAR_VALUE))
            .put("boolean_type", new AttributeValue().withBOOL(TEST_DATATYPES_BOOLEAN_VALUE))
            .put("float4_type", new AttributeValue(String.valueOf(TEST_DATATYPES_SINGLE_PRECISION_VALUE)))
            .put("float8_type", new AttributeValue(String.valueOf(1E-130))) // smallest number dynamo can handle
            .put("date_type", new AttributeValue(TEST_DATATYPES_DATE_VALUE))
            .put("timestamp_type", new AttributeValue(TEST_DATATYPES_TIMESTAMP_VALUE))
            .put("byte_type", new AttributeValue().withB(ByteBuffer.wrap(TEST_DATATYPES_BYTE_ARRAY_VALUE)))
            .put("textarray_type", new AttributeValue(TEST_DATATYPES_VARCHAR_ARRAY_VALUE)).build();
        tableUtils.putItem(TEST_DATATYPES_TABLE_NAME, item);
    }


    /**
     * Creates and returns a Map containing the item's attributes and adds the item to the table.
     * @param year Year attribute.
     * @param title Title attribute.
     * @param director Director attribute.
     * @param cast List of cast members.
     * @param genre List of movie generes.
     */
    private Map<String, AttributeValue> createMovieItem(String year, String title, String director,
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

        return item;
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        return Optional.of(PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(ImmutableList.of("dynamodb:DescribeTable", "dynamodb:ListSchemas",
                                "dynamodb:ListTables", "dynamodb:Query", "dynamodb:Scan"))
                        .resources(ImmutableList.of("*"))
                        .effect(Effect.ALLOW)
                        .build()))
                .build());
    }

    /**
     * Sets the environment variables for the Lambda function.
     */
    @Override
    protected void setConnectorEnvironmentVars(final Map environmentVars)
    {
        // This is a no-op for this connector.
    }

    /**
     * Sets up the DDB Table's CloudFormation stack.
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        tableUtils.setupTableStack(movieTableName, DDB_PARTITION_KEY, DDB_SORT_KEY, stack);
        tableUtils.setupTableStack(TEST_DATATYPES_TABLE_NAME, "varchar_type", "int_type", stack);
        tableUtils.setupTableStack(TEST_EMPTY_TABLE_NAME, "varchar_type", "int_type", stack);
        tableUtils.setupTableStack(TEST_NULL_TABLE_NAME, "varchar_type", "int_type", stack);
    }

    /**
     * Insert rows into the newly created DDB table.
     */
    @Override
    protected void setUpTableData()
    {
        logger.info("----------------------------------------------------");
        logger.info("Setting up DB table: {}", movieTableName);
        logger.info("----------------------------------------------------");

        addMovieItems();
        addDatatypeItems();
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains("default"));
    }

    @Test
    public void listTablesIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");

        List tableNames = listTables(dynamodbDbName);
        logger.info("Tables: {}", tableNames);
        assertTrue(String.format("Table not found: %s.", movieTableName), tableNames.contains(movieTableName));
    }

    @Test
    public void describeTableIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing describeTableIntegTest");
        logger.info("--------------------------------------");

        Map schema = describeTable(dynamodbDbName, movieTableName);
        logger.info("Schema: {}", schema);
        assertEquals("Wrong number of columns found.", 3, schema.size());
        assertTrue("Column not found: title", schema.containsKey("title"));
        assertTrue("Column not found: year", schema.containsKey("year"));
        assertTrue("Column not found: info", schema.containsKey("info"));
        assertEquals("Wrong column type.", "varchar", schema.get("title"));
        assertEquals("Wrong column type.", "decimal(38,9)", schema.get("year"));
        assertEquals("Wrong column type.",
                "struct<cast:array<varchar>,director:varchar,genre:array<varchar>>",schema.get("info"));
    }

    @Test
    public void selectColumnWithPredicateIntegTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select title from %s.%s.%s where year > 2000;",
                lambdaFunctionName, dynamodbDbName, movieTableName);
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
    public void selectNullValueTest()
    {
        // not applicable
    }

    @Test
    public void selectFloat8TypeTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing selectFloat8TypeTest");
        logger.info("--------------------------------------");

        String query = String.format("select float8_type from %s.%s.%s;",
                lambdaFunctionName, INTEG_TEST_DATABASE_NAME, TEST_DATATYPES_TABLE_NAME);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<Double> values = new ArrayList<>();
        rows.forEach(row -> values.add(Double.valueOf(row.getData().get(0).getVarCharValue())));
        AssertJUnit.assertEquals("Wrong number of DB records found.", 1, values.size());
        AssertJUnit.assertTrue("Float8 not found: " + 1E-130, values.contains(1E-130));
    }

}
