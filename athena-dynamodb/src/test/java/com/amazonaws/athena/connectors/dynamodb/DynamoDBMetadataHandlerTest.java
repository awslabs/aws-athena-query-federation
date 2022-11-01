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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.COLUMN_NAME_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.SOURCE_TABLE_PROPERTY;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler.DYNAMO_DB_FLAG;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler.MAX_SPLITS_PER_REQUEST;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_NAMES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_VALUES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.NON_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.PARTITION_TYPE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.QUERY_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SCAN_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.TABLE_METADATA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Glue logic is tested by GlueMetadataHandlerTest in SDK
 */
@RunWith(MockitoJUnitRunner.class)
public class DynamoDBMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMetadataHandlerTest.class);

    @Rule
    public TestName testName = new TestName();

    @Mock
    private AWSGlue glueClient;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    private DynamoDBMetadataHandler handler;

    private BlockAllocator allocator;

    @Before
    public void setup()
    {
        logger.info("{}: enter", testName.getMethodName());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        allocator = new BlockAllocatorImpl();
        handler = new DynamoDBMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", ddbClient, glueClient);
    }

    @After
    public void tearDown()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNamesGlueError()
            throws Exception
    {
        when(glueClient.getDatabases(any())).thenThrow(new AmazonServiceException(""));

        ListSchemasRequest req = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertThat(new ArrayList<>(res.getSchemas()), equalTo(Collections.singletonList(DEFAULT_SCHEMA)));
    }

    @Test
    public void doListSchemaNamesGlue()
            throws Exception
    {
        GetDatabasesResult result = new GetDatabasesResult().withDatabaseList(
                new Database().withName(DEFAULT_SCHEMA),
                new Database().withName("ddb").withLocationUri(DYNAMO_DB_FLAG),
                new Database().withName("s3").withLocationUri("blah"));

        when(glueClient.getDatabases(any())).thenReturn(result);

        ListSchemasRequest req = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertThat(res.getSchemas().size(), equalTo(2));
        assertThat(res.getSchemas().contains("default"), is(true));
        assertThat(res.getSchemas().contains("ddb"), is(true));
    }

    @Test
    public void doListTablesGlueAndDynamo()
            throws Exception
    {
        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        GetTablesResult mockResult = new GetTablesResult();
        List<Table> tableList = new ArrayList<>();
        tableList.add(new Table().withName("table1")
                .withParameters(ImmutableMap.of("classification", "dynamodb"))
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("some.location")));
        tableList.add(new Table().withName("table2")
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("some.location")
                        .withParameters(ImmutableMap.of("classification", "dynamodb"))));
        tableList.add(new Table().withName("table3")
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("arn:aws:dynamodb:us-east-1:012345678910:table/table3")));
        tableList.add(new Table().withName("notADynamoTable").withParameters(ImmutableMap.of()).withStorageDescriptor(
                new StorageDescriptor().withParameters(ImmutableMap.of()).withLocation("some_location")));
        mockResult.setTableList(tableList);
        when(glueClient.getTables(any())).thenReturn(mockResult);

        ListTablesRequest req = new ListTablesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, DEFAULT_SCHEMA,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());

        List<TableName> expectedTables = tableNames.stream().map(table -> new TableName(DEFAULT_SCHEMA, table)).collect(Collectors.toList());
        expectedTables.add(TEST_TABLE_NAME);
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table2"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table3"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table4"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table5"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table6"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table7"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table8"));


        assertThat(new HashSet<>(res.getTables()), equalTo(new HashSet<>(expectedTables)));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE_NAME);
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName().getSchemaName(), equalTo(DEFAULT_SCHEMA));
        assertThat(res.getTableName().getTableName(), equalTo(TEST_TABLE));
        assertThat(res.getSchema().getFields().size(), equalTo(11));
    }

    @Test
    public void doGetEmptyTable()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE_2_NAME);
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetEmptyTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(TEST_TABLE_2_NAME));
        assertThat(res.getSchema().getFields().size(), equalTo(2));
    }

    @Test
    public void testCaseInsensitiveResolve()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE_2_NAME);
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(TEST_TABLE_2_NAME));
    }

    @Test
    public void doGetTableLayoutScan()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col_3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());

        GetTableLayoutRequest req = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(TEST_CATALOG_NAME, TEST_TABLE),
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(res.getPartitions().getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(res.getPartitions().getRowCount(), equalTo(1));

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_3 = :v0 OR attribute_not_exists(#col_3) OR #col_3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_3", "col_3");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(true), ":v1", ItemUtils.toAttributeValue(null));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
    }

    @Test
    public void doGetTableLayoutQueryIndex()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        SortedRangeSet.Builder dateValueSet = SortedRangeSet.newBuilder(Types.MinorType.DATEDAY.getType(), false);
        SortedRangeSet.Builder timeValueSet = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
        LocalDateTime dateTime = LocalDateTime.of(2019, 9, 23, 11, 18, 37);
        Instant epoch = Instant.MIN; //Set to Epoch time
        dateValueSet.add(Range.equal(allocator, Types.MinorType.DATEDAY.getType(), ChronoUnit.DAYS.between(epoch, dateTime.toInstant(ZoneOffset.UTC))));
        LocalDateTime dateTime2 = dateTime.plusHours(26);
        dateValueSet.add(Range.equal(allocator, Types.MinorType.DATEDAY.getType(), ChronoUnit.DAYS.between(epoch, dateTime2.toInstant(ZoneOffset.UTC))));
        long startTime = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTime = dateTime2.toInstant(ZoneOffset.UTC).toEpochMilli();
        timeValueSet.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime, true,
                endTime, true));
        constraintsMap.put("col_4", dateValueSet.build());
        constraintsMap.put("col_5", timeValueSet.build());

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));

        logger.info("doGetTableLayout schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(QUERY_PARTITION_TYPE));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().containsKey(INDEX_METADATA), is(true));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(INDEX_METADATA), equalTo("test_index"));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(HASH_KEY_NAME_METADATA), equalTo("col_4"));
        assertThat(res.getPartitions().getRowCount(), equalTo(2));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_NAME_METADATA), equalTo("col_5"));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 BETWEEN :v0 AND :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_4", "col_4", "#col_5", "col_5");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(startTime), ":v1", ItemUtils.toAttributeValue(endTime));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));

        // Note that while we were able to fix the inclusive upper and lower bound cases, we cannot fix mixed
        // inclusion bounds for now.
        // So this key condition is expected to fail when used against a real DDB instance with:
        //    "KeyConditionExpressions must only contain one condition per key"
        // However, we still test the mixed cases below to make sure that we don't accidentally generate the BETWEEN version even though
        // this will cause customer queries with mixed inclusion to fail.
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
                true /* inclusive lowerbound */, endTime, false /* exclusive upperbound */));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 >= :v0 AND #col_5 < :v1)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
              false /* exclusive lowerbound */, endTime, true /* inclusive upperbound*/));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 > :v0 AND #col_5 <= :v1)"));
        }
        // -------------------------------------------------------------------------
        // Single bound constraint tests
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 > :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.greaterThanOrEqual(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 >= :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.lessThan(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 < :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.lessThanOrEqual(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 <= :v0)"));
        }
    }

    @Test
    public void doGetSplitsScan()
            throws Exception
    {
        GetTableLayoutResponse layoutResponse = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(ImmutableMap.of()),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));

        GetSplitsRequest req = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                layoutResponse.getPartitions(),
                ImmutableList.of(),
                new Constraints(new HashMap<>()),
                null);
        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertThat(rawResponse.getRequestType(), equalTo(MetadataRequestType.GET_SPLITS));

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(continuationToken == null, is(true));

        Split split = Iterables.getOnlyElement(response.getSplits());
        assertThat(split.getProperty(SEGMENT_ID_PROPERTY), equalTo("0"));

        logger.info("doGetSplitsScan: exit");
    }

    @Test
    public void doGetSplitsQuery()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        EquatableValueSet.Builder valueSet = EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false);
        for (int i = 0; i < 2000; i++) {
            valueSet.add("test_str_" + i);
        }
        constraintsMap.put("col_0", valueSet.build());
        GetTableLayoutResponse layoutResponse = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));

        GetSplitsRequest req = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                layoutResponse.getPartitions(),
                ImmutableList.of("col_0"),
                new Constraints(new HashMap<>()),
                null);
        logger.info("doGetSplits: req[{}]", req);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);
        assertThat(response.getRequestType(), equalTo(MetadataRequestType.GET_SPLITS));

        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(continuationToken, equalTo(String.valueOf(MAX_SPLITS_PER_REQUEST - 1)));
        assertThat(response.getSplits().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplits().stream().map(split -> split.getProperty("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));

        response = handler.doGetSplits(allocator, new GetSplitsRequest(req, continuationToken));

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(response.getContinuationToken(), equalTo(null));
        assertThat(response.getSplits().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplits().stream().map(split -> split.getProperty("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));
    }

    @Test
    public void validateSourceTableNamePropagation()
            throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int"));
        columns.add(new Column().withName("col2").withType("bigint"));
        columns.add(new Column().withName("col3").withType("string"));

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1 , col2=Col2 ,col3=Col3",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=datetime1,col3=datetime3 ");
        Table table = new Table()
                .withParameters(param)
                .withPartitionKeys()
                .withStorageDescriptor(new StorageDescriptor().withColumns(columns));
        GetTableResult mockResult = new GetTableResult().withTable(table);
        when(glueClient.getTable(any())).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, "glueTableForTestTable");
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName);
        GetTableResponse getTableResponse = handler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = getTableResponse.getSchema().getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(TEST_TABLE));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,Col3=datetime3"));

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                tableName,
                new Constraints(ImmutableMap.of()),
                getTableResponse.getSchema(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse getTableLayoutResponse = handler.doGetTableLayout(allocator, getTableLayoutRequest);
        logger.info("validateSourceTableNamePropagation: GetTableLayoutResponse[{}]", getTableLayoutResponse);
        assertThat(getTableLayoutResponse.getPartitions().getSchema().getCustomMetadata().get(TABLE_METADATA), equalTo(TEST_TABLE));
    }

    @Test
    public void doGetTableLayoutScanWithTypeOverride()
            throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int"));
        columns.add(new Column().withName("col2").withType("timestamptz"));
        columns.add(new Column().withName("col3").withType("string"));

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=datetime1,col3=datetime3 ");
        Table table = new Table()
                .withParameters(param)
                .withPartitionKeys()
                .withStorageDescriptor(new StorageDescriptor().withColumns(columns));
        GetTableResult mockResult = new GetTableResult().withTable(table);
        when(glueClient.getTable(any())).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, "glueTableForTestTable");
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName);
        GetTableResponse getTableResponse = handler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = getTableResponse.getSchema().getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(TEST_TABLE));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,col3=datetime3"));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());
        constraintsMap.put("col2",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                tableName,
                new Constraints(constraintsMap),
                getTableResponse.getSchema(),
                Collections.EMPTY_SET);


        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, getTableLayoutRequest);

        logger.info("doGetTableLayoutScanWithTypeOverride schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayoutScanWithTypeOverride partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(res.getPartitions().getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(res.getPartitions().getRowCount(), equalTo(1));

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col3 = :v0 OR attribute_not_exists(#col3) OR #col3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col3", "col3", "#col2", "col2");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(true), ":v1", ItemUtils.toAttributeValue(null));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
    }
}
