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
import com.amazonaws.athena.connector.lambda.ProtoUtils;
import com.amazonaws.athena.connector.lambda.data.Block;
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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
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
import org.mockito.junit.MockitoJUnitRunner;
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

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
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
import static org.mockito.ArgumentMatchers.any;
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
        handler = new DynamoDBMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", ddbClient, glueClient, com.google.common.collect.ImmutableMap.of());
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

        ListSchemasRequest req = ListSchemasRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .build();
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemasList());

        assertThat(new ArrayList<>(res.getSchemasList()), equalTo(Collections.singletonList(DEFAULT_SCHEMA)));
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

        ListSchemasRequest req = ListSchemasRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .build();
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemasList());

        assertThat(res.getSchemasList().size(), equalTo(2));
        assertThat(res.getSchemasList().contains("default"), is(true));
        assertThat(res.getSchemasList().contains("ddb"), is(true));
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

        ListTablesRequest req = ListTablesRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .setSchemaName(DEFAULT_SCHEMA)
            .setPageSize(UNLIMITED_PAGE_SIZE_VALUE)
            .build();
        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTablesList());

        List<com.amazonaws.athena.connector.lambda.proto.domain.TableName> expectedTables = 
            tableNames.stream().map(table -> new TableName(DEFAULT_SCHEMA, table)).map(ProtoUtils::toTableName).collect(Collectors.toList());
        expectedTables.add(ProtoUtils.toTableName(TEST_TABLE_NAME));
        expectedTables.add(ProtoUtils.toTableName(new TableName(DEFAULT_SCHEMA, "test_table2")));
        expectedTables.add(ProtoUtils.toTableName(new TableName(DEFAULT_SCHEMA, "test_table3")));
        expectedTables.add(ProtoUtils.toTableName(new TableName(DEFAULT_SCHEMA, "test_table4")));
        expectedTables.add(ProtoUtils.toTableName(new TableName(DEFAULT_SCHEMA, "test_table5")));
        expectedTables.add(ProtoUtils.toTableName(new TableName(DEFAULT_SCHEMA, "test_table6")));
        expectedTables.add(ProtoUtils.toTableName(new TableName(DEFAULT_SCHEMA, "test_table7")));
        expectedTables.add(ProtoUtils.toTableName(new TableName(DEFAULT_SCHEMA, "test_table8")));

        // there was a bug in these tests before - the ExampleMetadataHandler doesn't actually sort the tables if it has no pagination,
        // but the tests implied they were supposed to by comparing the objects. However, the equals method defined in the old Response class
        // just checked if the two lists had all the same values (unordered). Because the equals method is now more refined for the generated
        // protobuf class, we have to manually do the same checks.
        assertThat(new HashSet<>(res.getTablesList()), equalTo(new HashSet<>(expectedTables)));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = GetTableRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
            .build();
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName().getSchemaName(), equalTo(DEFAULT_SCHEMA));
        assertThat(res.getTableName().getTableName(), equalTo(TEST_TABLE));
        assertThat(ProtoUtils.fromProtoSchema(allocator, res.getSchema()).getFields().size(), equalTo(11));
    }

    @Test
    public void doGetEmptyTable()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = GetTableRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .setTableName(ProtoUtils.toTableName(TEST_TABLE_2_NAME))
            .build();
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetEmptyTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(ProtoUtils.toTableName(TEST_TABLE_2_NAME)));
        assertThat(ProtoUtils.fromProtoSchema(allocator, res.getSchema()).getFields().size(), equalTo(2));
    }

    @Test
    public void testCaseInsensitiveResolve()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = GetTableRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .setTableName(ProtoUtils.toTableName(TEST_TABLE_2_NAME))
            .build();
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(ProtoUtils.toTableName(TEST_TABLE_2_NAME)));
    }

    @Test
    public void doGetTableLayoutScan()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col_3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());

        GetTableLayoutRequest req = GetTableLayoutRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .setTableName(ProtoUtils.toTableName(new TableName(TEST_CATALOG_NAME, TEST_TABLE)))
            .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
            .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
            .build();

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);
        Block partitionsBlock = ProtoUtils.fromProtoBlock(allocator, res.getPartitions());

        logger.info("doGetTableLayout schema - {}", partitionsBlock.getSchema());
        logger.info("doGetTableLayout partitions - {}", partitionsBlock);

        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(partitionsBlock.getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(partitionsBlock.getRowCount(), equalTo(1));

        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_3 = :v0 OR attribute_not_exists(#col_3) OR #col_3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_3", "col_3");
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(true), ":v1", ItemUtils.toAttributeValue(null));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
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

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
        Block partitionsBlock = ProtoUtils.fromProtoBlock(allocator, res.getPartitions());

        logger.info("doGetTableLayout schema - {}", partitionsBlock.getSchema());
        logger.info("doGetTableLayout partitions - {}", partitionsBlock);

        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(QUERY_PARTITION_TYPE));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().containsKey(INDEX_METADATA), is(true));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(INDEX_METADATA), equalTo("test_index"));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(HASH_KEY_NAME_METADATA), equalTo("col_4"));
        assertThat(partitionsBlock.getRowCount(), equalTo(2));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(RANGE_KEY_NAME_METADATA), equalTo("col_5"));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 BETWEEN :v0 AND :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_4", "col_4", "#col_5", "col_5");
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(startTime), ":v1", ItemUtils.toAttributeValue(endTime));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));

        // Tests to validate that we correctly generate predicates that avoid this error:
        //    "KeyConditionExpressions must only contain one condition per key"
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
                true /* inclusive lowerbound */, endTime, false /* exclusive upperbound */));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            
            // Verify that only the upper bound is present
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 < :v0)"));
        }

        // For the same filters that we applied above, validate that we still get two conditions for non sort keys
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
                true /* inclusive lowerbound */, endTime, false /* exclusive upperbound */));
            constraintsMap.put("col_6", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            // Verify that both bounds are present for col_6 which is not a sort key
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_6 < :v1 AND #col_6 >= :v2)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
              false /* exclusive lowerbound */, endTime, true /* inclusive upperbound*/));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            // Verify that only the upper bound is present
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 <= :v0)"));
        }

        // For the same filters that we applied above, validate that we still get two conditions for non sort keys
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
                false /* exclusive lowerbound */, endTime, true /* inclusive upperbound */));
            constraintsMap.put("col_6", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            // Verify that both bounds are present for col_6 which is not a sort key
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_6 <= :v1 AND #col_6 > :v2)"));
        }

        // -------------------------------------------------------------------------
        // Single bound constraint tests
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 > :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.greaterThanOrEqual(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 >= :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.lessThan(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 < :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.lessThanOrEqual(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());
            assertThat(ProtoUtils.fromProtoBlock(allocator, res2.getPartitions()).getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 <= :v0)"));
        }
    }

    @Test
    public void doGetSplitsScan()
            throws Exception
    {
        GetTableLayoutResponse layoutResponse = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(ImmutableMap.of())))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());

        GetSplitsRequest req = GetSplitsRequest.newBuilder()
            .setIdentity(FederatedIdentity.newBuilder()
                .setAccount(TEST_IDENTITY.getAccount())
                .setArn(TEST_IDENTITY.getArn())
                .build())
            .setCatalogName(TEST_CATALOG_NAME)
            .setQueryId(TEST_QUERY_ID)
            .setTableName(
                com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                .setSchemaName(TEST_TABLE_NAME.getSchemaName())
                .setTableName(TEST_TABLE_NAME.getTableName())
                .build())
            .setPartitions(layoutResponse.getPartitions())
            .build();

        logger.info("doGetSplits: req[{}]", req);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);
        assertThat(response.getType(), equalTo("GetSplitsResponse"));
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplitsList().size());

        assertThat(response.hasContinuationToken(), is(false));

        com.amazonaws.athena.connector.lambda.proto.domain.Split split = Iterables.getOnlyElement(response.getSplitsList());
        assertThat(split.getPropertiesMap().get(SEGMENT_ID_PROPERTY), equalTo("0"));

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
        GetTableLayoutResponse layoutResponse = handler.doGetTableLayout(allocator, GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(TEST_TABLE_NAME))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
                .build());


        GetSplitsRequest req = GetSplitsRequest.newBuilder()
        .setIdentity(FederatedIdentity.newBuilder()
            .setAccount(TEST_IDENTITY.getAccount())
            .setArn(TEST_IDENTITY.getArn())
            .build())
        .setCatalogName(TEST_CATALOG_NAME)
        .setQueryId(TEST_QUERY_ID)
        .setTableName(
            com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
            .setSchemaName(TEST_TABLE_NAME.getSchemaName())
            .setTableName(TEST_TABLE_NAME.getTableName())
            .build())
        .setPartitions(layoutResponse.getPartitions())
        .addAllPartitionCols(List.of("col_0"))
        .build();

        logger.info("doGetSplits: req[{}]", req);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);
        assertThat(response.getType(), equalTo("GetSplitsResponse"));

        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplitsList().size());

        assertThat(continuationToken, equalTo(String.valueOf(MAX_SPLITS_PER_REQUEST - 1)));
        assertThat(response.getSplitsList().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplitsList().stream().map(split -> split.getPropertiesMap().get("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));

        GetSplitsRequest nextReq = GetSplitsRequest.newBuilder()
        .setIdentity(FederatedIdentity.newBuilder()
            .setAccount(TEST_IDENTITY.getAccount())
            .setArn(TEST_IDENTITY.getArn())
            .build())
        .setCatalogName(TEST_CATALOG_NAME)
        .setQueryId(TEST_QUERY_ID)
        .setTableName(
            com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
            .setSchemaName(TEST_TABLE_NAME.getSchemaName())
            .setTableName(TEST_TABLE_NAME.getTableName())
            .build())
        .setPartitions(layoutResponse.getPartitions())
        .addAllPartitionCols(List.of("col_0"))
        .setContinuationToken(continuationToken)
        .build();

        response = handler.doGetSplits(allocator, nextReq);

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplitsList().size());

        assertThat(response.hasContinuationToken(), equalTo(false));
        assertThat(response.getSplitsList().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplitsList().stream().map(split -> split.getPropertiesMap().get("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));
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
        GetTableRequest getTableRequest = GetTableRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .setTableName(ProtoUtils.toTableName(tableName))
            .build();
        GetTableResponse getTableResponse = handler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = ProtoUtils.fromProtoSchema(allocator, getTableResponse.getSchema()).getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(TEST_TABLE));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,Col3=datetime3"));

        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setCatalogName(TEST_CATALOG_NAME)
            .setQueryId(TEST_QUERY_ID)
            .setTableName(ProtoUtils.toTableName(tableName))
            .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(ImmutableMap.of())))
            .setSchema(getTableResponse.getSchema())
            .build();

                

        GetTableLayoutResponse getTableLayoutResponse = handler.doGetTableLayout(allocator, getTableLayoutRequest);
        logger.info("validateSourceTableNamePropagation: GetTableLayoutResponse[{}]", getTableLayoutResponse);
        assertThat(ProtoUtils.fromProtoBlock(allocator, getTableLayoutResponse.getPartitions()).getSchema().getCustomMetadata().get(TABLE_METADATA), equalTo(TEST_TABLE));
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
        GetTableRequest getTableRequest = GetTableRequest.newBuilder()
            .setIdentity(PROTO_TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG_NAME)
            .setTableName(ProtoUtils.toTableName(tableName))
            .build();
        GetTableResponse getTableResponse = handler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = ProtoUtils.fromProtoSchema(allocator, getTableResponse.getSchema()).getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(TEST_TABLE));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,col3=datetime3"));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());
        constraintsMap.put("col2",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());

        GetTableLayoutRequest getTableLayoutRequest = GetTableLayoutRequest.newBuilder()
                .setIdentity(PROTO_TEST_IDENTITY)
                .setCatalogName(TEST_CATALOG_NAME)
                .setQueryId(TEST_QUERY_ID)
                .setTableName(ProtoUtils.toTableName(tableName))
                .setConstraints(ProtoUtils.toProtoConstraints(new Constraints(constraintsMap)))
                .setSchema(getTableResponse.getSchema())
                .build();


        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, getTableLayoutRequest);
        Block partitionsBlock = ProtoUtils.fromProtoBlock(allocator, res.getPartitions());

        logger.info("doGetTableLayoutScanWithTypeOverride schema - {}", partitionsBlock.getSchema());
        logger.info("doGetTableLayoutScanWithTypeOverride partitions - {}", partitionsBlock);

        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(partitionsBlock.getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(partitionsBlock.getRowCount(), equalTo(1));

        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col3 = :v0 OR attribute_not_exists(#col3) OR #col3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col3", "col3", "#col2", "col2");
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(true), ":v1", ItemUtils.toAttributeValue(null));
        assertThat(partitionsBlock.getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
    }
}
