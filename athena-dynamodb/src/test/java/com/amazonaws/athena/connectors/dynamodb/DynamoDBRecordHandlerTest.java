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
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.LocalDateTime;
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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.COLUMN_NAME_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.SOURCE_TABLE_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_NAMES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_VALUES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.NON_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.TABLE_METADATA;

import static com.amazonaws.services.dynamodbv2.document.ItemUtils.toAttributeValue;
import static com.amazonaws.util.json.Jackson.toJsonString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBRecordHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRecordHandlerTest.class);

    private static final SpillLocation SPILL_LOCATION = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(UUID.randomUUID().toString())
            .withIsDirectory(true)
            .build();

    private BlockAllocator allocator;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private DynamoDBRecordHandler handler;
    private DynamoDBMetadataHandler metadataHandler;

    @Mock
    private AWSGlue glueClient;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup()
    {
        logger.info("{}: enter", testName.getMethodName());

        allocator = new BlockAllocatorImpl();
        handler = new DynamoDBRecordHandler(ddbClient, mock(AmazonS3.class), mock(AWSSecretsManager.class), mock(AmazonAthena.class), "source_type");
        metadataHandler = new DynamoDBMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", ddbClient, glueClient);
    }

    @After
    public void tearDown()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void testReadScanSplit()
            throws Exception
    {
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(1000, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testReadScanSplitFiltered()
            throws Exception
    {
        Map<String, String> expressionNames = ImmutableMap.of("#col_6", "col_6");
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", toAttributeValue(0), ":v1", toAttributeValue(1));
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .add(NON_KEY_FILTER_METADATA, "NOT #col_6 IN (:v0,:v1)")
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, toJsonString(expressionValues))
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadScanSplitFiltered: rows[{}]", response.getRecordCount());

        assertEquals(992, response.getRecords().getRowCount());
        logger.info("testReadScanSplitFiltered: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testReadQuerySplit()
            throws Exception
    {
        Map<String, String> expressionNames = ImmutableMap.of("#col_1", "col_1");
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", toAttributeValue(1));
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(HASH_KEY_NAME_METADATA, "col_0")
                .add("col_0", toJsonString(toAttributeValue("test_str_0")))
                .add(RANGE_KEY_FILTER_METADATA, "#col_1 >= :v0")
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, toJsonString(expressionValues))
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadQuerySplit: rows[{}]", response.getRecordCount());

        assertEquals(2, response.getRecords().getRowCount());
        logger.info("testReadQuerySplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testZeroRowQuery()
            throws Exception
    {
        Map<String, String> expressionNames = ImmutableMap.of("#col_1", "col_1");
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", toAttributeValue(1));
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(HASH_KEY_NAME_METADATA, "col_0")
                .add("col_0", toJsonString(toAttributeValue("test_str_999999")))
                .add(RANGE_KEY_FILTER_METADATA, "#col_1 >= :v0")
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, toJsonString(expressionValues))
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testZeroRowQuery: rows[{}]", response.getRecordCount());

        assertEquals(0, response.getRecords().getRowCount());
    }

    @Test
    public void testDateTimeSupportFromGlueTable() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col0").withType("string"));
        columns.add(new Column().withName("col1").withType("timestamp"));
        columns.add(new Column().withName("col2").withType("timestamp"));
        columns.add(new Column().withName("col3").withType("date"));
        columns.add(new Column().withName("col4").withType("date"));

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE3,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1 , col2=Col2 ,col3=Col3, col4=Col4",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=yyyyMMdd'S'HHmmss,col3=dd/MM/yyyy ");
        Table table = new Table()
                .withParameters(param)
                .withPartitionKeys()
                .withStorageDescriptor(new StorageDescriptor().withColumns(columns));
        GetTableResult mockResult = new GetTableResult().withTable(table);
        when(glueClient.getTable(any())).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE3);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName);
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testDateTimeSupportFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testDateTimeSupportFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema3 = getTableResponse.getSchema();

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE3)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_3_NAME,
                schema3,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

        assertEquals(1, response.getRecords().getRowCount());
        assertEquals(new LocalDateTime(2020, 2, 27, 9, 12, 27),
                response.getRecords().getFieldReader("Col1").readLocalDateTime());
        assertEquals(new LocalDateTime(2020, 2, 27, 9, 12, 27),
                response.getRecords().getFieldReader("Col2").readLocalDateTime());
        assertEquals(LocalDate.of(2020, 02, 27),
                LocalDate.ofEpochDay(response.getRecords().getFieldReader("Col3").readInteger()));

        assertEquals(LocalDate.of(2020, 02, 27),
                LocalDate.ofEpochDay(response.getRecords().getFieldReader("Col4").readInteger()));
    }

    @Test
    public void testStructWithNullFromGlueTable() throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col0").withType("string"));
        columns.add(new Column().withName("col1").withType("struct"));
        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE4,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1,col2=Col2");
        Table table = new Table()
                .withParameters(param)
                .withPartitionKeys()
                .withStorageDescriptor(new StorageDescriptor().withColumns(columns));
        GetTableResult mockResult = new GetTableResult().withTable(table);
        when(glueClient.getTable(any())).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE4);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName);
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testStructWithNullFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testStructWithNullFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema4 = getTableResponse.getSchema();
        for (Field f : schema4.getFields()) {
            if (f.getName().equals("Col2")) {
                assertEquals(2, f.getChildren().size());
                assertTrue(f.getType() instanceof ArrowType.Struct);
            }
        }

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE4)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_4_NAME,
                schema4,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testStructWithNullFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema4, result.getSchema());
        assertEquals("[Col0 : hashVal], [Col1 : {[field1 : someField1]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testStructWithNullFromDdbTable() throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new EntityNotFoundException(""));

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE4);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName);
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testStructWithNullFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testStructWithNullFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema4 = getTableResponse.getSchema();
        for (Field f : schema4.getFields()) {
            if (f.getName().equals("Col2")) {
                assertEquals(1, f.getChildren().size());
                assertTrue(f.getType() instanceof ArrowType.Struct);
            }
        }
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE4)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_4_NAME,
                schema4,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testStructWithNullFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema4, result.getSchema());
        assertEquals("[Col0 : hashVal], [Col1 : {[field1 : someField1]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }
}
