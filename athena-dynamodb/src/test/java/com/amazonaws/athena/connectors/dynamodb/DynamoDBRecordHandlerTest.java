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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
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
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
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
import static org.mockito.ArgumentMatchers.any;
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
    private GlueClient glueClient;

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient athena;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup()
    {
        logger.info("{}: enter", testName.getMethodName());

        allocator = new BlockAllocatorImpl();
        handler = new DynamoDBRecordHandler(ddbClient, mock(S3Client.class), mock(SecretsManagerClient.class), mock(AthenaClient.class), "source_type", com.google.common.collect.ImmutableMap.of());
        metadataHandler = new DynamoDBMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", ddbClient, glueClient, com.google.common.collect.ImmutableMap.of());
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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(),null),
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
    public void testReadScanSplitWithLimit()
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
                new Constraints(ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), 5, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(5, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testReadScanSplitWithLimitLargerThanN()
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
                new Constraints(ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), 10_000, Collections.emptyMap(), null),
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
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(0), ":v1", DDBTypeUtils.toAttributeValue(1));
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .add(NON_KEY_FILTER_METADATA, "NOT #col_6 IN (:v0,:v1)")
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, EnhancedDocument.fromAttributeValueMap(expressionValues).toJson())
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(1));
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(HASH_KEY_NAME_METADATA, "col_0")
                .add("col_0", DDBTypeUtils.attributeToJson(DDBTypeUtils.toAttributeValue("test_str_0"), "col_0"))
                .add(RANGE_KEY_FILTER_METADATA, "#col_1 >= :v0")
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, EnhancedDocument.fromAttributeValueMap(expressionValues).toJson())
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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
    public void testReadQuerySplitWithLimit()
            throws Exception
    {
        Map<String, String> expressionNames = ImmutableMap.of("#col_1", "col_1");
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(1));
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(HASH_KEY_NAME_METADATA, "col_0")
                .add("col_0", DDBTypeUtils.attributeToJson(DDBTypeUtils.toAttributeValue("test_str_0"), "col_0"))
                .add(RANGE_KEY_FILTER_METADATA, "#col_1 >= :v0")
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, EnhancedDocument.fromAttributeValueMap(expressionValues).toJson())
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), 1, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadQuerySplit: rows[{}]", response.getRecordCount());

        assertEquals(1, response.getRecords().getRowCount());
        logger.info("testReadQuerySplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testZeroRowQuery()
            throws Exception
    {
        Map<String, String> expressionNames = ImmutableMap.of("#col_1", "col_1");
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(1));
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(HASH_KEY_NAME_METADATA, "col_0")
                .add("col_0", DDBTypeUtils.attributeToJson(DDBTypeUtils.toAttributeValue("test_str_999999"), "col_0"))
                .add(RANGE_KEY_FILTER_METADATA, "#col_1 >= :v0")
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, EnhancedDocument.fromAttributeValueMap(expressionValues).toJson())
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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
        columns.add(Column.builder().name("col0").type("string").build());
        columns.add(Column.builder().name("col1").type("timestamp").build());
        columns.add(Column.builder().name("col2").type("timestamp").build());
        columns.add(Column.builder().name("col3").type("date").build());
        columns.add(Column.builder().name("col4").type("date").build());
        columns.add(Column.builder().name("col5").type("timestamptz").build());
        columns.add(Column.builder().name("col6").type("timestamptz").build());
        columns.add(Column.builder().name("col7").type("timestamptz").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE3,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1 , col2=Col2 ,col3=Col3, col4=Col4,col5=Col5,col6=Col6,col7=Col7",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=yyyyMMdd'S'HHmmss,col3=dd/MM/yyyy ");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(tableResponse);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE3);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

        LocalDate expectedDate = LocalDate.of(2020, 02, 27);
        LocalDateTime expectedDateTime = LocalDateTime.of(2020, 2, 27, 9, 12, 27);
        assertEquals(1, response.getRecords().getRowCount());
        assertEquals(expectedDateTime, response.getRecords().getFieldReader("Col1").readLocalDateTime());
        assertEquals(expectedDateTime, response.getRecords().getFieldReader("Col2").readLocalDateTime());
        assertEquals(expectedDate, LocalDate.ofEpochDay(response.getRecords().getFieldReader("Col3").readInteger()));
        assertEquals(expectedDate, LocalDate.ofEpochDay(response.getRecords().getFieldReader("Col4").readInteger()));
        assertEquals(getPackedDateTimeWithZone("2015-12-21T17:42:34-05:00"), response.getRecords().getFieldReader("Col5").readLong().longValue());
        assertEquals(getPackedDateTimeWithZone("2015-12-21T17:42:34Z"), response.getRecords().getFieldReader("Col6").readLong().longValue());
        assertEquals(getPackedDateTimeWithZone("2015-12-21T17:42:34Z"), response.getRecords().getFieldReader("Col7").readLong().longValue());
    }

    @Test
    public void testStructWithNullFromGlueTable() throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col0").type("string").build());
        columns.add(Column.builder().name("col1").type("struct<field1:string, field2:string>").build());
        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE4,
                COLUMN_NAME_MAPPING_PROPERTY, "col0=Col0,col1=Col1,col2=Col2");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(tableResponse);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE4);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testStructWithNullFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema4, result.getSchema());
        assertEquals("[Col0 : hashVal], [Col1 : {[field1 : someField1],[field2 : null]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testStructWithNullFromDdbTable() throws Exception
    {
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(EntityNotFoundException.builder().message("").build());

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE4);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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
    public void testMapWithSchemaFromGlueTable() throws Exception
    {
        // Disable this test if MAP_DISABLED
        if (GlueFieldLexer.MAP_DISABLED) {
            return;
        }

        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col0").type("string").build());
        columns.add(Column.builder().name("outermap").type("MAP<STRING,array<STRING,STRING>>").build());
        columns.add(Column.builder().name("structcol").type("MAP<STRING,struct<key1:STRING,key2:STRING>>").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE5,
                COLUMN_NAME_MAPPING_PROPERTY, "col0=Col0,col1=Col1,col2=Col2");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(tableResponse);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE5);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testMapWithSchemaFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testMapWithSchemaFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema5 = getTableResponse.getSchema();

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE5)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_5_NAME,
                schema5,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testMapWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema5, result.getSchema());
        assertEquals("[Col0 : hashVal], [outermap : {[key : list],[value : {list1,list2}]}], [structcol : {[key : structKey],[value : {[key1 : str1],[key2 : str2]}]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testStructWithSchemaFromGlueTable() throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col0").type("string").build());
        columns.add(Column.builder().name("outermap").type("struct<list:array<string>>").build());
        columns.add(Column.builder().name("structcol").type("struct<structKey:struct<key1:STRING,key2:STRING>>").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE6,
                COLUMN_NAME_MAPPING_PROPERTY, "col0=Col0,col1=Col1,col2=Col2");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(tableResponse);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE6);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testStructWithSchemaFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testStructWithSchemaFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema = getTableResponse.getSchema();

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE6)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_6_NAME,
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testStructWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema, result.getSchema());

        assertEquals("[Col0 : hashVal], [outermap : {[list : {list1,list2}]}], [structcol : {[structKey : {[key1 : str1],[key2 : str2]}]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void testListWithSchemaFromGlueTable() throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col0").type("string").build());
        columns.add(Column.builder().name("stringList").type("ARRAY <STRING>").build());
        columns.add(Column.builder().name("intList").type("ARRAY <int>").build());
        columns.add(Column.builder().name("listStructCol").type("array<struct<key1:STRING,key2:STRING>>").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE7,
                COLUMN_NAME_MAPPING_PROPERTY, "col0=Col0,col1=Col1,col2=Col2");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(tableResponse);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE7);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testListWithSchemaFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testListWithSchemaFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema = getTableResponse.getSchema();

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE7)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_7_NAME,
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testListWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema, result.getSchema());

        FieldReader stringListValReader = result.getFieldReader("stringList").reader();
        stringListValReader.setPosition(0);
        assertEquals(stringListValReader.readText().toString(), "list1");
        stringListValReader.setPosition(1);
        assertEquals(stringListValReader.readText().toString(), "list2");

        UnionListReader intListValReader = (UnionListReader) result.getFieldReader("intList");
        JsonStringArrayList<Integer> intListValList = (JsonStringArrayList<Integer>) intListValReader.readObject();
        assertEquals(intListValList.get(1), (Integer) 1);
        assertEquals(intListValList.get(2), (Integer) 2);

        JsonStringArrayList listStructReader = (JsonStringArrayList) result.getFieldReader("listStructCol").readObject();
        JsonStringHashMap item1 = (JsonStringHashMap) listStructReader.get(0);
        assertTrue(item1.containsKey("key1"));
        assertTrue(item1.containsKey("key2"));
        assertEquals(item1.get("key1").toString(), "str1");
        assertEquals(item1.get("key2").toString(), "str2");

        JsonStringHashMap item2 = (JsonStringHashMap) listStructReader.get(1);
        assertTrue(item2.containsKey("key1"));
        assertTrue(item2.containsKey("key2"));
        assertEquals(item2.get("key1").toString(), "str11");
        assertEquals(item2.get("key2").toString(), "str22");
    }

    @Test
    public void testNumMapWithSchemaFromGlueTable() throws Exception
    {
        // Disable this test if MAP_DISABLED
        if (GlueFieldLexer.MAP_DISABLED) {
            return;
        }

        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col0").type("string").build());
        columns.add(Column.builder().name("nummap").type("map<String,int>").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE8,
                COLUMN_NAME_MAPPING_PROPERTY, "col0=Col0,col1=Col1,col2=Col2");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse mockResult = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE8);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testNumMapWithSchemaFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testNumMapWithSchemaFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema = getTableResponse.getSchema();

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE8)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_8_NAME,
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testNumMapWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema, result.getSchema());
        FieldReader numMapReader = result.getFieldReader("nummap");
        assertEquals(numMapReader.getField().getChildren().size(), 1);
        FieldReader key = numMapReader.reader().reader("key");
        FieldReader value = numMapReader.reader().reader("value");
        key.setPosition(0);
        value.setPosition(0);
        assertEquals(key.readText().toString(), "key1");
        assertEquals(value.readInteger(), (Integer) 1);

        key.setPosition(1);
        value.setPosition(1);
        assertEquals(key.readText().toString(), "key2");
        assertEquals(value.readInteger(), (Integer) 2);
    }

    @Test
    public void testNumStructWithSchemaFromGlueTable() throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col0").type("string").build());
        columns.add(Column.builder().name("nummap").type("struct<key1:int,key2:int>").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE8,
                COLUMN_NAME_MAPPING_PROPERTY, "col0=Col0,col1=Col1,col2=Col2");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse mockResult = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, TEST_TABLE8);
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
        GetTableResponse getTableResponse = metadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("testNumStructWithSchemaFromGlueTable: GetTableResponse[{}]", getTableResponse);
        logger.info("testNumStructWithSchemaFromGlueTable: GetTableResponse Schema[{}]", getTableResponse.getSchema());

        Schema schema = getTableResponse.getSchema();

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE8)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_8_NAME,
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testNumStructWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema, result.getSchema());
        FieldReader numMapReader = result.getFieldReader("nummap");
        assertEquals(numMapReader.getField().getChildren().size(), 2);
        assertEquals(numMapReader.reader("key1").readInteger(), (Integer) 1);
        assertEquals(numMapReader.reader("key2").readInteger(), (Integer) 2);
    }

    private long getPackedDateTimeWithZone(String s)
    {
        ZonedDateTime zdt = ZonedDateTime.parse(s);
        return DateTimeFormatterUtil.timestampMilliTzHolderFromObject(zdt, null).value;
    }
}
