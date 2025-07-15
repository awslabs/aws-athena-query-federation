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
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.qpt.DDBQueryPassthrough;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
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
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
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
import static com.amazonaws.util.json.Jackson.toJsonString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBRecordHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRecordHandlerTest.class);
    private static final String SCHEMA_FUNCTION_NAME = "system.query";
    private static final String SELECT_COL_0_COL_1_QUERY = "SELECT col_0, col_1 FROM " + TEST_TABLE + " WHERE col_0 = 'test_str_0'";
    private static final String COL_0 = "col_0";
    private static final String COL_1 = "col_1";
    private static final String TEST_STR_0 = "test_str_0";
    private static final long SPILL_SIZE = 100_000_000_000L;
    private static final int VALUE_1 = 1;
    private static final int VALUE_2 = 2;
    private static final int VALUE_3 = 3;

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
    public void doReadRecords_withScanSplit_readsAllRecords()
            throws Exception
    {
        Split split = createScanSplit(TEST_TABLE);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(1000, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withScanSplitAndLimitFromQueryPlan_readsLimitedRecords()
            throws Exception
    {
        //SELECT * FROM test_table limit 100
        QueryPlan queryPlan = getQueryPlan("GqwDEqkDCuACGt0CCgIKABLSAjrPAgoOEgwKCgoLDA0ODxAREhMSxgEKwwEKAgoAEq4BCgVjb2x" +
                "fMAoFY29sXzEKBWNvbF8yCgVjb2xfMwoFY29sXzQKBWNvbF81CgVjb2xfNgoFY29sXzcKBWNvbF84CgVjb2xfORJmCgiyAQUI6AcYA" +
                "QoIsgEFCOgHGAEKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIB" +
                "BQjoBxgBCgiyAQUI6AcYARgBOgwKClRFU1RfVEFCTEUaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIA" +
                "yIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGA" +
                "AgZBIFY29sXzASBWNvbF8xEgVjb2xfMhIFY29sXzMSBWNvbF80EgVjb2xfNRIFY29sXzYSBWNvbF83EgVjb2xfOBIFY29sXzk=");

        Split split = createScanSplit(TEST_TABLE);

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                constraintsWithQueryPlan(queryPlan),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(100, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withScanSplitLimitAndOrderBy_readsAllRecords()
            throws Exception
    {
        //SELECT * FROM test_table order by col_0 limit 100
        QueryPlan queryPlan = getQueryPlan("GsQDEsEDCvgCGvUCCgIKABLqAirnAgoCCgAS0gI6zwIKDhIMCgoKCwwNDg8QERITEsYBCsMBCgI" +
                "KABKuAQoFY29sXzAKBWNvbF8xCgVjb2xfMgoFY29sXzMKBWNvbF80CgVjb2xfNQoFY29sXzYKBWNvbF83CgVjb2xfOAoFY29sXzkSZ" +
                "goIsgEFCOgHGAEKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQj" +
                "oBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEYAToMCgpURVNUX1RBQkxFGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKE" +
                "ggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBI" +
                "CCAkiABoMCggSBgoCEgAiABACGAAgZBIFY29sXzASBWNvbF8xEgVjb2xfMhIFY29sXzMSBWNvbF80EgVjb2xfNRIFY29sXzYSBWNvbF8" +
                "3EgVjb2xfOBIFY29sXzk=");

        Split split = createScanSplit(TEST_TABLE);

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                TEST_TABLE_NAME,
                schema,
                split,
                constraintsWithQueryPlan(queryPlan),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(1000, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withScanSplitAndLimit_readsLimitedRecords()
        throws Exception
    {
        Split split = createScanSplit(TEST_TABLE);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                constraintsWithLimit(5));

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(5, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withScanSplitAndLimitLargerThanRecords_readsAllRecords()
            throws Exception
    {
        Split split = createScanSplit(TEST_TABLE);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                constraintsWithLimit(10_000));

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(1000, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withScanSplitAndFilter_filtersRecords()
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

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadScanSplitFiltered: rows[{}]", response.getRecordCount());

        assertEquals(992, response.getRecords().getRowCount());
        logger.info("testReadScanSplitFiltered: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withQuerySplit_readsMatchingRecords()
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

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadQuerySplit: rows[{}]", response.getRecordCount());

        assertEquals(2, response.getRecords().getRowCount());
        logger.info("testReadQuerySplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withQuerySplitAndLimit_readsLimitedRecords()
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

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                constraintsWithLimit(1));

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testReadQuerySplit: rows[{}]", response.getRecordCount());

        assertEquals(1, response.getRecords().getRowCount());
        logger.info("testReadQuerySplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withQuerySplitNoMatches_readsZeroRecords()
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

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testZeroRowQuery: rows[{}]", response.getRecordCount());

        assertEquals(0, response.getRecords().getRowCount());
    }

    @Test
    public void doReadRecords_withDateTimeSupportFromGlueTable_formatsDates() throws Exception
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

        Split split = createScanSplit(TEST_TABLE3);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_3_NAME,
                schema3,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);

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
    public void doReadRecords_withStructWithNullFromGlueTable_preservesNullInStruct() throws Exception
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

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_4_NAME,
                schema4,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testStructWithNullFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema4, result.getSchema());
        assertEquals("[Col0 : hashVal], [Col1 : {[field1 : someField1],[field2 : null]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withStructWithNullFromDdbTable_removesNullFromStruct() throws Exception
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
        Split split = createScanSplit(TEST_TABLE4);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_4_NAME,
                schema4,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testStructWithNullFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema4, result.getSchema());
        assertEquals("[Col0 : hashVal], [Col1 : {[field1 : someField1]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withMapWithSchemaFromGlueTable_preservesMapStructure() throws Exception
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

        Split split = createScanSplit(TEST_TABLE5);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_5_NAME,
                schema5,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testMapWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema5, result.getSchema());
        assertEquals("[Col0 : hashVal], [outermap : {[key : list],[value : {list1,list2}]}], [structcol : {[key : structKey],[value : {[key1 : str1],[key2 : str2]}]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withStructWithSchemaFromGlueTable_preservesStructStructure() throws Exception
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

        Split split = createScanSplit(TEST_TABLE6);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_6_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testStructWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema, result.getSchema());

        assertEquals("[Col0 : hashVal], [outermap : {[list : {list1,list2}]}], [structcol : {[structKey : {[key1 : str1],[key2 : str2]}]}]", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_withListWithSchemaFromGlueTable_preservesListStructure() throws Exception
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

        Split split = createScanSplit(TEST_TABLE7);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_7_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
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
    public void doReadRecords_withNumMapWithSchemaFromGlueTable_preservesNumMapStructure() throws Exception
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

        Split split = createScanSplit(TEST_TABLE8);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_8_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
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
    public void doReadRecords_withNumStructWithSchemaFromGlueTable_preservesNumStructStructure() throws Exception
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

        Split split = createScanSplit(TEST_TABLE8);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_8_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        logger.info("testNumStructWithSchemaFromGlueTable: {}", BlockUtils.rowToString(response.getRecords(), 0));
        Block result = response.getRecords();
        assertEquals(1, result.getRowCount());
        assertEquals(schema, result.getSchema());
        FieldReader numMapReader = result.getFieldReader("nummap");
        assertEquals(numMapReader.getField().getChildren().size(), 2);
        assertEquals(numMapReader.reader("key1").readInteger(), (Integer) 1);
        assertEquals(numMapReader.reader("key2").readInteger(), (Integer) 2);
    }

    @Test
    public void doReadRecords_withNonExistentTable_throwsAthenaConnectorException() throws Exception
    {
        Split split = createScanSplit("nonexistent_table");

        ReadRecordsRequest request = createReadRecordsRequest(
                new TableName(DEFAULT_SCHEMA, "nonexistent_table"),
                schema,
                split,
                defaultConstraints());

        AthenaConnectorException exception = assertThrows(AthenaConnectorException.class, () -> {
            handler.doReadRecords(allocator, request);
        });
        assertEquals(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString(),
                exception.getErrorDetails().errorCode());
    }

    @Test
    public void doReadRecords_withQueryPassthroughPartiQLQuery_readsRecordsMatchingQuery()
            throws Exception
    {
        // Prepare the constraints with query passthrough enabled
        Map<String, String> qptArguments = ImmutableMap.of(
                "schemaFunctionName", SCHEMA_FUNCTION_NAME,
                DDBQueryPassthrough.QUERY, SELECT_COL_0_COL_1_QUERY
        );

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                qptArguments,
                null
        );

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .applyProperties(qptArguments)
                .build();

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                constraints);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);

        logger.info("doReadRecords_withQueryPassthroughPartiQLQuery_returnsRecordsMatchingQuery: rows[{}]", response.getRecordCount());

        assertTrue("Should have at least one record from query passthrough", response.getRecords().getRowCount() >= 1);

        // Verify the data structure is correct - all returned records should have col_0 = 'test_str_0'
        Block records = response.getRecords();
        logger.info("doReadRecords_withQueryPassthroughPartiQLQuery_returnsRecordsMatchingQuery: Found {} records", records.getRowCount());

        // Verify the first record to confirm query passthrough is working
        String col0Value = records.getFieldReader(COL_0).readText().toString();
        logger.info("doReadRecords_withQueryPassthroughPartiQLQuery_returnsRecordsMatchingQuery: First record: col_0={}, col_1={}",
                col0Value, records.getFieldReader(COL_1).readBigDecimal());
        assertEquals("col_0 value should match expected test string", TEST_STR_0, col0Value);
        // col_1 should be a valid number
        assertNotNull("col_1 should not be null", records.getFieldReader(COL_1).readBigDecimal());
    }

    @Test
    public void doReadRecords_withInFilterOnRangeKey_queriesWithMultipleValues()
            throws Exception
    {
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(
                ":v1", DDBTypeUtils.toAttributeValue(VALUE_1),
                ":v2", DDBTypeUtils.toAttributeValue(VALUE_2),
                ":v3", DDBTypeUtils.toAttributeValue(VALUE_3));
        Split split = createQuerySplitWithRangeFilter("#col_1 IN (:v1,:v2,:v3)", expressionValues);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        assertNotNull("Response should not be null", response);
    }

    @Test
    public void doReadRecords_withEqualityFilterOnRangeKey_queriesWithSingleValue()
            throws Exception
    {
        Map<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(VALUE_1));
        Split split = createQuerySplitWithRangeFilter("#col_1 = :v0", expressionValues);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        assertNotNull("Response should not be null", response);
    }

    @Test
    public void doReadRecords_withQuerySplitWithoutRangeKeyFilter_handlesQueryWithHashKeyOnly()
            throws Exception
    {
        Split split = createQuerySplitWithoutRangeFilter();

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);
        assertNotNull("Response should not be null", response);
    }

    @Test
    public void doReadRecords_withInvalidSplitMetadata_throwsIllegalArgumentOrAthenaConnectorException()
    {
        try {
            Split invalidSplit = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                    .add(TABLE_METADATA, "invalid_table")
                    .add(SEGMENT_ID_PROPERTY, "invalid")
                    .add(SEGMENT_COUNT_METADATA, "1")
                    .build();

            ReadRecordsRequest request = createReadRecordsRequest(
                    new TableName(DEFAULT_SCHEMA, "invalid_table"),
                    schema,
                    invalidSplit,
                    defaultConstraints());

            handler.doReadRecords(allocator, request);
            fail("Expected exception was not thrown for invalid split metadata");
        }
        catch (Exception ex) {
            assertTrue("Exception should be IllegalArgumentException or AthenaConnectorException for invalid split metadata",
                    ex instanceof IllegalArgumentException || ex instanceof AthenaConnectorException);
        }
    }

    @Test
    public void doReadRecords_withEmptyTable_readsZeroRecords()
            throws Exception
    {
        Split split = createScanSplit(TEST_TABLE2);

        ReadRecordsRequest request = createReadRecordsRequest(
                TEST_TABLE_2_NAME,
                schema,
                split,
                defaultConstraints());

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = assertAndCastToReadRecordsResponse(rawResponse);

        assertEquals("Empty table should return zero records", 0, response.getRecords().getRowCount());
    }

    private long getPackedDateTimeWithZone(String s)
    {
        ZonedDateTime zdt = ZonedDateTime.parse(s);
        return DateTimeFormatterUtil.timestampMilliTzHolderFromObject(zdt, null).value;
    }
    private Split createQuerySplitWithRangeFilter(String rangeFilter, Map<String, AttributeValue> expressionValues)
    {
        Map<String, String> expressionNames = ImmutableMap.of("#col_1", COL_1);
        return Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(HASH_KEY_NAME_METADATA, COL_0)
                .add(COL_0, DDBTypeUtils.attributeToJson(DDBTypeUtils.toAttributeValue(TEST_STR_0), COL_0))
                .add(RANGE_KEY_FILTER_METADATA, rangeFilter)
                .add(EXPRESSION_NAMES_METADATA, toJsonString(expressionNames))
                .add(EXPRESSION_VALUES_METADATA, EnhancedDocument.fromAttributeValueMap(expressionValues).toJson())
                .build();
    }

    private Split createQuerySplitWithoutRangeFilter()
    {
        return Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, TEST_TABLE)
                .add(HASH_KEY_NAME_METADATA, COL_0)
                .add(COL_0, DDBTypeUtils.attributeToJson(DDBTypeUtils.toAttributeValue(TEST_STR_0), COL_0))
                .build();
    }

    private ReadRecordsRequest createReadRecordsRequest(TableName tableName, Schema schema, Split split, Constraints constraints)
    {
        return new ReadRecordsRequest(
                TEST_IDENTITY,
                TEST_CATALOG_NAME,
                TEST_QUERY_ID,
                tableName,
                schema,
                split,
                constraints,
                SPILL_SIZE,
                SPILL_SIZE);
    }

    private Split createScanSplit(String tableName)
    {
        return Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(TABLE_METADATA, tableName)
                .add(SEGMENT_ID_PROPERTY, "0")
                .add(SEGMENT_COUNT_METADATA, "1")
                .build();
    }

    private ReadRecordsResponse assertAndCastToReadRecordsResponse(RecordResponse rawResponse)
    {
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        return (ReadRecordsResponse) rawResponse;
    }

    private static Constraints defaultConstraints()
    {
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }

    private static Constraints constraintsWithLimit(long limit)
    {
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), limit, Collections.emptyMap(), null);
    }

    private static Constraints constraintsWithQueryPlan(QueryPlan queryPlan)
    {
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), queryPlan);
    }
}
