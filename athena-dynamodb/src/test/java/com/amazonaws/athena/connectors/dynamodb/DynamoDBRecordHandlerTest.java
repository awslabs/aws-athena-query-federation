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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

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
import static org.mockito.Mockito.mock;

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

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
        handler = new DynamoDBRecordHandler(ddbClient, mock(AmazonS3.class), mock(AWSSecretsManager.class), "source_type");
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testReadScanSplit()
            throws Exception
    {
        logger.info("testReadScanSplit: enter");
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
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(992, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("testReadScanSplit: exit");
    }

    @Test
    public void testReadQuerySplit()
            throws Exception
    {
        logger.info("testReadQuerySplit: enter");
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

        logger.info("testReadQuerySplit: exit");
    }

    @Test
    public void testZeroRowQuery()
            throws Exception
    {
        logger.info("testZeroRowQuery: enter");
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

        logger.info("testZeroRowQuery: exit");
    }
}
