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
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_ID_PROPERTY;
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
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
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

        RecordResponse recordResponse = handler.doReadRecords(allocator, request);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(1000, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("testReadScanSplit: exit");
    }

    @Test
    public void testReadQuerySplit()
            throws Exception
    {
        logger.info("testReadQuerySplit: enter");
        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                .add(HASH_KEY_NAME_METADATA, "col_0")
                .add("col_0", Jackson.toJsonString(ItemUtils.toAttributeValue("test_str_0")))
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

        RecordResponse recordResponse = handler.doReadRecords(allocator, request);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(1, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("testReadQuerySplit: exit");
    }
}