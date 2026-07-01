package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3BlockSpillerTest
{
    private static final Logger logger = LoggerFactory.getLogger(S3BlockSpillerTest.class);

    private String bucket = "MyBucket";
    private String prefix = "blocks/spill";
    private String requestId = "requestId";
    private String splitId = "splitId";

    @Mock
    private S3Client mockS3;

    private S3BlockSpiller blockWriter;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private Block expected;
    private BlockAllocatorImpl allocator;
    private SpillConfig spillConfig;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();

        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .build();

        spillConfig = SpillConfig.newBuilder().withEncryptionKey(keyFactory.create())
                .withRequestId(requestId)
                .withSpillLocation(S3SpillLocation.newBuilder()
                        .withBucket(bucket)
                        .withPrefix(prefix)
                        .withQueryId(requestId)
                        .withSplitId(splitId)
                        .withIsDirectory(true)
                        .build())
                .withRequestId(requestId)
                .build();

        blockWriter = new S3BlockSpiller(mockS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());

        expected = allocator.createBlock(schema);
        BlockUtils.setValue(expected.getFieldVector("col1"), 1, 100);
        BlockUtils.setValue(expected.getFieldVector("col2"), 1, "VarChar");
        BlockUtils.setValue(expected.getFieldVector("col1"), 1, 101);
        BlockUtils.setValue(expected.getFieldVector("col2"), 1, "VarChar1");
        expected.setRowCount(2);
    }

    @After
    public void tearDown()
            throws Exception
    {
        expected.close();
        allocator.close();
        blockWriter.close();
    }

    @Test
    public void spillTest()
            throws IOException
    {
        logger.info("spillTest: enter");

        logger.info("spillTest: starting write test");

        final ByteHolder byteHolder = new ByteHolder();

        ArgumentCaptor<PutObjectRequest> requestArgument = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyArgument = ArgumentCaptor.forClass(RequestBody.class);

        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        PutObjectResponse response = PutObjectResponse.builder().build();
                        InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                        return response;
                    }
                });

        SpillLocation blockLocation = blockWriter.write(expected);

        if (blockLocation instanceof S3SpillLocation) {
            assertEquals(bucket, ((S3SpillLocation) blockLocation).getBucket());
            assertEquals(prefix + "/" + requestId + "/" + splitId + ".0", ((S3SpillLocation) blockLocation).getKey());
        }
        verify(mockS3, times(1)).putObject(requestArgument.capture(), bodyArgument.capture());
        assertEquals(requestArgument.getValue().bucket(), bucket);
        assertEquals(requestArgument.getValue().key(), prefix + "/" + requestId + "/" + splitId + ".0");

        SpillLocation blockLocation2 = blockWriter.write(expected);

        if (blockLocation2 instanceof S3SpillLocation) {
            assertEquals(bucket, ((S3SpillLocation) blockLocation2).getBucket());
            assertEquals(prefix + "/" + requestId + "/" + splitId + ".1", ((S3SpillLocation) blockLocation2).getKey());
        }

        verify(mockS3, times(2)).putObject(requestArgument.capture(), bodyArgument.capture());
        assertEquals(requestArgument.getValue().bucket(), bucket);
        assertEquals(requestArgument.getValue().key(), prefix + "/" + requestId + "/" + splitId + ".1");

        verifyNoMoreInteractions(mockS3);
        reset(mockS3);

        logger.info("spillTest: Starting read test.");

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                    }
                });

        Block block = blockWriter.read((S3SpillLocation) blockLocation2, spillConfig.getEncryptionKey(), expected.getSchema());

        assertEquals(expected, block);

        verify(mockS3, times(1))
                .getObject(any(GetObjectRequest.class));

        verifyNoMoreInteractions(mockS3);

        logger.info("spillTest: exit");
    }

    @Test
    public void spillTest_WithRequestHeaders_SetsHeadersInOverrideConfiguration()
            throws IOException
    {
        // Setup config with spill_put_request_headers for SSE-KMS
        String spillHeaders = "{\"x-amz-server-side-encryption\":\"aws:kms\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"arn:aws:kms:us-east-1:123456789012:key/test-key-id\"}";
        java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of("spill_put_request_headers", spillHeaders);

        PutObjectRequest capturedRequest = executeSpillWithConfig(configOptions);

        // Verify headers are in overrideConfiguration (request headers), NOT in metadata
        assertTrue("Request should have overrideConfiguration", capturedRequest.overrideConfiguration().isPresent());
        AwsRequestOverrideConfiguration overrideConfig = capturedRequest.overrideConfiguration().get();
        
        // Verify SSE-KMS headers are present in request headers
        assertTrue("x-amz-server-side-encryption header should be present", 
                overrideConfig.headers().containsKey("x-amz-server-side-encryption"));
        assertEquals("aws:kms", overrideConfig.headers().get("x-amz-server-side-encryption").get(0));
        
        assertTrue("x-amz-server-side-encryption-aws-kms-key-id header should be present", 
                overrideConfig.headers().containsKey("x-amz-server-side-encryption-aws-kms-key-id"));
        assertEquals("arn:aws:kms:us-east-1:123456789012:key/test-key-id", 
                overrideConfig.headers().get("x-amz-server-side-encryption-aws-kms-key-id").get(0));

        // Verify SSE-KMS request headers are NOT in user-metadata (they belong in overrideConfiguration)
        assertFalse("SSE-KMS header should not appear in user-metadata",
                capturedRequest.metadata().containsKey("x-amz-server-side-encryption"));
        assertFalse("SSE-KMS key-id header should not appear in user-metadata",
                capturedRequest.metadata().containsKey("x-amz-server-side-encryption-aws-kms-key-id"));
    }

    @Test
    public void spillTest_WithoutRequestHeaders_DoesNotSetOverrideConfiguration()
            throws IOException
    {
        // Setup config without spill_put_request_headers
        java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of();

        PutObjectRequest capturedRequest = executeSpillWithConfig(configOptions);

        // Verify no overrideConfiguration when headers are not configured
        assertFalse("Request should not have overrideConfiguration when no headers configured", 
                capturedRequest.overrideConfiguration().isPresent());

        // Spill stats metadata is always present even when no request headers are configured.
        assertTrue("Spill stats metadata should always be set",
                capturedRequest.metadata().containsKey(S3BlockSpiller.SPILL_METADATA_ROW_COUNT));
    }

    @Test
    public void spillTest_WithInvalidJsonHeaders_HandlesGracefully()
            throws IOException
    {
        // Setup config with invalid JSON in spill_put_request_headers
        String invalidJson = "{\"x-amz-server-side-encryption\":\"aws:kms\"invalid}";
        java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of("spill_put_request_headers", invalidJson);

        PutObjectRequest capturedRequest = executeSpillWithConfig(configOptions);

        // Verify no overrideConfiguration when JSON is invalid (should be handled gracefully)
        assertFalse("Request should not have overrideConfiguration when JSON is invalid", 
                capturedRequest.overrideConfiguration().isPresent());
    }

    @Test
    public void spillTest_WithMultipleHeaders_SetsAllHeadersInOverrideConfiguration()
            throws IOException
    {
        // Setup config with multiple headers
        String spillHeaders = "{\"x-amz-server-side-encryption\":\"aws:kms\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"arn:aws:kms:us-east-1:123456789012:key/test-key-id\",\"x-amz-storage-class\":\"STANDARD_IA\"}";
        java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of("spill_put_request_headers", spillHeaders);

        PutObjectRequest capturedRequest = executeSpillWithConfig(configOptions);

        // Verify all headers are present in overrideConfiguration
        assertTrue("Request should have overrideConfiguration", capturedRequest.overrideConfiguration().isPresent());
        AwsRequestOverrideConfiguration overrideConfig = capturedRequest.overrideConfiguration().get();
        
        assertEquals("aws:kms", overrideConfig.headers().get("x-amz-server-side-encryption").get(0));
        assertEquals("arn:aws:kms:us-east-1:123456789012:key/test-key-id", 
                overrideConfig.headers().get("x-amz-server-side-encryption-aws-kms-key-id").get(0));
        assertEquals("STANDARD_IA", overrideConfig.headers().get("x-amz-storage-class").get(0));
        
        assertEquals("Should have 3 headers", 3, overrideConfig.headers().size());

        // Verify SSE-KMS / storage-class request headers are NOT in user-metadata
        assertFalse(capturedRequest.metadata().containsKey("x-amz-server-side-encryption"));
        assertFalse(capturedRequest.metadata().containsKey("x-amz-server-side-encryption-aws-kms-key-id"));
        assertFalse(capturedRequest.metadata().containsKey("x-amz-storage-class"));
    }

    @Test
    public void spillTest_WritesArrowStatsMetadata()
            throws IOException
    {
        // expected has 2 columns (col1 Int32, col2 Utf8) and rowCount=2 (set in @Before).
        PutObjectRequest capturedRequest = executeSpillWithConfig(com.google.common.collect.ImmutableMap.of());

        java.util.Map<String, String> meta = capturedRequest.metadata();

        // Row count: matches expected.getRowCount()
        assertEquals("x-arrow-row-count must reflect Block.getRowCount()",
                Integer.toString(expected.getRowCount()),
                meta.get(S3BlockSpiller.SPILL_METADATA_ROW_COUNT));

        // Byte size: matches Block.getSize() (uncompressed Arrow buffer size, not encrypted bytes).
        assertEquals("x-arrow-byte-size must reflect Block.getSize()",
                Long.toString(expected.getSize()),
                meta.get(S3BlockSpiller.SPILL_METADATA_BYTE_SIZE));

        // Column count: schema has 2 fields (col1, col2)
        assertEquals("x-arrow-num-columns must reflect Schema.getFields().size()",
                Integer.toString(expected.getSchema().getFields().size()),
                meta.get(S3BlockSpiller.SPILL_METADATA_NUM_COLUMNS));

        // No unexpected metadata keys leaked in (e.g. SSE-KMS request headers).
        assertEquals("Only the three x-arrow-* stats keys should be in user-metadata",
                3, meta.size());
    }

    /**
     * Helper method to create S3BlockSpiller with given config options
     */
    private S3BlockSpiller createBlockSpiller(java.util.Map<String, String> configOptions)
    {
        return new S3BlockSpiller(mockS3, spillConfig, allocator, expected.getSchema(), 
                ConstraintEvaluator.emptyEvaluator(), configOptions);
    }

    /**
     * Helper method to setup mock S3 putObject call
     */
    private void setupMockPutObject(ByteHolder byteHolder)
    {
        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        PutObjectResponse response = PutObjectResponse.builder().build();
                        InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                        return response;
                    }
                });
    }

    /**
     * Helper method to execute spill and capture PutObjectRequest
     */
    private PutObjectRequest executeSpillWithConfig(java.util.Map<String, String> configOptions)
            throws IOException
    {
        S3BlockSpiller blockWriter = createBlockSpiller(configOptions);
        ByteHolder byteHolder = new ByteHolder();
        ArgumentCaptor<PutObjectRequest> requestArgument = ArgumentCaptor.forClass(PutObjectRequest.class);

        setupMockPutObject(byteHolder);
        blockWriter.write(expected);

        verify(mockS3, times(1)).putObject(requestArgument.capture(), any(RequestBody.class));
        PutObjectRequest capturedRequest = requestArgument.getValue();

        blockWriter.close();
        return capturedRequest;
    }

    @Test(expected = RuntimeException.class)
    public void getSpillLocationsThrowsOnAsyncS3Error()
            throws IOException
    {
        // Create a spiller with async spill threads enabled
        SpillConfig asyncSpillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(keyFactory.create())
                .withRequestId(requestId)
                .withSpillLocation(S3SpillLocation.newBuilder()
                        .withBucket(bucket)
                        .withPrefix(prefix)
                        .withQueryId(requestId)
                        .withSplitId(splitId)
                        .withIsDirectory(true)
                        .build())
                .withRequestId(requestId)
                .withMaxBlockBytes(100) // small so it spills immediately
                .withMaxInlineBlockBytes(0) // force spill
                .withNumSpillThreads(1)
                .build();

        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .build();

        S3BlockSpiller asyncSpiller = new S3BlockSpiller(mockS3, asyncSpillConfig, allocator, schema,
                ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());

        // Mock S3 putObject to throw a permission error (simulating AccessDenied)
        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new RuntimeException("Access Denied"));

        try {
            // Write enough data to trigger a spill
            asyncSpiller.writeRows((Block block, int rowNum) -> {
                BlockUtils.setValue(block.getFieldVector("col1"), rowNum, 100);
                return 1;
            });

            // This should throw because the async S3 write failed with permission error
            asyncSpiller.getSpillLocations();
        }
        finally {
            asyncSpiller.close();
        }
    }

    @Test(expected = RuntimeException.class)
    public void spilledThrowsOnAsyncS3Error()
            throws IOException
    {
        // Create a spiller with async spill threads enabled
        SpillConfig asyncSpillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(keyFactory.create())
                .withRequestId(requestId)
                .withSpillLocation(S3SpillLocation.newBuilder()
                        .withBucket(bucket)
                        .withPrefix(prefix)
                        .withQueryId(requestId)
                        .withSplitId(splitId)
                        .withIsDirectory(true)
                        .build())
                .withRequestId(requestId)
                .withMaxBlockBytes(100) // small so it spills immediately
                .withMaxInlineBlockBytes(0) // force spill
                .withNumSpillThreads(1)
                .build();

        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .build();

        S3BlockSpiller asyncSpiller = new S3BlockSpiller(mockS3, asyncSpillConfig, allocator, schema,
                ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());

        // Mock S3 putObject to throw a permission error
        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new RuntimeException("Access Denied"));

        try {
            // Write multiple rows to exceed maxBlockBytes and trigger a spill
            for (int i = 0; i < 20; i++) {
                asyncSpiller.writeRows((Block block, int rowNum) -> {
                    BlockUtils.setValue(block.getFieldVector("col1"), rowNum, 100);
                    BlockUtils.setValue(block.getFieldVector("col2"), rowNum, "SomeDataToIncreaseBlockSize");
                    return 1;
                });
            }

            // Close the spiller which awaits async thread termination
            asyncSpiller.close();

            // After close, the async thread has completed - spilled() should throw
            asyncSpiller.spilled();
        }
        finally {
            // already closed above, but safe to call again
        }
    }

    @Test
    public void getSpillLocationsSucceedsWhenS3WriteSucceeds()
            throws IOException
    {
        // Create a spiller with async spill threads enabled
        SpillConfig asyncSpillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(keyFactory.create())
                .withRequestId(requestId)
                .withSpillLocation(S3SpillLocation.newBuilder()
                        .withBucket(bucket)
                        .withPrefix(prefix)
                        .withQueryId(requestId)
                        .withSplitId(splitId)
                        .withIsDirectory(true)
                        .build())
                .withRequestId(requestId)
                .withMaxBlockBytes(100) // small so it spills immediately
                .withMaxInlineBlockBytes(0) // force spill
                .withNumSpillThreads(1)
                .build();

        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .build();

        S3BlockSpiller asyncSpiller = new S3BlockSpiller(mockS3, asyncSpillConfig, allocator, schema,
                ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());

        // Mock S3 putObject to succeed
        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        try {
            // Write enough data to trigger a spill
            asyncSpiller.writeRows((Block block, int rowNum) -> {
                BlockUtils.setValue(block.getFieldVector("col1"), rowNum, 100);
                return 1;
            });

            // Should succeed without exception
            java.util.List<SpillLocation> locations = asyncSpiller.getSpillLocations();
            assertNotNull(locations);
            assertFalse(locations.isEmpty());
        }
        finally {
            asyncSpiller.close();
        }
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
