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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
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
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
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
    private AmazonS3 mockS3;

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

        blockWriter = new S3BlockSpiller(mockS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator());

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

        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        when(mockS3.putObject(anyObject()))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                        return mock(PutObjectResult.class);
                    }
                });

        SpillLocation blockLocation = blockWriter.write(expected);

        if (blockLocation instanceof S3SpillLocation) {
            assertEquals(bucket, ((S3SpillLocation) blockLocation).getBucket());
            assertEquals(prefix + "/" + requestId + "/" + splitId + ".0", ((S3SpillLocation) blockLocation).getKey());
        }
        verify(mockS3, times(1)).putObject(argument.capture());
        assertEquals(argument.getValue().getBucketName(), bucket);
        assertEquals(argument.getValue().getKey(), prefix + "/" + requestId + "/" + splitId + ".0");

        SpillLocation blockLocation2 = blockWriter.write(expected);

        if (blockLocation2 instanceof S3SpillLocation) {
            assertEquals(bucket, ((S3SpillLocation) blockLocation2).getBucket());
            assertEquals(prefix + "/" + requestId + "/" + splitId + ".1", ((S3SpillLocation) blockLocation2).getKey());
        }

        verify(mockS3, times(2)).putObject(argument.capture());
        assertEquals(argument.getValue().getBucketName(), bucket);
        assertEquals(argument.getValue().getKey(), prefix + "/" + requestId + "/" + splitId + ".1");

        verifyNoMoreInteractions(mockS3);
        reset(mockS3);

        logger.info("spillTest: Starting read test.");

        when(mockS3.getObject(eq(bucket), eq(prefix + "/" + requestId + "/" + splitId + ".1")))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        S3Object mockObject = mock(S3Object.class);
                        when(mockObject.getObjectContent()).thenReturn(new S3ObjectInputStream(new ByteArrayInputStream(byteHolder.getBytes()), null));
                        return mockObject;
                    }
                });

        Block block = blockWriter.read((S3SpillLocation) blockLocation2, spillConfig.getEncryptionKey(), expected.getSchema());

        assertEquals(expected, block);

        verify(mockS3, times(1))
                .getObject(eq(bucket), eq(prefix + "/" + requestId + "/" + splitId + ".1"));

        verifyNoMoreInteractions(mockS3);

        logger.info("spillTest: exit");
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
