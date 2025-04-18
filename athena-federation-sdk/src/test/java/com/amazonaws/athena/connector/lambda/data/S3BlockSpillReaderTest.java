/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.data;

import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class S3BlockSpillReaderTest
{
    private BlockAllocatorImpl allocator;
    private Schema schema;
    private EncryptionKey encryptionKey;
    private S3Client mockS3;
    private S3BlockSpillReader blockReader;
    private S3SpillLocation spillLocation;
    private Block expected;

    private final String bucket = "test-bucket";
    private final String prefix = "test-prefix";
    private final String requestId = "1234";
    private final String splitId = "5678";

    @Before
    public void setup()
            throws Exception
    {
        allocator = new BlockAllocatorImpl();
        mockS3 = mock(S3Client.class);
        blockReader = new S3BlockSpillReader(mockS3, allocator);

        schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .build();

        EncryptionKeyFactory keyFactory = new LocalKeyFactory();
        encryptionKey = keyFactory.create();

        spillLocation = S3SpillLocation.newBuilder()
                .withBucket(bucket)
                .withPrefix(prefix)
                .withQueryId(requestId)
                .withSplitId(splitId)
                .withIsDirectory(false)
                .build();

        expected = allocator.createBlock(schema);
        BlockUtils.setValue(expected.getFieldVector("col1"), 0, 100);
        BlockUtils.setValue(expected.getFieldVector("col2"), 0, "VarChar");
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
    }

    @Test
    public void testReadEncryptedBlock()
            throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BlockCrypto blockCrypto = (encryptionKey != null)
                ? new AesGcmBlockCrypto(allocator)
                : new NoOpBlockCrypto(allocator);

        byte[] serialized = blockCrypto.encrypt(encryptionKey, expected);
        out.write(serialized);

        byte[] spilledBytes = out.toByteArray();

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream(spilledBytes)));

        Block actual = blockReader.read(spillLocation, encryptionKey, schema);

        assertEquals(expected, actual);
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verifyNoMoreInteractions(mockS3);
    }

    @Test
    public void testReadEncryptedBytes()
    {
        BlockCrypto blockCrypto = new AesGcmBlockCrypto(allocator);
        byte[] encryptedBytes = blockCrypto.encrypt(encryptionKey, expected);

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream(encryptedBytes)));

        byte[] bytes = blockReader.read(spillLocation, encryptionKey);

        assertNotNull(bytes);
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verifyNoMoreInteractions(mockS3);
    }
}
