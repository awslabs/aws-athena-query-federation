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
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
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
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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

    private static final String bucket = "test-bucket";
    private static final String prefix = "test-prefix";
    private static final String requestId = "1234";
    private static final String splitId = "5678";

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
    public void read_EncryptedBlock_Succeeds()
    {
        BlockCrypto blockCrypto = new AesGcmBlockCrypto(allocator);
        byte[] spilledBytes = blockCrypto.encrypt(encryptionKey, expected);

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
    public void readBytes_EncryptedBytes_Succeeds()
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

    @Test
    public void read_NullSpillLocation_ThrowsNullPointerException()
    {
        assertThrows(NullPointerException.class, () -> {
            blockReader.read(null, encryptionKey, schema);
        });
    }

    @Test
    public void read_S3Failure_ThrowsS3Exception()
    {
        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenThrow(S3Exception.builder()
                        .message("Failed to read from S3")
                        .build());

        S3Exception exception = assertThrows(S3Exception.class, () -> {
            blockReader.read(spillLocation, encryptionKey, schema);
        });

        assertEquals("Failed to read from S3", exception.getMessage());
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void read_CorruptedEncryptedData_ThrowsAthenaConnectorException()
    {
        // Provide corrupted data (e.g., random bytes)
        byte[] corruptedBytes = new byte[]{1, 2, 3, 4, 5};

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream(corruptedBytes)));

        assertThrows(AthenaConnectorException.class, () -> {
            blockReader.read(spillLocation, encryptionKey, schema);
        });

        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void read_MismatchedEncryptionKey_ThrowsAthenaConnectorException()
    {
        BlockCrypto blockCrypto = new AesGcmBlockCrypto(allocator);
        byte[] spilledBytes = blockCrypto.encrypt(encryptionKey, expected);

        // Create a different encryption key
        EncryptionKeyFactory keyFactory = new LocalKeyFactory();
        EncryptionKey wrongKey = keyFactory.create();

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream(spilledBytes)));

        assertThrows(AthenaConnectorException.class, () -> {
            blockReader.read(spillLocation, wrongKey, schema);
        });

        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void read_NullSchema_ThrowsNullPointerException()
    {
        BlockCrypto blockCrypto = new AesGcmBlockCrypto(allocator);
        byte[] spilledBytes = blockCrypto.encrypt(encryptionKey, expected);

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream(spilledBytes)));

        assertThrows(NullPointerException.class, () -> {
            blockReader.read(spillLocation, encryptionKey, null);
        });
    }

    @Test
    public void read_UnencryptedBlockWithNullEncryptionKey_Succeeds()
    {
        encryptionKey = null;
        BlockCrypto blockCrypto = new NoOpBlockCrypto(allocator);
        byte[] spilledBytes = blockCrypto.encrypt(null, expected);

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream(spilledBytes)));

        Block actual = blockReader.read(spillLocation, null, schema);

        assertEquals(expected, actual);
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }
}