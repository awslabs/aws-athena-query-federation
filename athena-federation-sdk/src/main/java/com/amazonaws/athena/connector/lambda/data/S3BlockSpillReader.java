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

import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class S3BlockSpillReader
{
    private static final Logger logger = LoggerFactory.getLogger(S3BlockSpillReader.class);

    private final S3Client amazonS3;
    private final BlockAllocator allocator;

    public S3BlockSpillReader(S3Client amazonS3, BlockAllocator allocator)
    {
        this.amazonS3 = requireNonNull(amazonS3, "amazonS3 was null");
        this.allocator = requireNonNull(allocator, "allocator was null");
    }

    /**
     * Reads a spilled block.
     *
     * @param spillLocation The location to read the spilled Block from.
     * @param key The encryption key to use when reading the spilled Block.
     * @param schema The Schema to use when deserializing the spilled Block.
     * @return The Block stored at the spill location.
     */
    public Block read(S3SpillLocation spillLocation, EncryptionKey key, Schema schema)
    {
        ResponseInputStream<GetObjectResponse> responseStream = null;
        try {
            logger.debug("read: Started reading block from S3");
            responseStream = amazonS3.getObject(GetObjectRequest.builder()
                    .bucket(spillLocation.getBucket())
                    .key(spillLocation.getKey())
                    .build());
            logger.debug("read: Completed reading block from S3");
            BlockCrypto blockCrypto = (key != null) ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
            Block block = blockCrypto.decrypt(key, ByteStreams.toByteArray(responseStream), schema);
            logger.debug("read: Completed decrypting block of size.");
            return block;
        }
        catch (IOException ex) {
            throw new AthenaConnectorException(ex, ex.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
        finally {
            if (responseStream != null) {
                try {
                    responseStream.close();
                }
                catch (IOException ex) {
                    logger.warn("read: Exception while closing S3 response stream", ex);
                }
            }
        }
    }

    /**
     * Reads spilled data as a byte[].
     *
     * @param spillLocation The location to read the spilled Block from.
     * @param key The encryption key to use when reading the spilled Block.
     * @return The Block stored at the spill location.
     */
    public byte[] read(S3SpillLocation spillLocation, EncryptionKey key)
    {
        ResponseInputStream<GetObjectResponse> responseStream = null;
        try {
            logger.debug("read: Started reading block from S3");
            responseStream = amazonS3.getObject(GetObjectRequest.builder()
                    .bucket(spillLocation.getBucket())
                    .key(spillLocation.getKey())
                    .build());
            logger.debug("read: Completed reading block from S3");
            BlockCrypto blockCrypto = (key != null) ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
            return blockCrypto.decrypt(key, ByteStreams.toByteArray(responseStream));
        }
        catch (IOException ex) {
            throw new AthenaConnectorException(ex, ex.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
        finally {
            if (responseStream != null) {
                try {
                    responseStream.close();
                }
                catch (IOException ex) {
                    logger.warn("read: Exception while closing S3 response stream", ex);
                }
            }
        }
    }
}
