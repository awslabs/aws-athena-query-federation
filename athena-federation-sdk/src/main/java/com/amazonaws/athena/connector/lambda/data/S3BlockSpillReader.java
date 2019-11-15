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
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class S3BlockSpillReader
{
    private static final Logger logger = LoggerFactory.getLogger(S3BlockSpillReader.class);

    private final AmazonS3 amazonS3;
    private final BlockAllocator allocator;

    public S3BlockSpillReader(AmazonS3 amazonS3, BlockAllocator allocator)
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
        S3Object fullObject = null;
        try {
            logger.debug("read: Started reading block from S3");
            fullObject = amazonS3.getObject(spillLocation.getBucket(), spillLocation.getKey());
            logger.debug("read: Completed reading block from S3");
            BlockCrypto blockCrypto = (key != null) ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
            Block block = blockCrypto.decrypt(key, ByteStreams.toByteArray(fullObject.getObjectContent()), schema);
            logger.debug("read: Completed decrypting block of size.");
            return block;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            if (fullObject != null) {
                try {
                    fullObject.close();
                }
                catch (IOException ex) {
                    logger.warn("read: Exception while closing S3 object", ex);
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
        S3Object fullObject = null;
        try {
            logger.debug("read: Started reading block from S3");
            fullObject = amazonS3.getObject(spillLocation.getBucket(), spillLocation.getKey());
            logger.debug("read: Completed reading block from S3");
            BlockCrypto blockCrypto = (key != null) ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
            return blockCrypto.decrypt(key, ByteStreams.toByteArray(fullObject.getObjectContent()));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            if (fullObject != null) {
                try {
                    fullObject.close();
                }
                catch (IOException ex) {
                    logger.warn("read: Exception while closing S3 object", ex);
                }
            }
        }
    }
}
