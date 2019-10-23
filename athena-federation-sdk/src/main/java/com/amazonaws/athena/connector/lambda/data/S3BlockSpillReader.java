package com.amazonaws.athena.connector.lambda.data;

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
