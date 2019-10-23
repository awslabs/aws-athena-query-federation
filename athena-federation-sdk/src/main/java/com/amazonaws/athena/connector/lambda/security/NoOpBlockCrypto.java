package com.amazonaws.athena.connector.lambda.security;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.RecordBatchSerDe;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class NoOpBlockCrypto
        implements BlockCrypto
{
    private final RecordBatchSerDe serDe;
    private final BlockAllocator allocator;

    public NoOpBlockCrypto(BlockAllocator allocator)
    {
        this.serDe = new RecordBatchSerDe(allocator);
        this.allocator = allocator;
    }

    public byte[] encrypt(EncryptionKey key, Block block)
    {
        if (key != null) {
            throw new RuntimeException("Real key provided to NoOpBlockCrypto, likely indicates you wanted real crypto.");
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serDe.serialize(block.getRecordBatch(), out);
            return out.toByteArray();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Block decrypt(EncryptionKey key, byte[] bytes, Schema schema)
    {
        try {
            Block resultBlock = allocator.createBlock(schema);
            resultBlock.loadRecordBatch(serDe.deserialize(bytes));
            return resultBlock;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] decrypt(EncryptionKey key, byte[] bytes)
    {
        return bytes;
    }
}
