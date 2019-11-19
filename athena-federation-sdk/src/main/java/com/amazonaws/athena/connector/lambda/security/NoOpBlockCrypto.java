package com.amazonaws.athena.connector.lambda.security;

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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.RecordBatchSerDe;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Implementation of BlockCrypto does a No-OP (nothing) for encrypting and decrypting blocks. This is helpful when you
 * want to disable encryption or do testing without having to handle disabled encryption as a special case in code.
 *
 * @see BlockCrypto
 */
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
