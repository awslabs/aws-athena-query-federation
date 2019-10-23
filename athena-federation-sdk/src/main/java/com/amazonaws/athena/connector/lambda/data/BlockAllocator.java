package com.amazonaws.athena.connector.lambda.data;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

public interface BlockAllocator
        extends AutoCloseable
{
    Block createBlock(Schema schema);

    ArrowBuf createBuffer(int size);

    ArrowRecordBatch registerBatch(BatchGenerator generator);

    long getUsage();

    void close();

    boolean isClosed();

    interface BatchGenerator
    {
        ArrowRecordBatch generate(BufferAllocator allocator)
                throws Exception;
    }
}
