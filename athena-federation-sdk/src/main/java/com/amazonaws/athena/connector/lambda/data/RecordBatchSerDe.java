package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

public class RecordBatchSerDe
{
    private final BlockAllocator allocator;

    public RecordBatchSerDe(BlockAllocator allocator)
    {
        this.allocator = allocator;
    }

    public void serialize(ArrowRecordBatch batch, OutputStream out)
            throws IOException
    {
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);
        }
        finally {
            batch.close();
        }
    }

    public ArrowRecordBatch deserialize(byte[] in)
            throws IOException
    {
        ArrowRecordBatch batch = null;
        try {
            return allocator.registerBatch((BufferAllocator root) ->
                    (ArrowRecordBatch) MessageSerializer.deserializeMessageBatch(
                            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(in))),
                            root)
            );
        }
        catch (Exception ex) {
            if (batch != null) {
                batch.close();
            }
            throw ex;
        }
    }
}
