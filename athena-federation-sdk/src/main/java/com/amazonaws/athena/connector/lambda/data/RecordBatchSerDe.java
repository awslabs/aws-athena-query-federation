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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

/**
 * used to serialize and deserialize ArrowRecordBatch.
 *
 * @deprecated {@link com.amazonaws.athena.connector.lambda.serde.v3.ArrowRecordBatchSerDeV3} should be used instead
 */
@Deprecated
public class RecordBatchSerDe
{
    private final BlockAllocator allocator;

    public RecordBatchSerDe(BlockAllocator allocator)
    {
        this.allocator = allocator;
    }

    /**
     * Serialized the provided ArrowRecordBatch to the provided OutputStream and closes the batch once
     * it is fully written to the OutputStream.
     *
     * @param batch The ArrowRecordBatch to serialize.
     * @param out The OutputStream to write to.
     * @throws IOException
     */
    public void serialize(ArrowRecordBatch batch, OutputStream out)
            throws IOException
    {
        try {
            IpcOption option = new IpcOption(true, MetadataVersion.V4);
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch, option);
        }
        finally {
            batch.close();
        }
    }

    /**
     * Attempts to deserialize the provided byte[] into an ArrowRecordBatch.
     *
     * @param in The byte[] that is expected to contain a serialized ArrowRecordBatch.
     * @return The resulting ArrowRecordBatch if the byte[] contains a valid ArrowRecordBatch.
     * @throws IOException
     */
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
