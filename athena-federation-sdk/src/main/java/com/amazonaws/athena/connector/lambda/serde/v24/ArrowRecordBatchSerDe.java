/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.serde.BaseSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class ArrowRecordBatchSerDe
        extends BaseSerDe<ArrowRecordBatch>
{
    private final BlockAllocator blockAllocator;

    public ArrowRecordBatchSerDe(BlockAllocator blockAllocator)
    {
        this.blockAllocator = requireNonNull(blockAllocator, "blockAllocator is null");
    }

    @Override
    public void serialize(JsonGenerator jgen, ArrowRecordBatch arrowRecordBatch)
            throws IOException
    {
        // ArrowRecordBatch is serialized inline and not wrapped
        doSerialize(jgen, arrowRecordBatch);
    }

    @Override
    public ArrowRecordBatch deserialize(JsonParser jparser)
            throws IOException
    {
        // ArrowRecordBatch should be deserialized inline and not unwrapped
        return doDeserialize(jparser);
    }

    @Override
    public void doSerialize(JsonGenerator jgen, ArrowRecordBatch arrowRecordBatch)
            throws IOException
    {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), arrowRecordBatch);
            jgen.writeBinary(out.toByteArray());
        }
        finally {
            arrowRecordBatch.close();
        }
    }

    @Override
    public ArrowRecordBatch doDeserialize(JsonParser jparser)
            throws IOException
    {
        if (jparser.nextToken() != JsonToken.VALUE_EMBEDDED_OBJECT) {
            throw new IllegalStateException("Expecting " + JsonToken.VALUE_STRING + " but found " + jparser.getCurrentLocation());
        }
        byte[] bytes = jparser.getBinaryValue();
        AtomicReference<ArrowRecordBatch> batch = new AtomicReference<>();
        try {
            return blockAllocator.registerBatch((BufferAllocator root) -> {
                batch.set((ArrowRecordBatch) MessageSerializer.deserializeMessageBatch(
                        new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))), root));
                return batch.get();
            });
        }
        catch (Exception ex) {
            if (batch.get() != null) {
                batch.get().close();
            }
            throw ex;
        }
    }
}
