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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * used to serialize and deserialize ArrowRecordBatch.
 *
 * @deprecated {@link com.amazonaws.athena.connector.lambda.serde.v3.ArrowRecordBatchSerDeV3} should be used instead
 */
@Deprecated
public final class ArrowRecordBatchSerDe
{
    private ArrowRecordBatchSerDe(){}

    static final class Serializer extends BaseSerializer<ArrowRecordBatch>
    {
        Serializer()
        {
            super(ArrowRecordBatch.class);
        }

        @Override
        public void doSerialize(ArrowRecordBatch arrowRecordBatch, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            try {
                IpcOption option = new IpcOption(true, MetadataVersion.V4);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), arrowRecordBatch, option);
                jgen.writeBinary(out.toByteArray());
            }
            finally {
                arrowRecordBatch.close();
            }
        }
    }

    static final class Deserializer extends BaseDeserializer<ArrowRecordBatch>
    {
        private final BlockAllocator blockAllocator;

        Deserializer(BlockAllocator allocator)
        {
            super(ArrowRecordBatch.class);
            this.blockAllocator = requireNonNull(allocator, "allocator is null");
        }

        @Override
        public ArrowRecordBatch doDeserialize(JsonParser jparser, DeserializationContext ctxt)
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
}
