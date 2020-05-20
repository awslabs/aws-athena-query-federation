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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorRegistry;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

final class BlockSerDe
{
    private static final String ALLOCATOR_ID_FIELD_NAME = "aId";
    private static final String SCHEMA_FIELD_NAME = "schema";
    private static final String BATCH_FIELD_NAME = "records";

    private BlockSerDe(){}

    static final class Serializer extends BaseSerializer<Block>
    {
        private SchemaSerDe.Serializer schemaSerializer;

        Serializer(SchemaSerDe.Serializer schemaSerializer)
        {
            super(Block.class);
            this.schemaSerializer = requireNonNull(schemaSerializer, "schemaSerializer is null");
        }

        @Override
        protected void doSerialize(Block block, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeStringField(ALLOCATOR_ID_FIELD_NAME, block.getAllocatorId());

            jgen.writeFieldName(SCHEMA_FIELD_NAME);
            schemaSerializer.serialize(block.getSchema(), jgen, provider);

            jgen.writeFieldName(BATCH_FIELD_NAME);
            if (block.getRowCount() > 0) {
                jgen.writeBinary(serializeRecordBatch(block.getRecordBatch()));
            }
            else {
                jgen.writeString("");
            }
        }

        private byte[] serializeRecordBatch(ArrowRecordBatch recordBatch)
                throws IOException
        {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), recordBatch);
                return out.toByteArray();
            }
            finally {
                recordBatch.close();
            }
        }
    }

    static final class Deserializer extends BaseDeserializer<Block>
    {
        private final BlockAllocator allocator;
        private final BlockAllocatorRegistry allocatorRegistry;
        private SchemaSerDe.Deserializer schemaDeserializer;

        Deserializer(BlockAllocator allocator, SchemaSerDe.Deserializer schemaDeserializer)
        {
            super(Block.class);
            this.schemaDeserializer = requireNonNull(schemaDeserializer, "schemaDeserializer is null");
            this.allocator = allocator;
            this.allocatorRegistry = null;
        }

        Deserializer(BlockAllocatorRegistry allocatorRegistry, SchemaSerDe.Deserializer schemaDeserializer)
        {
            super(Block.class);
            this.schemaDeserializer = requireNonNull(schemaDeserializer, "schemaDeserializer is null");
            this.allocator = null;
            this.allocatorRegistry = requireNonNull(allocatorRegistry, "allocatorRegistry is null");
        }

        @Override
        protected Block doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String allocatorId = getNextStringField(jparser, ALLOCATOR_ID_FIELD_NAME);

            assertFieldName(jparser, SCHEMA_FIELD_NAME);
            Schema schema = schemaDeserializer.deserialize(jparser, ctxt);

            byte[] batchBytes = getNextBinaryField(jparser, BATCH_FIELD_NAME);
            Block block = getOrCreateAllocator(allocatorId).createBlock(schema);
            if (batchBytes.length > 0) {
                ArrowRecordBatch batch = deserializeBatch(allocatorId, batchBytes);
                block.loadRecordBatch(batch);
            }

            return block;
        }

        private BlockAllocator getOrCreateAllocator(String allocatorId)
        {
            if (allocator != null) {
                return allocator;
            }
            else if (allocatorRegistry != null) {
                return allocatorRegistry.getOrCreateAllocator(allocatorId);
            }
            else {
                throw new IllegalStateException("allocator and allocatorRegistry are both null");
            }
        }

        private ArrowRecordBatch deserializeBatch(String allocatorId, byte[] batchBytes)
                throws IOException
        {
            return deserializeRecordBatch(getOrCreateAllocator(allocatorId), batchBytes);
        }

        private ArrowRecordBatch deserializeRecordBatch(BlockAllocator allocator, byte[] in)
        {
            AtomicReference<ArrowRecordBatch> batch = new AtomicReference<>();
            try {
                return allocator.registerBatch((BufferAllocator root) -> {
                    batch.set((ArrowRecordBatch) MessageSerializer.deserializeMessageBatch(
                            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(in))), root));
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
