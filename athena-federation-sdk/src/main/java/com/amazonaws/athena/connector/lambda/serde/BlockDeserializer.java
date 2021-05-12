package com.amazonaws.athena.connector.lambda.serde;

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
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorRegistry;
import com.amazonaws.athena.connector.lambda.data.RecordBatchSerDe;
import com.amazonaws.athena.connector.lambda.data.SchemaSerDe;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Uses either an explicit BlockAllocator or a BlockAllocatorRegistry to handle memory pooling associated with
 * deserializing blocks. Blocks are serialized as an Apache Arrow Schema + Apache Arrow Record Batch. If you
 * need to serialize multiple Blocks of the same Schema we do not recommend using this class since it will
 * result in the same schema being serialized multiple times. It may be more efficient for you to serialize
 * the Schema once, separately, and then each Record Batch.
 * <p>
 * This class also attempts to make use of the allocator_id field of the Block so that it will reuse the
 * same allocator id when deserializing. This can be helpful when attempting to limit the number of copy
 * operations that are required to move a Block around. It also allows you to put tighter control around
 * which parts of your query execution get which memory pool / limit.
 *
 * @deprecated {@link com.amazonaws.athena.connector.lambda.serde.v3.BlockSerDeV3} should be used instead
 */
@Deprecated
public class BlockDeserializer
        extends StdDeserializer<Block>
{
    private final BlockAllocatorRegistry allocatorRegistry;
    private final BlockAllocator allocator;
    private final SchemaSerDe schemaSerDe;
    private final RecordBatchSerDe recordBatchSerDe;

    public BlockDeserializer(BlockAllocator allocator)
    {
        super(Block.class);
        this.schemaSerDe = new SchemaSerDe();
        this.recordBatchSerDe = new RecordBatchSerDe(allocator);
        this.allocator = allocator;
        this.allocatorRegistry = null;
    }

    public BlockDeserializer(BlockAllocatorRegistry allocatorRegistry)
    {
        super(Block.class);
        this.schemaSerDe = new SchemaSerDe();
        this.allocator = null;
        this.recordBatchSerDe = null;
        this.allocatorRegistry = allocatorRegistry;
    }

    @Override
    public Block deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException
    {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        String allocatorId = node.get(BlockSerializer.ALLOCATOR_ID_FIELD_NAME).asText();
        byte[] schemaBytes = node.get(BlockSerializer.SCHEMA_FIELD_NAME).binaryValue();
        byte[] batchBytes = node.get(BlockSerializer.BATCH_FIELD_NAME).binaryValue();

        Schema schema = schemaSerDe.deserialize(new ByteArrayInputStream(schemaBytes));
        Block block = getOrCreateAllocator(allocatorId).createBlock(schema);

        if (batchBytes.length > 0) {
            ArrowRecordBatch batch = deserializeBatch(allocatorId, batchBytes);
            block.loadRecordBatch(batch);
        }
        return block;
    }

    private ArrowRecordBatch deserializeBatch(String allocatorId, byte[] batchBytes)
            throws IOException
    {
        return getOrCreateBatchSerde(allocatorId).deserialize(batchBytes);
    }

    private RecordBatchSerDe getOrCreateBatchSerde(String allocatorId)
    {
        if (recordBatchSerDe != null) {
            return recordBatchSerDe;
        }

        return new RecordBatchSerDe(getOrCreateAllocator(allocatorId));
    }

    private BlockAllocator getOrCreateAllocator(String allocatorId)
    {
        if (allocator != null) {
            return allocator;
        }

        return allocatorRegistry.getOrCreateAllocator(allocatorId);
    }
}
