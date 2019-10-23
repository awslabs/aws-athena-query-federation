package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
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
 * deserializing blocks.
 */
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
