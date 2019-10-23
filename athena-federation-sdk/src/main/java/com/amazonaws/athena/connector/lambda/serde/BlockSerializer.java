package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.RecordBatchSerDe;
import com.amazonaws.athena.connector.lambda.data.SchemaSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BlockSerializer
        extends StdSerializer<Block>
{
    protected static final String ALLOCATOR_ID_FIELD_NAME = "aId";
    protected static final String SCHEMA_FIELD_NAME = "schema";
    protected static final String BATCH_FIELD_NAME = "records";
    private final SchemaSerDe schemaSerDe;
    private final RecordBatchSerDe recordBatchSerDe;

    public BlockSerializer()
    {
        super(Block.class);
        this.schemaSerDe = new SchemaSerDe();
        this.recordBatchSerDe = new RecordBatchSerDe(null);
    }

    @Override
    public void serialize(Block block, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField(ALLOCATOR_ID_FIELD_NAME, block.getAllocatorId());

        ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
        schemaSerDe.serialize(block.getSchema(), schemaOut);
        jsonGenerator.writeBinaryField(SCHEMA_FIELD_NAME, schemaOut.toByteArray());
        schemaOut.close();

        ByteArrayOutputStream batchOut = new ByteArrayOutputStream();
        if (block != null && block.getRowCount() > 0) {
            recordBatchSerDe.serialize(block.getRecordBatch(), batchOut);
        }
        jsonGenerator.writeBinaryField(BATCH_FIELD_NAME, batchOut.toByteArray());

        jsonGenerator.writeEndObject();
    }
}
