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
import com.amazonaws.athena.connector.lambda.data.RecordBatchSerDe;
import com.amazonaws.athena.connector.lambda.data.SchemaSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Uses either an explicit BlockAllocator or a BlockAllocatorRegistry to handle memory pooling associated with
 * serializing blocks. Blocks are serialized as an Apache Arrow Schema + Apache Arrow Record Batch. If you
 * need to serialize multiple Blocks of the same Schema we do not recommend using this class since it will
 * result in the same schema being serialized multiple times. It may be more efficient for you to serialize
 * the Schema once, separately, and then each Record Batch.
 *
 * This class also attempts to make use of the allocator_id field of the Block so that it will reuse the
 * same allocator id when deserializing. This can be helpful when attempting to limit the number of copy
 * operations that are required to move a Block around. It also allows you to put tighter control around
 * which parts of your query execution get which memory pool / limit.
 *
 * @deprecated {@link com.amazonaws.athena.connector.lambda.serde.v2.BlockSerDe} should be used instead
 */
@Deprecated
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
