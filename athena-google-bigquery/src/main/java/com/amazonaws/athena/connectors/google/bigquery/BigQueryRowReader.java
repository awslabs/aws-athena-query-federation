/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

class BigQueryRowReader implements AutoCloseable
{
    // Decoder object will be reused to avoid re-allocation and too much garbage collection.
    private final VectorSchemaRoot root;
    private final VectorLoader loader;
    BufferAllocator allocator;

    public BigQueryRowReader(ArrowSchema arrowSchema, BufferAllocator allocator) throws IOException
    {
        this.allocator = allocator;
        Schema schema =
                MessageSerializer.deserializeSchema(
                        new ReadChannel(
                                new ByteArrayReadableSeekableByteChannel(
                                        arrowSchema.getSerializedSchema().toByteArray())));
        requireNonNull(schema);
        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : schema.getFields()) {
            vectors.add(field.createVector(allocator));
        }
        root = new VectorSchemaRoot(vectors);
        loader = new VectorLoader(root);
    }

    /**
     * Sample method for processing Arrow data which only validates decoding.
     *
     * @param batch object returned from the ReadRowsResponse.
     */
    public VectorSchemaRoot processRows(ArrowRecordBatch batch) throws IOException
    {
        org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch =
                MessageSerializer.deserializeRecordBatch(
                        new ReadChannel(
                                new ByteArrayReadableSeekableByteChannel(
                                        batch.getSerializedRecordBatch().toByteArray())),
                        allocator);

        loader.load(deserializedBatch);
        // Release buffers from batch (they are still held in the vectors in root).
        deserializedBatch.close();
        // Release buffers from vectors in root.
        return root;
    }

    @Override
    public void close()
    {
        root.close();
    }
}
