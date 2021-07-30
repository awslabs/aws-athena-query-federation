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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Defines the interface that should be implemented by all reference counting Apache Arrow resource allocators
 * that are provided by this SDK. You should use a BlockAllocator over an Apache Arrow BufferAllocator if the lifecycle
 * of your Apache Arrow resources are not fully contained in narrow code path. In practice we've found that ensuring
 * proper lifecycle for Apache Arrow resources led us to change the structure of our code in ways that made it less
 * maintainable than if we had a mechanism to control the lifecyle of Apache Arrow resources that cross-cut our
 * request lifecycle.
 */
public interface BlockAllocator
        extends AutoCloseable
{
    /**
     * Creates an empty Apache Arrow Block with the provided Schema.
     *
     * @param schema The schema of the Apache Arrow Block.
     * @return THe resulting Block.
     * @note Once created the Block is also registered with this BlockAllocator such that closing this BlockAllocator
     * also closes this Block, freeing its Apache Arrow resources.
     */
    Block createBlock(Schema schema);

    /**
     * Creates an empty Apache Arrow Buffer of the requested size. This is useful when working with certain Apache Arrow
     * types directly.
     *
     * @param size The number of bytes to reserve for the requested buffer.
     * @return THe resulting Apache Arrow Buffer..
     * @note Once created the buffer is also registered with this BlockAllocator such that closing this BlockAllocator
     * also closes this buffer, freeing its Apache Arrow resources.
     */
    ArrowBuf createBuffer(int size);

    /**
     * Allows for a leak-free way to create Apache Arrow Batches. At first glance this method's signature may seem ackward
     * when compared to createBuffer(...) or createBlock(...) but ArrowRecordBatches are typically as part of serialization
     * and as such are prone to leakage when you serialize or deserialize and invalid Block. With this approach the
     * BlockAllocator is able to capture any exceptions from your BatchGenerator and perform nessesary clean up without
     * your code having to implement the boiler plate for handling those edge cases.
     *
     * @param generator The generator which is expected to create an ArrowRecordBatch.
     * @return THe resulting Apache Arrow Batch..
     * @note Once created the batch is also registered with this BlockAllocator such that closing this BlockAllocator
     * also closes this batch, freeing its Apache Arrow resources.
     */
    ArrowRecordBatch registerBatch(BatchGenerator generator);

    /**
     * Provides access to the current memory pool usage on the underlying Apache Arrow BufferAllocator.
     *
     * @return The number of bytes that have been used (e.g. assigned to an Apache Arrow Resource like a block, batch, or buffer).
     */
    long getUsage();

    /**
     * Closes all Apache Arrow resources tracked by this BlockAllocator, freeing their memory.
     */
    void close();

    /**
     * Provides access to the current state of this BlockAllocator.
     *
     * @return True if close has been called, False otherwise.
     */
    boolean isClosed();

    /**
     * Used to generate a batch in a leak free way using the BlockAllocator to handle
     * the boiler plate aspects of error detection and rollback.
     */
    interface BatchGenerator
    {
        /**
         * When called by the BlockAllocator you can generate your batch.
         *
         * @param allocator A referrence to the BlockAllocator you can use to create your batch.
         * @return The resulting ArrowRecordBatch.
         * @throws Exception
         */
        ArrowRecordBatch generate(BufferAllocator allocator)
                throws Exception;
    }
}
