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
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Basic BlockAllocator which uses reference counting to perform garbage collection of Apache Arrow resources.
 *
 * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
 */
public class BlockAllocatorImpl
        implements BlockAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(BlockAllocatorImpl.class);

    //Identifier for this block allocator, mostly used by BlockAllocatorRegistry.
    private final String id;
    //The Apache Arrow Buffer Allocator that we are wrapping with reference counting and clean up.
    private final BufferAllocator rootAllocator;
    private final boolean ownRootAllocator;
    //The Blocks that have been allocated via this BlockAllocator
    private final List<Block> blocks = new ArrayList<>();
    //The record batches that have been allocated via this BlockAllocator
    private final List<ArrowRecordBatch> recordBatches = new ArrayList<>();
    //The arrow buffers that have been allocated via this BlockAllocator
    private final List<ArrowBuf> arrowBufs = new ArrayList<>();
    //Flag inficating if this allocator has been closed.
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * Default constructor.
     */
    public BlockAllocatorImpl()
    {
        this(UUID.randomUUID().toString(), Integer.MAX_VALUE);
    }

    /**
     * Default constructor that takes in a shared RootAllocator
     */
    public BlockAllocatorImpl(RootAllocator rootAllocator)
    {
        this(UUID.randomUUID().toString(), rootAllocator);
    }

    /**
     * Constructs a BlockAllocatorImpl with the given id.
     *
     * @param id The id used to identify this BlockAllocatorImpl
     */
    public BlockAllocatorImpl(String id)
    {
        this(id, Integer.MAX_VALUE);
    }

    /**
     * Constructs a BlockAllocatorImpl with the given id and a shared RootAllocator
     *
     * @param id The id used to identify this BlockAllocatorImpl
     * @param rootAllocator the shared RootAllocator
     */
    public BlockAllocatorImpl(String id, RootAllocator rootAllocator)
    {
        this.rootAllocator = rootAllocator;
        this.ownRootAllocator = false;
        this.id = id;
    }

    /**
     * Constructs a BlockAllocatorImpl with the given id and memory byte limit.
     *
     * @param id The id used to identify this BlockAllocatorImpl
     * @param memoryLimit The max memory, in bytes, that this BlockAllocator is allows to use.
     */
    public BlockAllocatorImpl(String id, long memoryLimit)
    {
        this.rootAllocator = new RootAllocator(memoryLimit);
        this.ownRootAllocator = true;
        this.id = id;
    }

    /**
     * Creates a block and registers it for later clean up if the block isn't explicitly closed by the caller.
     *
     * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
     */
    public synchronized Block createBlock(Schema schema)
    {
        Block block = null;
        VectorSchemaRoot vectorSchemaRoot = null;
        List<FieldVector> vectors = new ArrayList();
        try {
            for (Field next : schema.getFields()) {
                vectors.add(next.createVector(rootAllocator));
            }
            vectorSchemaRoot = new VectorSchemaRoot(schema, vectors, 0);
            block = new Block(id, schema, vectorSchemaRoot);
            blocks.add(block);
        }
        catch (Exception ex) {
            if (block != null) {
                try {
                    block.close();
                }
                catch (Exception ex2) {
                    logger.error("createBlock: error while closing block during previous error.", ex2);
                }
            }

            if (vectorSchemaRoot != null) {
                vectorSchemaRoot.close();
            }

            for (FieldVector next : vectors) {
                next.close();
            }

            throw ex;
        }
        return block;
    }

    /**
     * Creates an ArrowBuf and registers it for later clean up if the ArrowBuff isn't explicitly closed by the caller.
     *
     * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
     */
    public ArrowBuf createBuffer(int size)
    {
        ArrowBuf buffer = null;
        try {
            buffer = rootAllocator.buffer(size);
            arrowBufs.add(buffer);
            return buffer;
        }
        catch (Exception ex) {
            if (buffer != null) {
                buffer.close();
            }
            throw ex;
        }
    }

    /**
     * Creates an ArrowRecordBatch and registers it for later clean up if the ArrowRecordBatch isn't explicitly closed
     * by the caller.
     *
     * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
     */
    public synchronized ArrowRecordBatch registerBatch(BatchGenerator generator)
    {
        try {
            logger.debug("registerBatch: {}", recordBatches.size());
            ArrowRecordBatch batch = generator.generate(getRawAllocator());
            recordBatches.add(batch);
            return batch;
        }
        catch (org.apache.arrow.memory.OutOfMemoryException ex) {
            //Must not wrap or we may break resource management logic elsewhere
            throw ex;
        }
        catch (RuntimeException ex) {
            throw ex;
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Provides access to the underlying Apache Arrow Allocator.
     *
     * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
     */
    protected synchronized BufferAllocator getRawAllocator()
    {
        logger.debug("getRawAllocator: enter");
        return rootAllocator;
    }

    /**
     * Attempts to close all Blocks allocated by this BlockAllocator.
     */
    @VisibleForTesting
    protected synchronized void closeBlocks()
    {
        logger.debug("closeBlocks: {}", blocks.size());
        for (Block next : blocks) {
            try {
                next.close();
            }
            catch (Exception ex) {
                logger.warn("closeBlocks: Error closing block", ex);
            }
        }
        blocks.clear();
    }

    /**
     * Attempts to close all buffers allocated by this BlockAllocator.
     */
    @VisibleForTesting
    protected synchronized void closeBuffers()
    {
        logger.debug("closeBuffers: {}", arrowBufs.size());
        for (ArrowBuf next : arrowBufs) {
            try {
                next.close();
            }
            catch (Exception ex) {
                logger.warn("closeBuffers: Error closing buffer", ex);
            }
        }
        arrowBufs.clear();
    }

    /**
     * Attempts to close all batches allocated by this BlockAllocator.
     */
    @VisibleForTesting
    protected synchronized void closeBatches()
    {
        logger.debug("closeBatches: {}", recordBatches.size());
        for (ArrowRecordBatch next : recordBatches) {
            try {
                next.close();
            }
            catch (Exception ex) {
                logger.warn("closeBatches: Error closing batch", ex);
            }
        }
        recordBatches.clear();
    }

    /**
     * Returns number of bytes in the Apache Arrow Pool that are used. This is not the same as the actual
     * reserved memory usage you may be familiar with from your operating system.
     *
     * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
     */
    public long getUsage()
    {
        return rootAllocator.getAllocatedMemory();
    }

    /**
     * Closes all Apache Arrow Resources allocated via this BlockAllocator and then attempts to
     * close the underlying Apache Arrow Allocator which would actually free memory. This operation may
     * fail if the underlying Apache Arrow Allocator was used to allocate resources without registering
     * them to this BlockAllocator and those resources were not freed prior to calling close.
     *
     * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
     */
    @Override
    public synchronized void close()
    {
        if (!isClosed.get()) {
            isClosed.set(true);
            closeBatches();
            closeBlocks();
            closeBuffers();
            // Do not close rootAllocators that we do not own
            if (ownRootAllocator) {
                rootAllocator.close();
            }
        }
    }

    /**
     * Indicates if this BlockAllocator has been closed.
     *
     * @see com.amazonaws.athena.connector.lambda.data.BlockAllocator
     */
    @Override
    public boolean isClosed()
    {
        return isClosed.get();
    }
}
