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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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

public class BlockAllocatorImpl
        implements BlockAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(BlockAllocatorImpl.class);

    private final String id;
    private final BufferAllocator rootAllocator;
    private final List<Block> blocks = new ArrayList<>();
    private final List<ArrowRecordBatch> recordBatches = new ArrayList<>();
    private final List<ArrowBuf> arrowBufs = new ArrayList<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public BlockAllocatorImpl()
    {
        this(UUID.randomUUID().toString(), Integer.MAX_VALUE);
    }

    public BlockAllocatorImpl(String id)
    {
        this(id, Integer.MAX_VALUE);
    }

    public BlockAllocatorImpl(String id, long memoryLimit)
    {
        this.rootAllocator = new RootAllocator(memoryLimit);
        this.id = id;
    }

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
     * Allows atomic generation and registration to avoid GC race conditions.
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

    protected synchronized BufferAllocator getRawAllocator()
    {
        logger.debug("getRawAllocator: enter");
        return rootAllocator;
    }

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

    public long getUsage()
    {
        return rootAllocator.getAllocatedMemory();
    }

    @Override
    public synchronized void close()
    {
        if (!isClosed.get()) {
            isClosed.set(true);
            closeBatches();
            closeBlocks();
            closeBuffers();
            rootAllocator.close();
        }
    }

    @Override
    public boolean isClosed()
    {
        return isClosed.get();
    }
}
