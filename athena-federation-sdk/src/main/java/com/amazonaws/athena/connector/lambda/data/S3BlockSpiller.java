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

import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static java.util.Objects.requireNonNull;

public class S3BlockSpiller
        implements AutoCloseable, BlockSpiller
{
    private static final Logger logger = LoggerFactory.getLogger(S3BlockSpiller.class);
    private static final String S3_SEPERATOR = "/";
    private static final long ASYNC_SHUTDOWN_MILLIS = 10_000;

    private final AmazonS3 amazonS3;
    private final BlockCrypto blockCrypto;
    private final BlockAllocator allocator;
    private final SpillConfig spillConfig;
    private final Schema schema;
    private final long maxRowsPerCall;

    private final List<SpillLocation> spillLocations = new ArrayList<>();
    private final AtomicReference<Block> inProgressBlock = new AtomicReference<>();

    //Allows a degree of pipelining to take place so we don't block reading from the source
    //while we are spilling.
    private final ExecutorService asyncSpillPool;
    //Allows us to provide thread safety between async spill completion and calls to
    //getSpill info
    private final ReadWriteLock spillLock = new StampedLock().asReadWriteLock();

    //Used to create monotonically increasing spill locations, if the locations are not
    //monotonically increasing then read performance may suffer as the engine's ability to
    //pipeline reads before write are completed may use this characteristic of the writes
    //to ensure consistency
    private final AtomicLong spillNumber = new AtomicLong(0);

    private final AtomicReference<RuntimeException> asyncException = new AtomicReference<>(null);

    public S3BlockSpiller(AmazonS3 amazonS3, SpillConfig spillConfig, BlockAllocator allocator, Schema schema)
    {
        this(amazonS3, spillConfig, allocator, schema, 100);
    }

    public S3BlockSpiller(AmazonS3 amazonS3,
            SpillConfig spillConfig,
            BlockAllocator allocator,
            Schema schema,
            int maxRowsPerCall)
    {
        this.amazonS3 = requireNonNull(amazonS3, "amazonS3 was null");
        this.spillConfig = requireNonNull(spillConfig, "spillConfig was null");
        this.allocator = requireNonNull(allocator, "allocator was null");
        this.schema = requireNonNull(schema, "schema was null");
        this.blockCrypto = (spillConfig.getEncryptionKey() != null) ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
        asyncSpillPool = (spillConfig.getNumSpillThreads() <= 0) ? null :
                Executors.newFixedThreadPool(spillConfig.getNumSpillThreads());
        this.maxRowsPerCall = maxRowsPerCall;
    }

    public void writeRows(RowWriter rowWriter)
    {
        ensureInit();

        Block block = inProgressBlock.get();
        int rowCount = block.getRowCount();

        int rows = rowWriter.writeRows(block, rowCount);

        if (rows > maxRowsPerCall) {
            throw new RuntimeException("Call generated more than " + maxRowsPerCall + "rows. Generating " +
                    "too many rows per call to writeRows(...) can result in blocks that exceed the max size.");
        }
        if (rows > 0) {
            block.setRowCount(rowCount + rows);
        }

        if (block.getSize() > spillConfig.getMaxBlockBytes()) {
            logger.info("writeRow: Spilling block with {} rows and {} bytes and config {} bytes",
                    new Object[] {block.getRowCount(), block.getSize(), spillConfig.getMaxBlockBytes()});
            spillBlock(block);
            inProgressBlock.set(this.allocator.createBlock(this.schema));
        }
    }

    /**
     * Used to tell if any blocks were spilled or if the response can be inline.
     *
     * @return True is spill occurred, false otherwise.
     */
    public boolean spilled()
    {
        if (asyncException.get() != null) {
            throw asyncException.get();
        }

        //We use the write lock because we want this to have exclusive access to the state
        Lock lock = spillLock.writeLock();
        try {
            lock.lock();
            ensureInit();
            Block block = inProgressBlock.get();
            return !spillLocations.isEmpty() || block.getSize() >= spillConfig.getMaxInlineBlockSize();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * If spilled() returns false this can be used to access the block.
     *
     * @return Block to be inlined in the response.
     * @Throws RuntimeException if blocks were spilled and this method is called.
     */
    public Block getBlock()
    {
        if (spilled()) {
            throw new RuntimeException("Blocks have spilled, calls to getBlock not permitted. use getSpillLocations instead.");
        }

        logger.info("getBlock: Inline Block size[{}] bytes vs {}", inProgressBlock.get().getSize(), spillConfig.getMaxInlineBlockSize());
        return inProgressBlock.get();
    }

    /**
     * If spilled() returns true this can be used to access the spill locations of all blocks.
     *
     * @return List of spill locations.
     * @Throws RuntimeException if blocks were not spilled and this method is called.
     */
    public List<SpillLocation> getSpillLocations()
    {
        if (!spilled()) {
            throw new RuntimeException("Blocks have not spilled, calls to getSpillLocations not permitted. use getBlock instead.");
        }

        Lock lock = spillLock.writeLock();
        try {
            /**
             * Flush the in-progress block in nessesary.
             */
            Block block = inProgressBlock.get();
            if (block.getRowCount() > 0) {
                logger.info("getSpillLocations: Spilling final block with {} rows and {} bytes and config {} bytes",
                        new Object[] {block.getRowCount(), block.getSize(), spillConfig.getMaxBlockBytes()});

                spillBlock(block);

                inProgressBlock.set(this.allocator.createBlock(this.schema));
            }

            lock.lock();
            return spillLocations;
        }
        finally {
            lock.unlock();
        }
    }

    public void close()
    {
        if (asyncSpillPool == null) {
            return;
        }

        asyncSpillPool.shutdown();
        try {
            if (!asyncSpillPool.awaitTermination(ASYNC_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS)) {
                asyncSpillPool.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            asyncSpillPool.shutdownNow();
        }
    }

    protected SpillLocation write(Block block)
    {
        try {
            S3SpillLocation spillLocation = makeSpillLocation();
            EncryptionKey encryptionKey = spillConfig.getEncryptionKey();

            logger.info("write: Started encrypting block for write to {}", spillLocation);
            byte[] bytes = blockCrypto.encrypt(encryptionKey, block);

            logger.info("write: Started spilling block of size {} bytes", bytes.length);
            amazonS3.putObject(spillLocation.getBucket(),
                    spillLocation.getKey(),
                    new ByteArrayInputStream(bytes),
                    new ObjectMetadata());
            logger.info("write: Completed spilling block of size {} bytes", bytes.length);

            return spillLocation;
        }
        catch (RuntimeException ex) {
            asyncException.compareAndSet(null, ex);
            logger.warn("write: Encountered error while writing block.", ex);
            throw ex;
        }
    }

    protected Block read(S3SpillLocation spillLocation, EncryptionKey key, Schema schema)
    {
        try {
            logger.debug("write: Started reading block from S3");
            S3Object fullObject = amazonS3.getObject(spillLocation.getBucket(), spillLocation.getKey());
            logger.debug("write: Completed reading block from S3");
            Block block = blockCrypto.decrypt(key, ByteStreams.toByteArray(fullObject.getObjectContent()), schema);
            logger.debug("write: Completed decrypting block of size.");
            return block;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void spillBlock(Block block)
    {
        if (asyncSpillPool != null) {
            //We use the read lock here because we want to allow these in parallel, its a bit counter intuitive
            Lock lock = spillLock.readLock();
            try {
                //We lock before going async but unlock after spilling in the async thread, this makes it easy to use
                //the ReadWrite lock to tell if all spills are completed without killing the thread pool.
                lock.lock();
                asyncSpillPool.submit(() -> {
                    try {
                        SpillLocation spillLocation = write(block);
                        spillLocations.add(spillLocation);
                        //Free the memory from the previous block since it has been spilled
                        safeClose(block);
                    }
                    finally {
                        lock.unlock();
                    }
                });
            }
            catch (Exception ex) {
                //If we hit an exception, make sure we unlock to avoid a deadlock before throwing.
                lock.unlock();
                throw ex;
            }
        }
        else {
            SpillLocation spillLocation = write(block);
            spillLocations.add(spillLocation);
            safeClose(block);
        }
    }

    private void ensureInit()
    {
        if (inProgressBlock.get() == null) {
            //Create the initial block
            inProgressBlock.set(this.allocator.createBlock(this.schema));
        }
    }

    /**
     * This needs to be thread safe and generate locations in a format of:
     * location.0
     * location.1
     * location.2
     * <p>
     * The read engine may elect to exploit this naming convention to speed up the pipelining of
     * reads while the spiller is still writing. Violating this convention may reduce performance
     * or increase calls to S3.
     */
    private S3SpillLocation makeSpillLocation()
    {
        S3SpillLocation splitSpillLocation = (S3SpillLocation) spillConfig.getSpillLocation();
        if (!splitSpillLocation.isDirectory()) {
            throw new RuntimeException("Split's SpillLocation must be a directory because multiple blocks may be spilled.");
        }
        String blockKey = splitSpillLocation.getKey() + "." + spillNumber.getAndIncrement();
        return new S3SpillLocation(splitSpillLocation.getBucket(), blockKey, false);
    }

    private void safeClose(AutoCloseable block)
    {
        try {
            block.close();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
