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

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of BlockSpiller which spills Blocks from large results to S3 with optional AES-GCM encryption.
 *
 * @note The size at which this implementation will spill to S3 are configured using SpillConfig.
 */
public class S3BlockSpiller
        implements AutoCloseable, BlockSpiller
{
    private static final Logger logger = LoggerFactory.getLogger(S3BlockSpiller.class);
    //Used to control how long we will wait for background spill threads to exit.
    private static final long ASYNC_SHUTDOWN_MILLIS = 10_000;
    //The default max number of rows that are allowed to be written per call to writeRows(...)
    private static final int MAX_ROWS_PER_CALL = 100;
    //Config to set spill queue capacity
    private static final String SPILL_QUEUE_CAPACITY = "SPILL_QUEUE_CAPACITY";

    private static final String SPILL_PUT_REQUEST_HEADERS_ENV = "spill_put_request_headers";
    //Used to write to S3
    private final S3Client amazonS3;
    //Used to optionally encrypt Blocks.
    private final BlockCrypto blockCrypto;
    //Used to create new blocks.
    private final BlockAllocator allocator;
    //Controls how/when/where/if this implementation will spill to S3.
    private final SpillConfig spillConfig;
    //The schema to use for Blocks.
    private final Schema schema;
    //The max number of rows that are allowed to be written per call to writeRows(...)
    private final long maxRowsPerCall;
    //If we spilled, the spill locations are kept here.
    private final List<SpillLocation> spillLocations = new ArrayList<>();
    //Reference to the in progress Block.
    private final AtomicReference<Block> inProgressBlock = new AtomicReference<>();
    //Allows a degree of pipelining to take place so we don't block reading from the source
    //while we are spilling.
    private final ExecutorService asyncSpillPool;
    //Allows us to provide thread safety between async spill completion and calls to getSpill status
    private final ReadWriteLock spillLock = new StampedLock().asReadWriteLock();
    //Used to create monotonically increasing spill locations, if the locations are not
    //monotonically increasing then read performance may suffer as the engine's ability to
    //pre-fetch/pipeline reads before write are completed may use this characteristic of the writes
    //to ensure consistency
    private final AtomicLong spillNumber = new AtomicLong(0);
    //Holder that is used to surface any exceptions encountered in our background spill threads.
    private final AtomicReference<RuntimeException> asyncException = new AtomicReference<>(null);
    //
    private final ConstraintEvaluator constraintEvaluator;
    //Used to track total bytes written
    private final AtomicLong totalBytesSpilled = new AtomicLong();
    //Time this BlockSpiller wss created.
    private final long startTime = System.currentTimeMillis();

    // Config options
    // These are from System.getenv() when the connector is being used from an AWS Lambda (*CompositeHandler).
    // When being used from Spark, it is from the Spark session's options.
    private final java.util.Map<String, String> configOptions;

    /**
     * Constructor which uses the default maxRowsPerCall.
     *
     * @param amazonS3 AmazonS3 client to use for writing to S3.
     * @param spillConfig The spill config for this instance. Includes things like encryption key, s3 path, etc...
     * @param allocator The BlockAllocator to use when creating blocks.
     * @param schema The schema for blocks that should be written.
     * @param constraintEvaluator The ConstraintEvaluator that should be used to constrain writes.
     */
    public S3BlockSpiller(
        S3Client amazonS3,
        SpillConfig spillConfig,
        BlockAllocator allocator,
        Schema schema,
        ConstraintEvaluator constraintEvaluator,
        java.util.Map<String, String> configOptions)
    {
        this(amazonS3, spillConfig, allocator, schema, constraintEvaluator, MAX_ROWS_PER_CALL, configOptions);
    }

    /**
     * Constructs a new S3BlockSpiller.
     *
     * @param amazonS3 AmazonS3 client to use for writing to S3.
     * @param spillConfig The spill config for this instance. Includes things like encryption key, s3 path, etc...
     * @param allocator The BlockAllocator to use when creating blocks.
     * @param schema The schema for blocks that should be written.
     * @param constraintEvaluator The ConstraintEvaluator that should be used to constrain writes.
     * @param maxRowsPerCall The max number of rows to allow callers to write in one call.
     */
    public S3BlockSpiller(
        S3Client amazonS3,
        SpillConfig spillConfig,
        BlockAllocator allocator,
        Schema schema,
        ConstraintEvaluator constraintEvaluator,
        int maxRowsPerCall,
        java.util.Map<String, String> configOptions)
    {
        this.configOptions = configOptions;
        this.amazonS3 = requireNonNull(amazonS3, "amazonS3 was null");
        this.spillConfig = requireNonNull(spillConfig, "spillConfig was null");
        this.allocator = requireNonNull(allocator, "allocator was null");
        this.schema = requireNonNull(schema, "schema was null");
        this.blockCrypto = (spillConfig.getEncryptionKey() != null) ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
        asyncSpillPool = (spillConfig.getNumSpillThreads() <= 0) ? null : makeAsyncSpillPool(spillConfig);
        this.maxRowsPerCall = maxRowsPerCall;
        this.constraintEvaluator = constraintEvaluator;
    }

    /**
     * Provides access to the constraint evaluator used to constrain blocks written via this BlockSpiller.
     *
     * @return
     */
    @Override
    public ConstraintEvaluator getConstraintEvaluator()
    {
        return constraintEvaluator;
    }

    /**
     * Used to write rows via the BlockWriter.
     *
     * @param rowWriter The RowWriter that the BlockWriter should use to write rows into the Block(s) it is managing.
     * @see BlockSpiller
     */
    public void writeRows(RowWriter rowWriter)
    {
        ensureInit();

        Block block = inProgressBlock.get();
        int rowCount = block.getRowCount();

        int rows;
        try {
            rows = rowWriter.writeRows(block, rowCount);
        }
        catch (Exception ex) {
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }

        if (rows > maxRowsPerCall) {
            throw new AthenaConnectorException("Call generated more than " + maxRowsPerCall + "rows. Generating " +
                    "too many rows per call to writeRows(...) can result in blocks that exceed the max size.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        if (rows > 0) {
            block.setRowCount(rowCount + rows);
        }

        if (block.getSize() > spillConfig.getMaxBlockBytes()) {
            logger.info("writeRow: Spilling block with {} rows and {} bytes and config {} bytes",
                    new Object[] {block.getRowCount(), block.getSize(), spillConfig.getMaxBlockBytes()});
            spillBlock(block);
            inProgressBlock.set(this.allocator.createBlock(this.schema));
            inProgressBlock.get().constrain(constraintEvaluator);
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
            throw new AthenaConnectorException("Blocks have spilled, calls to getBlock not permitted. use getSpillLocations instead.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
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
            throw new AthenaConnectorException("Blocks have not spilled, calls to getSpillLocations not permitted. use getBlock instead.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
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
                inProgressBlock.get().constrain(constraintEvaluator);
            }

            lock.lock();
            return spillLocations;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Frees any resources held by this BlockSpiller.
     *
     * @see BlockSpiller
     */
    public void close()
    {
        logger.info("close: Spilled a total of {} bytes in {} ms", totalBytesSpilled.get(), System.currentTimeMillis() - startTime);

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

    /**
     * Grabs the request headers from env and sets them on the request
     */
    private Map<String, String> getRequestHeadersFromEnv()
    {
        String headersFromEnvStr = configOptions.get(SPILL_PUT_REQUEST_HEADERS_ENV);
        if (headersFromEnvStr == null || headersFromEnvStr.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {};
            Map<String, String> headers = mapper.readValue(headersFromEnvStr, typeRef);
            return headers;
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            String message = String.format("Invalid value for environment variable: %s : %s",
                    SPILL_PUT_REQUEST_HEADERS_ENV, headersFromEnvStr);
            logger.error(message, e);
        }
        return Collections.emptyMap();
    }

    /**
     * Creates an AwsRequestOverrideConfiguration with custom headers from the environment
     */
    private Optional<AwsRequestOverrideConfiguration> createRequestOverrideConfig()
    {
        Map<String, String> headers = getRequestHeadersFromEnv();
        if (headers.isEmpty()) {
            return Optional.empty();
        }

        AwsRequestOverrideConfiguration.Builder overrideConfigBuilder = AwsRequestOverrideConfiguration.builder();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            overrideConfigBuilder.putHeader(header.getKey(), header.getValue());
        }
        return Optional.of(overrideConfigBuilder.build());
    }

    /**
     * Writes (aka spills) a Block.
     */
    protected SpillLocation write(Block block)
    {
        try {
            S3SpillLocation spillLocation = makeSpillLocation();
            EncryptionKey encryptionKey = spillConfig.getEncryptionKey();

            logger.info("write: Started encrypting block for write to {}", spillLocation);
            byte[] bytes = blockCrypto.encrypt(encryptionKey, block);

            totalBytesSpilled.addAndGet(bytes.length);

            logger.info("write: Started spilling block of size {} bytes", bytes.length);

            // Set the contentLength otherwise the s3 client will buffer again since it
            // only sees the InputStream wrapper.
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                    .bucket(spillLocation.getBucket())
                    .key(spillLocation.getKey())
                    .contentLength((long) bytes.length);

            // Set request headers via overrideConfiguration instead of metadata
            createRequestOverrideConfig().ifPresent(requestBuilder::overrideConfiguration);

            PutObjectRequest request = requestBuilder.build();
            amazonS3.putObject(request, RequestBody.fromBytes(bytes));
            logger.info("write: Completed spilling block of size {} bytes", bytes.length);

            return spillLocation;
        }
        catch (RuntimeException ex) {
            asyncException.compareAndSet(null, ex);
            logger.warn("write: Encountered error while writing block.", ex);
            throw ex;
        }
    }

    /**
     * Reads a spilled block.
     *
     * @param spillLocation The location to read the spilled Block from.
     * @param key The encryption key to use when reading the spilled Block.
     * @param schema The Schema to use when deserializing the spilled Block.
     * @return The Block stored at the spill location.
     */
    protected Block read(S3SpillLocation spillLocation, EncryptionKey key, Schema schema)
    {
        try {
            logger.debug("write: Started reading block from S3");
            ResponseInputStream<GetObjectResponse> responseStream = amazonS3.getObject(GetObjectRequest.builder()
                    .bucket(spillLocation.getBucket())
                    .key(spillLocation.getKey())
                    .build());
            logger.debug("write: Completed reading block from S3");
            Block block = blockCrypto.decrypt(key, ByteStreams.toByteArray(responseStream), schema);
            logger.debug("write: Completed decrypting block of size.");
            return block;
        }
        catch (IOException ex) {
            throw new AthenaConnectorException(ex.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
    }

    /**
     * Spills a block, potentially asynchronously depending on the settings.
     *
     * @param block The Block to spill.
     */
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

    /**
     * Ensures that the initial Block is initialized.
     */
    private void ensureInit()
    {
        if (inProgressBlock.get() == null) {
            //Create the initial block
            inProgressBlock.set(this.allocator.createBlock(this.schema));
            inProgressBlock.get().constrain(constraintEvaluator);
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
            throw new AthenaConnectorException("Split's SpillLocation must be a directory because multiple blocks may be spilled.", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
        String blockKey = splitSpillLocation.getKey() + "." + spillNumber.getAndIncrement();
        return new S3SpillLocation(splitSpillLocation.getBucket(), blockKey, false);
    }

    /**
     * Closes the supplied AutoCloseable and remaps any actions to Runtime.
     *
     * @param block The Block to close.
     */
    private void safeClose(AutoCloseable block)
    {
        try {
            block.close();
        }
        catch (Exception ex) {
            throw new AthenaConnectorException(ex.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
    }

    /**
     * Used to create a thread pool that will be used to service writes to S3 associated with spilling blocks.
     * This pool should use a blocking, fixed size, pool for work in order to avoid a fast producer from overhwelming
     * the Apache Arrow Allocator's memory pool.
     *
     * @return A fixed size thread pool with fixed size and blocking runnable queue.
     */
    private ThreadPoolExecutor makeAsyncSpillPool(SpillConfig config)
    {
        int spillQueueCapacity = config.getNumSpillThreads();

        String capacity = StringUtils.isNotBlank(configOptions.get(SPILL_QUEUE_CAPACITY)) ? configOptions.get(SPILL_QUEUE_CAPACITY) : configOptions.get(SPILL_QUEUE_CAPACITY.toLowerCase());
        if (capacity != null) {
            spillQueueCapacity = Integer.parseInt(capacity);
            logger.debug("Setting Spill Queue Capacity to {}", spillQueueCapacity);
        }

        RejectedExecutionHandler rejectedExecutionHandler = (r, executor) -> {
            if (!executor.isShutdown()) {
                try {
                    executor.getQueue().put(r);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RejectedExecutionException("Received an exception while submitting spillBlock task: ", e);
                }
            }
        };

        return new ThreadPoolExecutor(config.getNumSpillThreads(),
                config.getNumSpillThreads(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(spillQueueCapacity),
                rejectedExecutionHandler);
    }
}
