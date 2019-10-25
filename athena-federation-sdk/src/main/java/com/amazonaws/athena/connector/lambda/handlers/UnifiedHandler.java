package com.amazonaws.athena.connector.lambda.handlers;

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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordRequest;
import com.amazonaws.athena.connector.lambda.records.RecordRequestType;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.amazonaws.athena.connector.lambda.handlers.FederationCapabilities.CAPABILITIES;

/**
 * The UnifiedHandler provides a convenient way to use only 1 Lambda function for both meta-data
 * and data requests. This reduces cold start times and simplifies deployment at the expense of
 * more complex handler class and possible more expensive Lambda invocations since you won't be able
 * to select the memory size of the metadata vs. data Lambda invocations independently. In practice
 * this memory sizing is only meaningful with long running or high invocation rates.
 */
public abstract class UnifiedHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataHandler.class);
    private static final int NUM_SPILL_THREADS = 2;

    private final AmazonS3 amazonS3;
    private final String sourceType;

    /**
     * @param sourceType Used to aid in logging diagnostic info when raising a support case.
     */
    public UnifiedHandler(AmazonS3 amazonS3, String sourceType)
    {
        this.sourceType = sourceType;
        this.amazonS3 = amazonS3;
    }

    final public void handleRequest(InputStream inputStream, OutputStream outputStream, final Context context)
            throws IOException
    {
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            ObjectMapper objectMapper = ObjectMapperFactory.create(allocator);
            try (FederationRequest rawReq = objectMapper.readValue(inputStream, FederationRequest.class)) {
                handleRequest(allocator, rawReq, outputStream, objectMapper);
            }
        }
        catch (Exception ex) {
            logger.warn("handleRequest: Completed with an exception.", ex);
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    public void handleRequest(BlockAllocator allocator, FederationRequest rawReq, OutputStream outputStream, ObjectMapper objectMapper)
            throws Exception
    {
        if (rawReq instanceof PingRequest) {
            try (PingResponse response = doPing((PingRequest) rawReq)) {
                assertNotNull(response);
                objectMapper.writeValue(outputStream, response);
            }
            return;
        }
        else if (rawReq instanceof MetadataRequest) {
            handleMetadataRequest(allocator, objectMapper, rawReq, outputStream);
        }
        else if (rawReq instanceof RecordRequest) {
            handleRecordRequest(allocator, objectMapper, rawReq, outputStream);
        }
        else {
            throw new IllegalArgumentException("Unknown request class " + rawReq.getClass());
        }
    }

    private void handleRecordRequest(BlockAllocator allocator,
            ObjectMapper objectMapper,
            FederationRequest rawReq,
            OutputStream outputStream)
            throws Exception
    {
        RecordRequest req = (RecordRequest) rawReq;
        RecordRequestType type = req.getRequestType();
        switch (type) {
            case READ_RECORDS:
                try (RecordResponse response = doReadRecords(allocator, (ReadRecordsRequest) req)) {
                    assertNotNull(response);
                    objectMapper.writeValue(outputStream, response);
                }
                return;
            default:
                throw new IllegalArgumentException("Unknown request type " + type);
        }
    }

    public RecordResponse doReadRecords(BlockAllocator allocator, ReadRecordsRequest request)
            throws Exception
    {
        logger.info("doReadRecords: {}:{}", request.getSchema(), request.getSplit().getSpillLocation());
        SpillConfig spillConfig = getSpillConfig(request);
        try (S3BlockSpiller spiller = new S3BlockSpiller(amazonS3, spillConfig, allocator, request.getSchema())) {

            try (ConstraintEvaluator constraintEvaluator = new ConstraintEvaluator(allocator,
                    request.getSchema(),
                    request.getConstraints())) {
                readWithConstraint(constraintEvaluator, spiller, request);
            }
            catch (Exception ex) {
                throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
            }

            if (!spiller.spilled()) {
                return new ReadRecordsResponse(request.getCatalogName(), spiller.getBlock());
            }
            else {
                return new RemoteReadRecordsResponse(request.getCatalogName(),
                        request.getSchema(),
                        spiller.getSpillLocations(),
                        spillConfig.getEncryptionKey());
            }
        }
    }

    protected abstract void readWithConstraint(ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest)
            throws Exception;

    protected SpillConfig getSpillConfig(ReadRecordsRequest request)
    {
        return SpillConfig.newBuilder()
                .withSpillLocation(request.getSplit().getSpillLocation())
                .withMaxBlockBytes(request.getMaxBlockSize())
                .withMaxInlineBlockBytes(request.getMaxInlineBlockSize())
                .withRequestId(request.getQueryId())
                .withEncryptionKey(request.getSplit().getEncryptionKey())
                .withNumSpillThreads(NUM_SPILL_THREADS)
                .build();
    }

    private void handleMetadataRequest(BlockAllocator allocator,
            ObjectMapper objectMapper,
            FederationRequest rawReq,
            OutputStream outputStream)
            throws Exception
    {
        MetadataRequest req = (MetadataRequest) rawReq;
        MetadataRequestType type = req.getRequestType();
        switch (type) {
            case LIST_SCHEMAS:
                try (ListSchemasResponse response = doListSchemaNames(allocator, (ListSchemasRequest) req)) {
                    assertNotNull(response);
                    objectMapper.writeValue(outputStream, response);
                }
                return;
            case LIST_TABLES:
                try (ListTablesResponse response = doListTables(allocator, (ListTablesRequest) req)) {
                    assertNotNull(response);
                    objectMapper.writeValue(outputStream, response);
                }
                return;
            case GET_TABLE:
                try (GetTableResponse response = doGetTable(allocator, (GetTableRequest) req)) {
                    assertNotNull(response);
                    objectMapper.writeValue(outputStream, response);
                }
                return;
            case GET_TABLE_LAYOUT:
                try (GetTableLayoutResponse response = doGetTableLayout(allocator, (GetTableLayoutRequest) req)) {
                    assertNotNull(response);
                    objectMapper.writeValue(outputStream, response);
                }
                return;
            case GET_SPLITS:
                try (GetSplitsResponse response = doGetSplits(allocator, (GetSplitsRequest) req)) {
                    assertNotNull(response);
                    objectMapper.writeValue(outputStream, response);
                }
                return;
            default:
                throw new IllegalArgumentException("Unknown request type " + type);
        }
    }

    protected abstract ListSchemasResponse doListSchemaNames(final BlockAllocator allocator, final ListSchemasRequest request)
            throws Exception;

    protected abstract ListTablesResponse doListTables(final BlockAllocator allocator, final ListTablesRequest request)
            throws Exception;

    protected abstract GetTableResponse doGetTable(final BlockAllocator allocator, final GetTableRequest request)
            throws Exception;

    protected abstract GetTableLayoutResponse doGetTableLayout(final BlockAllocator allocator, final GetTableLayoutRequest request)
            throws Exception;

    protected abstract GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
            throws Exception;

    private final PingResponse doPing(PingRequest request)
    {
        PingResponse response = new PingResponse(request.getCatalogName(), request.getQueryId(), sourceType, CAPABILITIES);
        try {
            onPing(request);
        }
        catch (Exception ex) {
            logger.warn("doPing: encountered an exception while delegating onPing.", ex);
        }
        return response;
    }

    protected void onPing(PingRequest request)
    {
        //NoOp
    }

    private void assertNotNull(FederationResponse response)
    {
        if (response == null) {
            throw new RuntimeException("Response was null");
        }
    }
}
