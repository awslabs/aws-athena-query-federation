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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.records.RecordRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CompositeHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CompositeHandler.class);
    private final MetadataHandler metadataHandler;
    private final RecordHandler recordHandler;

    public CompositeHandler(MetadataHandler metadataHandler, RecordHandler recordHandler)
    {
        this.metadataHandler = metadataHandler;
        this.recordHandler = recordHandler;
    }

    public final void handleRequest(InputStream inputStream, OutputStream outputStream, final Context context)
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

    public final void handleRequest(BlockAllocator allocator, FederationRequest rawReq, OutputStream outputStream, ObjectMapper objectMapper)
            throws Exception
    {
        if (rawReq instanceof PingRequest) {
            try (PingResponse response = metadataHandler.doPing((PingRequest) rawReq)) {
                assertNotNull(response);
                objectMapper.writeValue(outputStream, response);
            }
            return;
        }

        if (rawReq instanceof MetadataRequest) {
            metadataHandler.doHandleRequest(allocator, objectMapper, (MetadataRequest) rawReq, outputStream);
        }
        else if (rawReq instanceof RecordRequest) {
            recordHandler.doHandleRequest(allocator, objectMapper, (RecordRequest) rawReq, outputStream);
        }
        else {
            throw new IllegalArgumentException("Unknown request class " + rawReq.getClass());
        }
    }

    private void assertNotNull(FederationResponse response)
    {
        if (response == null) {
            throw new RuntimeException("Response was null");
        }
    }
}
