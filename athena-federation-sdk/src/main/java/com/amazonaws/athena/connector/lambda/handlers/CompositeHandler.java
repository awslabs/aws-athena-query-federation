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
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class allows you to have a single Lambda function be responsible for both metadata and data operations by
 * composing a MetadataHandler with a RecordHandler and muxing requests to the appropriate class. You might choose
 * to use this CompositeHandler to run a single lambda function for the following reasons:
 * 1. Can be simpler to deploy and manage a single vs multiple Lambda functions
 * 2. You don't need to independently control the cost or performance of metadata vs. data operations.
 *
 * @see RequestStreamHandler
 */
public class CompositeHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CompositeHandler.class);
    //The MetadataHandler to delegate metadata operations to.
    private final MetadataHandler metadataHandler;
    //The RecordHandler to delegate data operations to.
    private final RecordHandler recordHandler;
    //(Optional) The UserDefinedFunctionHandler to delegate UDF operations to.
    private final UserDefinedFunctionHandler udfhandler;

    /**
     * Basic constructor that composes a MetadataHandler with a RecordHandler.
     *
     * @param metadataHandler The MetadataHandler to delegate metadata operations to.
     * @param recordHandler The RecordHandler to delegate data operations to.
     */
    public CompositeHandler(MetadataHandler metadataHandler, RecordHandler recordHandler)
    {
        this.metadataHandler = metadataHandler;
        this.recordHandler = recordHandler;
        this.udfhandler = null;
    }

    /**
     * Basic constructor that composes a MetadataHandler, RecordHandler, and a UserDefinedFunctionHandler
     *
     * @param metadataHandler The MetadataHandler to delegate metadata operations to.
     * @param recordHandler The RecordHandler to delegate data operations to.
     * @param udfhandler The UserDefinedFunctionHandler to delegate UDF operations to.
     */
    public CompositeHandler(MetadataHandler metadataHandler, RecordHandler recordHandler, UserDefinedFunctionHandler udfhandler)
    {
        this.metadataHandler = metadataHandler;
        this.recordHandler = recordHandler;
        this.udfhandler = udfhandler;
    }

    /**
     * Required by Lambda's RequestStreamHandler interface. In our case we use this method to handle some
     * basic resource lifecycle tasks for the request, namely the BlockAllocator and the request object itself.
     */
    public final void handleRequest(InputStream inputStream, OutputStream outputStream, final Context context)
            throws IOException
    {
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            ObjectMapper objectMapper = VersionedObjectMapperFactory.create(allocator, SerDeVersion.SERDE_VERSION);
            FederationRequest rawReq;
            byte[] allInputBytes = com.google.common.io.ByteStreams.toByteArray(inputStream);

            try {
                rawReq = objectMapper.readValue(allInputBytes, FederationRequest.class);
            }
            catch (IllegalStateException e) { // if client has not upgraded to our latest, fallback to v4
                logger.warn("Client's SerDe is not upgraded to latest version, defaulting to V4:", e);
                objectMapper = VersionedObjectMapperFactory.create(allocator, 4);
                rawReq = objectMapper.readValue(allInputBytes, FederationRequest.class);
            }
            if (rawReq instanceof MetadataRequest) {
                ((MetadataRequest) rawReq).setContext(context);
            }
            handleRequest(allocator, rawReq, outputStream, objectMapper);
            rawReq.close();
        }
        catch (Exception ex) {
            logger.warn("handleRequest: Completed with an exception.", ex);
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    /**
     * Handles routing the request to the appropriate Handler, either MetadataHandler or RecordHandler.
     *
     * @param allocator The BlockAllocator to use for Apache Arrow Resources.
     * @param rawReq The request object itself.
     * @param outputStream The OutputStream to which all responses should be written.
     * @param objectMapper The ObjectMapper that can be used for serializing responses.
     * @throws Exception
     * @note that PingRequests are routed to the MetadataHandler even though both MetadataHandler and RecordHandler
     * implemented PingRequest handling.
     */
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
        else if (udfhandler != null && rawReq instanceof UserDefinedFunctionRequest) {
            udfhandler.doHandleRequest(allocator, objectMapper, (UserDefinedFunctionRequest) rawReq, outputStream);
        }
        else {
            throw new IllegalArgumentException("Unknown request class " + rawReq.getClass());
        }
    }

    /**
     * Helper used to assert that the response generated by the handler is not null.
     */
    private void assertNotNull(FederationResponse response)
    {
        if (response == null) {
            throw new RuntimeException("Response was null");
        }
    }
}
