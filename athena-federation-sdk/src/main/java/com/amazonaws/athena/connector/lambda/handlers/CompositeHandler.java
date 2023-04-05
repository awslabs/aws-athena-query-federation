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
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.proto.request.PingRequest;
import com.amazonaws.athena.connector.lambda.proto.request.TypeHeader;
import com.amazonaws.athena.connector.lambda.proto.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

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
        // in the existing logic, we were attching the Lambda Context variable to the metadata request, which is messy.
        // For now, I'll stick it in the config options with only the part we need.
        metadataHandler.configOptions.put(MetadataHandler.FUNCTION_ARN_CONFIG_KEY, context.getInvokedFunctionArn());

        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            String inputJson = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            TypeHeader typeHeader = (TypeHeader) ProtobufSerDe.buildFromJson(inputJson, TypeHeader.newBuilder());
            handleRequest(allocator, typeHeader, inputJson, outputStream);
        }
        catch (Exception ex) {
            logger.warn("handleRequest: Completed with an exception.", ex);
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    /**
     * Based on the type of request, delegate to appropriate handler (Metadata/Record/Udf).
     * We work under the invariant that the delegate handlers will write to the output stream.
     */
    public final void handleRequest(BlockAllocator allocator, TypeHeader typeHeader, String inputJson, OutputStream outputStream)
            throws Exception
    {
        String type = typeHeader.getType();
        switch(type) {
            case "PingRequest":
                PingRequest pingRequest = (PingRequest) ProtobufSerDe.buildFromJson(inputJson, PingRequest.newBuilder());
                metadataHandler.doPing(pingRequest, outputStream);
                return;
            case "ReadRecordsRequest":
                ReadRecordsRequest readRecordsRequest = (ReadRecordsRequest) ProtobufSerDe.buildFromJson(inputJson, ReadRecordsRequest.newBuilder());
                recordHandler.doHandleRequest(allocator, readRecordsRequest, outputStream);
                return;
            case "UserDefinedFunctionRequest":
                UserDefinedFunctionRequest userDefinedFunctionRequest = (UserDefinedFunctionRequest) ProtobufSerDe.buildFromJson(inputJson, UserDefinedFunctionRequest.newBuilder());
                udfhandler.doHandleRequest(allocator, userDefinedFunctionRequest, outputStream);
                return;
            default:
                metadataHandler.doHandleRequest(allocator, typeHeader, inputJson, outputStream);
                return;
        }
    }
}
