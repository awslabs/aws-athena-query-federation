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
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.proto.request.PingRequest;
import com.amazonaws.athena.connector.lambda.proto.request.PingResponse;
import com.amazonaws.athena.connector.lambda.proto.request.TypeHeader;
import com.amazonaws.athena.connector.lambda.records.RecordRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
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
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            // TODO: if inputStream can be processed as a protobuf message, call the protobuf handleRequest method instead.
            byte[] allInputBytes = inputStream.readAllBytes();
            try {
                String inputJson = new String(allInputBytes, StandardCharsets.UTF_8);
                logger.warn("Incoming bytes, converted to string: {}", inputJson);
                TypeHeader.Builder typeHeaderBuilder = TypeHeader.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(inputJson, typeHeaderBuilder);
                TypeHeader typeHeader = typeHeaderBuilder.build();

                // in the existing logic, we were attching the Lambda Context variable to the metadata request, which is messy.
                // For now, I'll stick it in the config options with the fields we care about only.
                metadataHandler.configOptions.put(MetadataHandler.FUNCTION_ARN_CONFIG_KEY, context.getInvokedFunctionArn());

                handleRequest(allocator, typeHeader, inputJson, outputStream);
                return; // if protobuf was successful, stop
            } 
            catch (Exception e) {
                // could not parse as a protobuf message
                logger.error("Encounted problem reading in json as a protobuf message. Exception is {}. Will try with fallback jackson serde", e);
            }

            ObjectMapper objectMapper = VersionedObjectMapperFactory.create(allocator);
            try (FederationRequest rawReq = objectMapper.readValue(allInputBytes, FederationRequest.class)) {
                if (rawReq instanceof MetadataRequest) {
                    ((MetadataRequest) rawReq).setContext(context);
                }
                handleRequest(allocator, rawReq, outputStream, objectMapper);
            }
            catch (IllegalStateException e) { // if client has not upgraded to our latest, fallback to v2
                objectMapper = VersionedObjectMapperFactory.create(allocator, 2);
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

    // Needed to handle the checked exceptions from the protobuf parseFrom method
    // private AbstractMessage uncheckedWrapper(Callable<AbstractMessage> callable)
    // {
    //     try {
    //         return callable.call();
    //     }
    //     catch (Exception ex) {
    //         throw new RuntimeException(ex);
    //     }
    // }

    /**
     * For protobuf
     */
    public final void handleRequest(BlockAllocator allocator, TypeHeader typeHeader, String inputJson, OutputStream outputStream)
            throws Exception
    {
        String type = typeHeader.getType();
        switch(type) {
            case "PingRequest":
                PingRequest.Builder pingBuilder = PingRequest.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(inputJson, pingBuilder);
                PingResponse response = metadataHandler.doPing(pingBuilder.build());
                String outputJson = JsonFormat.printer().print(response);
                outputStream.write(outputJson.getBytes());
                logger.info("PingResponse - {}", outputJson);
                return;
            case "ReadRecordsRequest":
                ReadRecordsRequest.Builder readRecordsRequestBuilder = ReadRecordsRequest.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(inputJson, readRecordsRequestBuilder);
                recordHandler.doHandleRequest(allocator, readRecordsRequestBuilder.build(), outputStream);
                return;
            default:
                metadataHandler.doHandleRequest(allocator, typeHeader, inputJson, outputStream);
                return;
        }
        // Map<String, Function<byte[], AbstractMessage>> parserMap = Map.of(
        //     "@GetSplitsRequest", (byte[] in) -> uncheckedWrapper(() -> GetSplitsRequest.parseFrom(in)) 
        // );
        // AbstractMessage msg = parserMap.get(type).apply(inputBytes);
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
}
