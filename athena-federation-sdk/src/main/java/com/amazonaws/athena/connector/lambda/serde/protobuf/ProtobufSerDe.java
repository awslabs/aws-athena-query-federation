/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.protobuf;

import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class ProtobufSerDe
{
    private ProtobufSerDe()
    {
        // do nothing
    }
    public static final JsonFormat.Printer PROTOBUF_JSON_PRINTER = JsonFormat.printer().includingDefaultValueFields();
    public static final JsonFormat.Parser PROTOBUF_JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

    // find a better place for these later, but these are constants from the *Request and *Response objects that we want to preserve.
    public static final int UNLIMITED_PAGE_SIZE_VALUE = -1;

    private static final Logger logger = LoggerFactory.getLogger(ProtobufSerDe.class);
    
    public static Message buildFromJson(String inputJson, Message.Builder builder) throws InvalidProtocolBufferException
    {
        ProtobufSerDe.PROTOBUF_JSON_PARSER.merge(inputJson, builder);
        return builder.build();
    }

    /**
     * Write the serialized json to the outputstream. Also handles dealing with whatever compatibility tweaks need to be made to the default
     * proto message in order to stay backwards compatibile with the Jackson serdes.
     * @throws IOException
     */
    public static void writeResponse(Message responseMessage, OutputStream outputStream) throws IOException
    {
        String responseJson = null;
        if (responseMessage instanceof GetSplitsResponse) {
            responseJson = ProtobufCompatibilityLayer.buildGetSplitsResponseWithNullToken((GetSplitsResponse) responseMessage);
        }
        else if (responseMessage instanceof ListTablesResponse) {
            responseJson = ProtobufCompatibilityLayer.buildListTablesResponseWithNullToken((ListTablesResponse) responseMessage);
        }
        else {
            responseJson = ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(responseMessage);
        }
        logger.debug("Response json - {}", responseJson);
        outputStream.write(responseJson.getBytes());
    }
}
