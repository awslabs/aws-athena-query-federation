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

import com.amazonaws.athena.connector.lambda.proto.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.proto.request.PingRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProtobufSerDe
{
    private ProtobufSerDe()
    {
        // do nothing
    }

    /**
     * The JsonFormat.printer() is a little tricky becuase of it's behavior. By default, it will opt to not write key/value pairs if
     * either they were not set or if the value we assign to the field is equivalent to what Protobuf considers that type's default value to be.
     * So for example, if the correct value for a string field is the empty string "", Protobuf default behavior is to omit this
     * (with the logic being that any protobuf deserializer, upon retrieving the message, will set the field to "" anyway).
     * 
     * However because we are dealing with compatibility with our Jackson SerDes, we are forced to have all key/value pairs present, and
     * no additional key/value pairs present.
     * 
     * So, we have to add the flag `includingDefaultValueFields()` to allow these to write. However, this introduces a new caveat, as the 
     * defined behavior is: "Creates a new JsonFormat.Printer that will also print fields set to their defaults. Empty repeated fields and map fields will be printed as well."
     * 
     * This introduces complications due to how we need to create "uber-messages" to conform to the inheritance structure the Jackson SerDes are based off of.
     *  This means part of the serialization work is to strip away certain fields that were added that do not belong. This is a rarity, but is relevant for 
     * ArrowTypeMessage.proto (repeated int32 typeIds, which is only meant to be used for the Union type) and for ValueSet.proto (repeated Range ranges, which is just for SortedRangeSet). The messages which contain these are GetSplitsRequest, GetTableLayoutRequest, and ReadRecordsRequest (which all
     * have a Constraints value, which has a map where the values are ValueSet objects, and only ValueSet has the ArrowTypeMessage, and only for
     * SortedRangeSet and AllOrNoneValueSet).
     */
    public static final JsonFormat.Printer PROTOBUF_JSON_PRINTER = JsonFormat.printer().includingDefaultValueFields();
    public static final JsonFormat.Parser PROTOBUF_JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

    // find a better place for these later, but these are constants from the *Request and *Response objects that we want to preserve.
    public static final int UNLIMITED_PAGE_SIZE_VALUE = -1;

    private static final Logger logger = LoggerFactory.getLogger(ProtobufSerDe.class);
    
    public static Message buildFromJson(String inputJson, Message.Builder builder) throws InvalidProtocolBufferException, JsonMappingException, JsonProcessingException
    {
        if (builder instanceof GetDataSourceCapabilitiesResponse.Builder) {
            // need to rewrite to ADD the surrounding object.
            inputJson = ProtobufCompatibilityLayer.rewriteGetDataSourceCapabilitiesResponseJsonForProtobufFormat(inputJson);
        }
        ProtobufSerDe.PROTOBUF_JSON_PARSER.merge(inputJson, builder);
        return builder.build();
    }

    /**
     * Return the message as json. Also handles dealing with whatever compatibility tweaks need to be made to the default
     * proto message in order to stay backwards compatibile with the Jackson serdes.
     * @throws IOException
     */
    public static String writeMessageToJson(Message message) throws IOException
    {
        String jsonMessage = null;
        if (message instanceof GetSplitsResponse) {
            jsonMessage = ProtobufCompatibilityLayer.buildGetSplitsResponseWithNullToken((GetSplitsResponse) message);
        }
        else if (message instanceof ListTablesResponse) {
            jsonMessage = ProtobufCompatibilityLayer.buildListTablesResponseWithNullToken((ListTablesResponse) message);
        }
        else if (message instanceof PingRequest) {
            jsonMessage = ProtobufCompatibilityLayer.buildPingRequestWithDeprecatedIdentityFields((PingRequest) message);
        }
        else if (message instanceof ReadRecordsRequest || message instanceof GetTableLayoutRequest || message instanceof GetSplitsRequest) {
            jsonMessage = ProtobufCompatibilityLayer.lintMessageWithConstraints(message);
        }
        else if (message instanceof GetDataSourceCapabilitiesResponse) {
            // need to rewrite to REMOVE the surrounding object
            jsonMessage = ProtobufCompatibilityLayer.rewriteGetDataSourceCapabilitiesResponseMessageForJacksonFormat(message);
        }
        else {
            jsonMessage = ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(message);
        }

        logger.debug("json - {}", jsonMessage);
        return jsonMessage;
    }
}
