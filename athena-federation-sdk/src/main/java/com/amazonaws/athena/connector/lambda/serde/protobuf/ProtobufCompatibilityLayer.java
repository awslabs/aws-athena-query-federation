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
import com.amazonaws.athena.connector.lambda.proto.request.PingRequest;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProtobufCompatibilityLayer
{
    private ProtobufCompatibilityLayer()
    {
        // do nothing
    }   
    static Pattern emptyStringValuePattern = Pattern.compile("(\"\\w+\": )(\"\")");

    /**
     * The ListTablesResponse has a field, `nextToken`, which serves as a continuation token.
     * The existing serde enforces that it must write a value of null if it is not set, which
     * violates Protobuf's behavior. Because we cannot set a field to null on a protobuf message,
     * we have to do manually inject null.
     * @throws InvalidProtocolBufferException
     */
    public static String buildListTablesResponseWithNullToken(ListTablesResponse listTablesResponse) throws InvalidProtocolBufferException
    {
        if (!listTablesResponse.hasNextToken()) {
            listTablesResponse = listTablesResponse.toBuilder().setNextToken("").build();
        }
        String listTablesResponseJson = ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(listTablesResponse);
        return replaceEmptyStringPatternIfPresent(listTablesResponseJson);        
    }
    public static String buildGetSplitsResponseWithNullToken(GetSplitsResponse getSplitsResponse) throws InvalidProtocolBufferException
    {
        if (!getSplitsResponse.hasContinuationToken()) {
            getSplitsResponse = getSplitsResponse.toBuilder().setContinuationToken("").build();
        }
        String getSplitsResponseJson = ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(getSplitsResponse);
        return replaceEmptyStringPatternIfPresent(getSplitsResponseJson);
    }

    private static String replaceEmptyStringPatternIfPresent(String inputJson) 
    {
        Matcher nextTokenMatcher = emptyStringValuePattern.matcher(inputJson);
        if (nextTokenMatcher.find()) {
            return nextTokenMatcher.replaceAll("$1null");
        }
        return inputJson;
    }

    public static String buildPingRequestWithDeprecatedIdentityFields(PingRequest pingRequest) throws InvalidProtocolBufferException
    {
        FederatedIdentity identity = pingRequest.getIdentity();
        pingRequest = pingRequest.toBuilder().setIdentity(identity.toBuilder().setId("UNKNOWN").build()).build();
        return ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(pingRequest);
    }

    public static String lintMessageWithSummaryMap(Message message) throws JsonMappingException, JsonProcessingException, InvalidProtocolBufferException
    {
        String messageJson = ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(message);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(messageJson);
        jsonNode.get("constraints").get("summary").fields().forEachRemaining(ProtobufCompatibilityLayer::cleanSummaryEntry);
        return jsonNode.toString();
    }

    private static void cleanSummaryEntry(Entry<String, JsonNode> summaryMapEntry)
    {
        removeRangesFromSummaryEntry(summaryMapEntry);
        removeTypeIdsFromNonUnionTypeArrowTypes(summaryMapEntry);
    }

    private static void removeRangesFromSummaryEntry(Entry<String, JsonNode> summaryMapEntry)
    {
        if (!summaryMapEntry.getValue().get("@type").asText().equals("SortedRangeSet")) {
            ((ObjectNode) summaryMapEntry.getValue()).remove("ranges");
        }
    }

    private static void removeTypeIdsFromNonUnionTypeArrowTypes(Entry<String, JsonNode> summaryMapEntry)
    {
        if (summaryMapEntry.getValue().has("type")) { // not to be confused with @type
            ObjectNode arrowType = (ObjectNode) summaryMapEntry.getValue().get("type");
            if (!arrowType.get("@type").asText().equals("Union")) {
                arrowType.remove("typeIds");
            }
        }
    }
}
