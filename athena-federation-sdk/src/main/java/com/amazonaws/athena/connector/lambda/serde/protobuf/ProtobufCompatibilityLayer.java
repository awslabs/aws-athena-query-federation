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
import com.fasterxml.jackson.databind.node.ArrayNode;
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

    public static String lintMessageWithConstraints(Message message) throws JsonMappingException, JsonProcessingException, InvalidProtocolBufferException
    {
        // build JsonNode from proto json
        String messageJson = ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(message);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(messageJson);

        JsonNode constraintsNode = jsonNode.get("constraints");

        // clean summary map
        constraintsNode.get("summary").fields().forEachRemaining(ProtobufCompatibilityLayer::cleanSummaryEntry);
        
        // rewrite long type to long instead of string
        long longLimit = constraintsNode.get("limit").asLong(-1);
        ((ObjectNode) constraintsNode).put("limit", longLimit);

        // rewrite expression list to clean arrow type message and remove arguments array from variable/constant expressions.
        constraintsNode.get("expression").elements().forEachRemaining(ProtobufCompatibilityLayer::cleanExpressionElement);

        return jsonNode.toString();
    }

    /**
     * The proto message's "capabilities" field is a map that has entries which will look like this:
     * "supports_complex_expression_pushdown": {
            "optimiziationSubTypeList": [{
                "subType": "supported_function_expression_types",
                "properties": ["$add", "$subtract"]
            }]
        }
        * and we want to rewrite the entries to look like this:
        * "supports_complex_expression_pushdown" : [ {
            "subType" : "supported_function_expression_types",
            "properties" : [ "$add", "$subtract" ]
        } ]
    */
    public static String rewriteGetDataSourceCapabilitiesResponseMessageForJacksonFormat(Message message) throws JsonMappingException, JsonProcessingException, InvalidProtocolBufferException
    {
        // build JsonNode from proto json
        String messageJson = ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(message);
        
        // build JsonNode from proto json
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(messageJson);
         
        // Create a new object node to store the modified capabilities
        ObjectNode modifiedCapabilitiesNode = objectMapper.createObjectNode();

        // over each capability obj, copy the subtype list and rewrite without the wrapper obj
        jsonNode.get("capabilities").fields().forEachRemaining(entry -> {
            String optimizationName = entry.getKey();
            JsonNode optimizationNode = entry.getValue();
            ArrayNode subTypeList = objectMapper.createArrayNode();
            optimizationNode.get("optimiziationSubTypeList").elements().forEachRemaining(subTypeObj -> subTypeList.add(subTypeObj));
            modifiedCapabilitiesNode.set(optimizationName, subTypeList);
        });

        // then replace
        ((ObjectNode) jsonNode).set("capabilities", modifiedCapabilitiesNode);
        return jsonNode.toString();
    }

    /**
     * The input json we get from the Jackson serializer will have a capabilities entry which looks like this:
     * "supports_complex_expression_pushdown" : [ {
            "subType" : "supported_function_expression_types",
            "properties" : [ "$add", "$subtract" ]
        } ]
     *  but we need to make it look like this for proto's deserializer:
     * "supports_complex_expression_pushdown": {
            "optimiziationSubTypeList": [{
                "subType": "supported_function_expression_types",
                "properties": ["$add", "$subtract"]
            }]
        }
     * so we have to add a wrapper object around each capabilities object array field
     * @throws JsonProcessingException
     * @throws JsonMappingException
     */
    // TODO: obviously the work in this function and the previous one has the same boilerplate. abstract out a la the strategy method
    public static String rewriteGetDataSourceCapabilitiesResponseJsonForProtobufFormat(String inputJson) throws JsonMappingException, JsonProcessingException 
    {
        // build JsonNode from proto json
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(inputJson);

        // Create a new object node to store the modified capabilities
        ObjectNode modifiedCapabilitiesNode = objectMapper.createObjectNode();

        jsonNode.get("capabilities").fields().forEachRemaining(entry -> {
            String optimizationName = entry.getKey();
            ArrayNode subTypeList = (ArrayNode) entry.getValue();
            ObjectNode wrapperObj = objectMapper.createObjectNode();
            wrapperObj.set("optimiziationSubTypeList", subTypeList.deepCopy());
            modifiedCapabilitiesNode.set(optimizationName, wrapperObj);
        });

        // then replace
        ((ObjectNode) jsonNode).set("capabilities", modifiedCapabilitiesNode);
        return jsonNode.toString();
    }

    private static void cleanSummaryEntry(Entry<String, JsonNode> summaryMapEntry)
    {
        removeRangesFromSummaryEntry(summaryMapEntry.getValue());
        removeTypeIdsFromNonUnionTypeArrowTypes(summaryMapEntry.getValue());
    }

    // because we are forced to make a composed protobuf message for ValueSet and include default values in our serializer, all subclasses will have this ranges array.
    // But only SortedRangeSet should have it, so scrub it from the others.
    private static void removeRangesFromSummaryEntry(JsonNode node)
    {
        if (!node.get("@type").asText().equals("SortedRangeSet")) {
            ((ObjectNode) node).remove("ranges");
        }
    }

    //  because we are forced to make a composed arrow type message and include default values, every arrow type object in our proto message will have
    // an array 'typeIds' for the arrow Union type. So, we have to scrub it from the json if the arrow type is not the Union type.
    private static void removeTypeIdsFromNonUnionTypeArrowTypes(JsonNode node)
    {
        if (node.has("type")) { // not to be confused with @type
            ObjectNode arrowType = (ObjectNode) node.get("type");
            if (!arrowType.get("@type").asText().equals("Union")) {
                arrowType.remove("typeIds");
            }
        }
    }

    // because we are forced to make a composed FederationExpression proto message and include default values in our serializer, every federation expression
    // will have an arguments array, but only FunctionCallExpression should actually contain it. So, scrub it from the other subclasses before writing.
    private static void cleanExpressionElement(JsonNode node)
    {
        removeTypeIdsFromNonUnionTypeArrowTypes(node);
        if (!node.get("@type").asText().equals("FunctionCallExpression")) {
            ((ObjectNode) node).remove("arguments");
        }
        if (node.has("arguments")) {   
            // recursive call to clean functionexpression arguments
            node.get("arguments").elements().forEachRemaining(ProtobufCompatibilityLayer::cleanExpressionElement);
        }
    }
}
