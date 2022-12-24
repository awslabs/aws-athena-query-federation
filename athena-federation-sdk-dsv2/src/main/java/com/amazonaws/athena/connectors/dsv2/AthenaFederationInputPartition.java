/*-
 * #%L
 * athena-federation-sdk-dsv2
 * %%
 * Copyright (C) 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.dsv2;

import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.connector.read.InputPartition;

public class AthenaFederationInputPartition implements InputPartition
{
    final String readRecordsRequestJsonString;

    private AthenaFederationInputPartition(String readRecordsRequestJsonString)
    {
        this.readRecordsRequestJsonString = readRecordsRequestJsonString;
    }

    public static AthenaFederationInputPartition fromReadRecordsRequest(ReadRecordsRequest request, ObjectMapper objectMapper) throws JsonProcessingException
    {
        return new AthenaFederationInputPartition(objectMapper.writeValueAsString(request));
    }

    public ReadRecordsRequest toReadRecordsRequest(ObjectMapper objectMapper) throws JsonProcessingException
    {
        return (ReadRecordsRequest) objectMapper.readValue(readRecordsRequestJsonString, FederationRequest.class);
    }
}
