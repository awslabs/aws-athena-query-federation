/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.validation;

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.amazonaws.athena.connector.lambda.handlers.SerDeVersion.SERDE_VERSION;
import static com.amazonaws.athena.connector.validation.ConnectorValidator.BLOCK_ALLOCATOR;

public class FederationServiceProvider
{
    private static final Logger log = LoggerFactory.getLogger(FederationServiceProvider.class);

    private static final String VALIDATION_SUFFIX = "_validation";

    private static final Map<String, Integer> serdeVersionCache = new ConcurrentHashMap<>();

    private static final LambdaClient lambdaClient = LambdaClient.create();

    private FederationServiceProvider()
    {
        // Intentionally left blank.
    }

    private static <T, R> R invokeFunction(String lambdaFunction, T request, Class<R> responseClass, ObjectMapper objectMapper)
    {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(request);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize request object", e);
        }

        InvokeRequest invokeRequest = InvokeRequest.builder()
                .functionName(lambdaFunction)
                .payload(SdkBytes.fromUtf8String(payload))
                .build();

        InvokeResponse invokeResponse = lambdaClient.invoke(invokeRequest);

        String response = invokeResponse.payload().asUtf8String();
        try {
            return objectMapper.readValue(response, responseClass);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to deserialize response payload", e);
        }
    }

    public static FederationResponse callService(String lambdaFunction, FederatedIdentity identity, String catalog, FederationRequest request)
    {
        int serDeVersion = SERDE_VERSION;
        if (serdeVersionCache.containsKey(lambdaFunction)) {
            serDeVersion = serdeVersionCache.get(lambdaFunction);
        }
        else {
            ObjectMapper objectMapper = VersionedObjectMapperFactory.create(BLOCK_ALLOCATOR);
            PingRequest pingRequest = new PingRequest(identity, catalog, generateQueryId());
            PingResponse pingResponse = invokeFunction(lambdaFunction, pingRequest, PingResponse.class, objectMapper);
    
            int actualSerDeVersion = pingResponse.getSerDeVersion();
            log.info("SerDe version for function {}, catalog {} is {}", lambdaFunction, catalog, actualSerDeVersion);
            serdeVersionCache.put(lambdaFunction, actualSerDeVersion);
        }

        return invokeFunction(lambdaFunction, request, FederationResponse.class, VersionedObjectMapperFactory.create(BLOCK_ALLOCATOR, serDeVersion));
    }

    public static String generateQueryId()
    {
        return UUID.randomUUID().toString() + VALIDATION_SUFFIX;
    }
}
