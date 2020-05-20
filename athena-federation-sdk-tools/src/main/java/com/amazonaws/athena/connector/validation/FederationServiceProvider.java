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

import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaFunction;
import com.amazonaws.services.lambda.invoke.LambdaFunctionNameResolver;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactoryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.amazonaws.athena.connector.lambda.handlers.SerDeVersion.SERDE_VERSION;
import static com.amazonaws.athena.connector.validation.ConnectorValidator.BLOCK_ALLOCATOR;

public class FederationServiceProvider
{
    private static final Logger log = LoggerFactory.getLogger(FederationServiceProvider.class);

    private static final String VALIDATION_SUFFIX = "_validation";

    private static final Map<String, FederationService> serviceCache = new ConcurrentHashMap<>();

    private FederationServiceProvider()
    {
        // Intentionally left blank.
    }

    public static FederationService getService(String lambdaFunction, FederatedIdentity identity, String catalog)
    {
        FederationService service = serviceCache.get(lambdaFunction);
        if (service != null) {
            return service;
        }

        service = LambdaInvokerFactory.builder()
                .lambdaClient(AWSLambdaClientBuilder.defaultClient())
                .objectMapper(VersionedObjectMapperFactory.create(BLOCK_ALLOCATOR))
                .lambdaFunctionNameResolver(new Mapper(lambdaFunction))
                .build(FederationService.class);

        PingRequest pingRequest = new PingRequest(identity, catalog, generateQueryId());
        PingResponse pingResponse = (PingResponse) service.call(pingRequest);

        int actualSerDeVersion = pingResponse.getSerDeVersion();
        log.info("SerDe version for function {}, catalog {} is {}", lambdaFunction, catalog, actualSerDeVersion);

        if (actualSerDeVersion != SERDE_VERSION) {
            service = LambdaInvokerFactory.builder()
                    .lambdaClient(AWSLambdaClientBuilder.defaultClient())
                    .objectMapper(VersionedObjectMapperFactory.create(BLOCK_ALLOCATOR, actualSerDeVersion))
                    .lambdaFunctionNameResolver(new Mapper(lambdaFunction))
                    .build(FederationService.class);
        }

        serviceCache.put(lambdaFunction, service);
        return service;
    }

    public static final class Mapper
            implements LambdaFunctionNameResolver
    {
        private final String function;

        private Mapper(String function)
        {
            this.function = function;
        }

        @Override
        public String getFunctionName(Method method, LambdaFunction lambdaFunction,
                LambdaInvokerFactoryConfig lambdaInvokerFactoryConfig)
        {
            return function;
        }
    }

    public static String generateQueryId()
    {
        return UUID.randomUUID().toString() + VALIDATION_SUFFIX;
    }
}
