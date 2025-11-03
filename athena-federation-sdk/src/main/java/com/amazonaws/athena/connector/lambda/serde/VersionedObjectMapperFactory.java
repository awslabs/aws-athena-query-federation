/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.SerDeVersion;
import com.amazonaws.athena.connector.lambda.serde.v2.ObjectMapperFactoryV2;
import com.amazonaws.athena.connector.lambda.serde.v3.ObjectMapperFactoryV3;
import com.amazonaws.athena.connector.lambda.serde.v4.ObjectMapperFactoryV4;
import com.amazonaws.athena.connector.lambda.serde.v5.ObjectMapperFactoryV5;
import com.amazonaws.athena.connector.lambda.serde.v6.ObjectMapperFactoryV6;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

/**
 * Vends {@link ObjectMapper} instances that correspond to SerDe versions.
 */
public class VersionedObjectMapperFactory
{
    private static final Logger logger = LoggerFactory.getLogger(VersionedObjectMapperFactory.class);
    private VersionedObjectMapperFactory() {}

    /**
     * Creates an {@link ObjectMapper} using the current SDK SerDe version.
     *
     * @param allocator
     * @return
     */
    public static ObjectMapper create(BlockAllocator allocator)
    {
        return create(allocator, SerDeVersion.SERDE_VERSION);
    }

    /**
     * Creates an {@link ObjectMapper} using the provided SerDe version.
     *
     * @param allocator
     * @param version
     * @return
     */
    public static ObjectMapper create(BlockAllocator allocator, int version)
    {
        logger.debug("Athena SDK SerDe Version: " + version);
        switch (version) {
            case 1:
                return ObjectMapperFactory.create(allocator);
            case 2:
                return ObjectMapperFactoryV2.create(allocator);
            case 3:
                return ObjectMapperFactoryV3.create(allocator);
            case 4:
                return ObjectMapperFactoryV4.create(allocator);
            case 5:
                return ObjectMapperFactoryV5.create(allocator);
            case 6:
                return ObjectMapperFactoryV6.create(allocator);
            default:
                throw new AthenaConnectorException("No serde version " + version, ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
    }
}
