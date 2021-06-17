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
package com.amazonaws.athena.connector.lambda.serde.v3;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.io.IOException;

import static com.amazonaws.athena.connector.lambda.utils.TestUtils.SERDE_VERSION_THREE;
import static com.amazonaws.athena.connector.lambda.utils.TestUtils.SERDE_VERSION_TWO;

public class ObjectMapperFactoryV3Test
{
    @Test(expected = JsonMappingException.class)
    public void testStrictSerializer()
            throws JsonProcessingException
    {
        try (BlockAllocator allocator = new BlockAllocatorImpl()) {
            ObjectMapper mapper = VersionedObjectMapperFactory.create(allocator, SERDE_VERSION_THREE);
            mapper.writeValueAsString(new ArrowType.Null());
        }
    }

    @Test(expected = JsonMappingException.class)
    public void testStrictDeserializer()
            throws IOException
    {
        try (BlockAllocator allocator = new BlockAllocatorImpl()) {
            ObjectMapper mapper = VersionedObjectMapperFactory.create(allocator, SERDE_VERSION_THREE);
            mapper.readValue("{\"@type\" : \"FloatingPoint\", \"precision\" : \"DOUBLE\"}", ArrowType.FloatingPoint.class);
        }
    }
}
