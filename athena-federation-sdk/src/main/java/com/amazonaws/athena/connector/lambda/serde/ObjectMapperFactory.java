package com.amazonaws.athena.connector.lambda.serde;

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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.serde.v2.ObjectMapperFactoryV2;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Used to construct an ObjectMapper instead that is capable of handling some of our special objects (Apache Arrow Block,
 * and Schema).
 *
 * @deprecated replaced with {@link ObjectMapperFactoryV2}
 */
@Deprecated
public class ObjectMapperFactory
{
    private ObjectMapperFactory()
    {
    }

    public static ObjectMapper create(BlockAllocator allocator)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Schema.class, new SchemaSerializer());
        module.addDeserializer(Schema.class, new SchemaDeserializer());
        module.addDeserializer(Block.class, new BlockDeserializer(allocator));
        module.addSerializer(Block.class, new BlockSerializer());

        //todo provide a block serializer instead of batch serializer but only serialize the batch not the schema.
        objectMapper.registerModule(module)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        return objectMapper;
    }
}
