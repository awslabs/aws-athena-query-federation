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
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BlockSerializationTest
{
    private static final Logger logger = LoggerFactory.getLogger(BlockSerializationTest.class);

    //We use two allocators to test moving data across allocators, some operations only fail when done across.
    //because of how Arrow does zero copy buffer reuse.
    private BlockAllocatorImpl allocator;
    private BlockAllocatorImpl otherAllocator;
    private ObjectMapper objectMapper;

    @Before
    public void setup()
    {
        otherAllocator = new BlockAllocatorImpl();
        allocator = new BlockAllocatorImpl();
        objectMapper = ObjectMapperFactory.create(allocator);
    }

    @After
    public void tearDown()
    {
        otherAllocator.close();
        allocator.close();
    }

    @Test
    public void serializationTest()
            throws IOException
    {
        logger.info("serializationTest - enter");

        Block expected = BlockUtils.newBlock(otherAllocator, "col1", Types.MinorType.INT.getType(), 21);

        ObjectMapper serializer = ObjectMapperFactory.create(new BlockAllocatorImpl());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeValue(out, expected);

        Block actual = serializer.readValue(new ByteArrayInputStream(out.toByteArray()), Block.class);

        assertEquals(expected, actual);

        logger.info("serializationTest - exit");
    }
}
