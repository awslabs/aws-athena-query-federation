package com.amazonaws.athena.connector.lambda.serde;

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
