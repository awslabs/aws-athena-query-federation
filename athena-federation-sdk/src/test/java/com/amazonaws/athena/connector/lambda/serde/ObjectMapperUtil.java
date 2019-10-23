package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ObjectMapperUtil
{
    private ObjectMapperUtil() {}

    public static <T> void assertSerialization(Object object, Class<T> clazz)
    {
        Object actual = null;
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            ObjectMapper mapper = ObjectMapperFactory.create(allocator);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            mapper.writeValue(out, object);
            actual = mapper.readValue(new ByteArrayInputStream(out.toByteArray()), clazz);
            assertEquals(object, actual);
        }
        catch (IOException | AssertionError ex) {
            throw new RuntimeException(ex);
        }
    }
}
