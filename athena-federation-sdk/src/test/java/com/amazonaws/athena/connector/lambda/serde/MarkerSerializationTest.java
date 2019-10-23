package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
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

import static org.junit.Assert.*;

public class MarkerSerializationTest
{
    private static final Logger logger = LoggerFactory.getLogger(MarkerSerializationTest.class);
    private BlockAllocatorImpl allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void serializationTest()
            throws IOException
    {
        logger.info("serializationTest - enter");

        ObjectMapper serializer = ObjectMapperFactory.create(new BlockAllocatorImpl());

        int expectedValue = 1024;
        Marker expectedMarker = Marker.exactly(allocator, Types.MinorType.INT.getType(), expectedValue);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeValue(out, expectedMarker);

        ObjectMapper deserializer = ObjectMapperFactory.create(allocator);

        Marker actualMarker = deserializer.readValue(new ByteArrayInputStream(out.toByteArray()), Marker.class);

        assertEquals(expectedMarker.getSchema().getCustomMetadata(), actualMarker.getSchema().getCustomMetadata());
        assertEquals(expectedMarker.getSchema().getFields(), actualMarker.getSchema().getFields());
        assertEquals(expectedMarker.getBound(), actualMarker.getBound());
        assertEquals(expectedMarker.getValue(), actualMarker.getValue());
        assertEquals(expectedValue, actualMarker.getValue());

        logger.info("serializationTest - exit");
    }
}
