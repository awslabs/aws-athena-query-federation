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
        assertEquals(false, actualMarker.isNullValue());

        logger.info("serializationTest - exit");
    }

    @Test
    public void nullableSerializationTest()
            throws IOException
    {
        logger.info("nullableSerializationTest - enter");

        ObjectMapper serializer = ObjectMapperFactory.create(new BlockAllocatorImpl());
        Marker expectedMarker = Marker.nullMarker(allocator, Types.MinorType.INT.getType());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeValue(out, expectedMarker);

        ObjectMapper deserializer = ObjectMapperFactory.create(allocator);

        Marker actualMarker = deserializer.readValue(new ByteArrayInputStream(out.toByteArray()), Marker.class);

        assertEquals(expectedMarker.getSchema().getCustomMetadata(), actualMarker.getSchema().getCustomMetadata());
        assertEquals(expectedMarker.getSchema().getFields(), actualMarker.getSchema().getFields());
        assertEquals(expectedMarker.getBound(), actualMarker.getBound());
        assertEquals(true, actualMarker.isNullValue());

        logger.info("nullableSerializationTest - exit");
    }
}
