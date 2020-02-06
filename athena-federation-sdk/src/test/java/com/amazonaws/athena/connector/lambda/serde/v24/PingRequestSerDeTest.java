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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.utils.TestUtils;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class PingRequestSerDeTest
{
    private static final Logger logger = LoggerFactory.getLogger(PingRequestSerDeTest.class);

    private TestUtils utils = new TestUtils();
    private JsonFactory jsonFactory = new JsonFactory();

    private V24SerDeProvider serDeProvider = new V24SerDeProvider();
    private PingRequestSerDe serde;

    private BlockAllocator allocator;

    private PingRequest expected;
    private String expectedSerDeText;

    @Before
    public void before()
            throws IOException
    {
        allocator = new BlockAllocatorImpl();

        serde = serDeProvider.getPingRequestSerDe();

        FederatedIdentity federatedIdentity = new FederatedIdentity("test-id", "test-principal", "0123456789");
        expected = new PingRequest(federatedIdentity, "test-catalog", "test-query-id");

        String expectedSerDeFile = utils.getResourceOrFail("serde/v24", "PingRequest.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void serialize()
            throws IOException
    {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonGenerator jgen = jsonFactory.createGenerator(outputStream);
        jgen.useDefaultPrettyPrinter();

        serde.serialize(jgen, expected);

        jgen.close();
        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("serialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);

        logger.info("serialize: exit");
    }

    @Test
    public void deserialize()
            throws IOException
    {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());
        JsonParser jparser = jsonFactory.createParser(input);

        PingRequest actual = (PingRequest) serde.deserialize(jparser);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }

    @Test
    public void delegateSerialize()
            throws IOException
    {
        logger.info("delegateSerialize: enter");
        FederationRequestSerDe federationRequestSerDe = serDeProvider.getFederationRequestSerDe(allocator);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonGenerator jgen = jsonFactory.createGenerator(outputStream);
        jgen.useDefaultPrettyPrinter();

        federationRequestSerDe.serialize(jgen, expected);

        jgen.close();
        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("delegateSerialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);

        logger.info("delegateSerialize: exit");
    }

    @Test
    public void delegateDeserialize()
            throws IOException
    {
        logger.info("delegateDeserialize: enter");
        FederationRequestSerDe federationRequestSerDe = serDeProvider.getFederationRequestSerDe(allocator);
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());
        JsonParser jparser = jsonFactory.createParser(input);

        PingRequest actual = (PingRequest) federationRequestSerDe.deserialize(jparser);

        logger.info("delegateDeserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("delegateDeserialize: exit");
    }
}
