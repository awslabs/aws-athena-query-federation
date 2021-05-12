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

import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.fasterxml.jackson.core.JsonEncoding;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class PingResponseSerDeTest extends TypedSerDeTest<FederationResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PingResponseSerDeTest.class);

    @Before
    public void beforeTest()
            throws IOException
    {
        expected = new PingResponse("test-catalog", "test-query-id", "test-source-type", 23, 2);

        String expectedSerDeFile = utils.getResourceOrFail("serde", "PingResponse.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    @Test
    public void serialize()
            throws IOException
    {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        mapper.writeValue(outputStream, expected);

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

        PingResponse actual = (PingResponse) mapper.readValue(input, FederationResponse.class);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }

    @Test
    public void testBackwardsAndForwardsCompatibility()
    {
        ObjectMapperUtil.assertSerialization(expected);
    }

    @Test
    public void testForwardsCompatibility()
            throws IOException
    {
        logger.info("testForwardsCompatibility: enter");
        String expectedSerDeFile = utils.getResourceOrFail("serde", "PingResponseForwardsCompatible.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        PingResponse actual = (PingResponse) mapper.readValue(input, FederationResponse.class);

        logger.info("testForwardsCompatibility: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("testForwardsCompatibility: exit");
    }
}
