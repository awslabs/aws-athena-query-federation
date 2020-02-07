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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.SerDeTest;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
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

public class ReadRecordsResponseSerDeTest extends SerDeTest<FederationResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(ReadRecordsResponseSerDeTest.class);

    private V24SerDeProvider serDeProvider = new V24SerDeProvider();

    @Before
    public void beforeTest()
            throws IOException
    {
        allocator = new BlockAllocatorImpl("test-allocator-id");

        serde = serDeProvider.getReadRecordsResponseSerDe(allocator);

        String yearCol = "year";
        String monthCol = "month";
        String dayCol = "day";

        Schema schema = SchemaBuilder.newBuilder()
                .addField(yearCol, new ArrowType.Int(32, true))
                .addField(monthCol, new ArrowType.Int(32, true))
                .addField(dayCol, new ArrowType.Int(32, true))
                .build();

        Block records = allocator.createBlock(schema);
        int num_records = 10;
        for (int i = 0; i < num_records; i++) {
            BlockUtils.setValue(records.getFieldVector(yearCol), i, 2016 + i);
            BlockUtils.setValue(records.getFieldVector(monthCol), i, (i % 12) + 1);
            BlockUtils.setValue(records.getFieldVector(dayCol), i, (i % 28) + 1);
        }
        records.setRowCount(num_records);

        expected = new ReadRecordsResponse("test-catalog", records);

        String expectedSerDeFile = utils.getResourceOrFail("serde/v24", "ReadRecordsResponse.json");
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

        ReadRecordsResponse actual = (ReadRecordsResponse) serde.deserialize(jparser);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }

    @Test
    public void delegateSerialize()
            throws IOException
    {
        logger.info("delegateSerialize: enter");
        FederationResponseSerDe federationResponseSerDe = serDeProvider.getFederationResponseSerDe(allocator);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonGenerator jgen = jsonFactory.createGenerator(outputStream);
        jgen.useDefaultPrettyPrinter();

        federationResponseSerDe.serialize(jgen, expected);

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
        FederationResponseSerDe federationResponseSerDe = serDeProvider.getFederationResponseSerDe(allocator);
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());
        JsonParser jparser = jsonFactory.createParser(input);

        ReadRecordsResponse actual = (ReadRecordsResponse) federationResponseSerDe.deserialize(jparser);

        logger.info("delegateDeserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("delegateDeserialize: exit");
    }
}
