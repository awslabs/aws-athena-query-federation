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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.v24.V24SerDeProvider;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ObjectMapperUtil
{
    private static final Logger logger = LoggerFactory.getLogger(ObjectMapperUtil.class);

    private static final JsonFactory jsonFactory = new JsonFactory();
    private static final V24SerDeProvider serDeProvider = new V24SerDeProvider();

    private static final boolean failOnObjectMapperChecks = false;

    private ObjectMapperUtil() {}

    public static <T> void assertSerialization(Object object, BlockAllocator allocator)
    {
        try {
            DelegatingSerDe serDe;
            if (object instanceof FederationRequest) {
                serDe = serDeProvider.getFederationRequestSerDe(allocator);
            }
            else if (object instanceof FederationResponse) {
                serDe = serDeProvider.getFederationResponseSerDe(allocator);
            }
            else {
                throw new IllegalArgumentException(object.getClass() + " is not handled");
            }

            // check SerDe write, SerDe read
            ByteArrayOutputStream serDeOut = new ByteArrayOutputStream();
            JsonGenerator jgen = jsonFactory.createGenerator(serDeOut);
            serDe.serialize(jgen, object);
            jgen.close();
            byte[] serDeOutput = serDeOut.toByteArray();
            JsonParser jparser = jsonFactory.createParser(new ByteArrayInputStream(serDeOutput));
            assertEquals(object, serDe.deserialize(jparser));

            // TODO remove when ObjectMapper is deprecated
            ObjectMapper mapper = ObjectMapperFactory.create(allocator);
            ByteArrayOutputStream mapperOut = new ByteArrayOutputStream();
            mapper.writeValue(mapperOut, object);
            byte[] mapperOutput = mapperOut.toByteArray();
            // also check ObjectMapper write, SerDe read compatibility
            jparser = jsonFactory.createParser(mapperOutput);
            try {
                assertEquals(object, serDe.deserialize(jparser));
            }
            catch (Exception e) {
                if (failOnObjectMapperChecks) {
                    throw e;
                }
                logger.warn("Object serialized with ObjectMapper not deserializable with SerDe", e);
            }
            // also check SerDe write, ObjectMapper read compatibility
            try {
                assertEquals(object, mapper.readValue(serDeOutput, object.getClass()));
            }
            catch (Exception e) {
                if (failOnObjectMapperChecks) {
                    throw e;
                }
                logger.warn("Object serialized with SerDe not deserializable with ObjectMapper", e);
            }
        }
        catch (IOException | AssertionError ex) {
            throw new RuntimeException(ex);
        }
    }
}
