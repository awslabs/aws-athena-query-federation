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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.RecordBatchSerDe;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.serde.v24.AllOrNoneValueSetSerDe;
import com.amazonaws.athena.connector.lambda.serde.v24.BlockSerDe;
import com.amazonaws.athena.connector.lambda.serde.v24.SchemaSerDe;
import com.amazonaws.athena.connector.lambda.serde.v24.ValueSetSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

// TODO remove when ObjectMapper is no longer used
public class SerDeParityTest
{
    private static final Logger logger = LoggerFactory.getLogger(SerDeParityTest.class);

    private BlockAllocator allocator;
    private ObjectMapper objectMapper;

    @Before
    public void before()
    {
        allocator = new BlockAllocatorImpl();
        objectMapper = ObjectMapperFactory.create(allocator);
    }

    @After
    public void after()
    {
        allocator.close();
    }

//    @Test
//    public void testAllOrNoneValueSetParity()
//            throws IOException
//    {
//        AllOrNoneValueSet original = new AllOrNoneValueSet(ArrowType.Utf8.INSTANCE, true, false);
//        String objectMapperSerialized = objectMapper.writeValueAsString(original);
//        logger.info("objectMapperSerialized: " + objectMapperSerialized);
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        JsonGenerator jgen = objectMapper.getFactory().createGenerator(out);
////        AllOrNoneValueSetSerDe serDe = new AllOrNoneValueSetSerDe(objectMapper);
//        ValueSetSerDe serDe = new ValueSetSerDe(objectMapper, new BlockSerDe(allocator, objectMapper.getFactory(), new SchemaSerDe(), new RecordBatchSerDe(allocator)));
//        serDe.serialize(jgen, original);
//        jgen.close();
//        String serDeSerialized = out.toString();
//        logger.info("serDeSerialized: " + serDeSerialized);
//        assertEquals(objectMapperSerialized, serDeSerialized);
//        AllOrNoneValueSet objectMapperDeserialized = objectMapper.readValue(objectMapperSerialized, AllOrNoneValueSet.class);
//        ByteArrayInputStream in = new ByteArrayInputStream(serDeSerialized.getBytes());
//        JsonParser jparser = objectMapper.getFactory().createParser(in);
//        AllOrNoneValueSet serDeDeserialized = (AllOrNoneValueSet) serDe.deserialize(jparser);
//        assertEquals(objectMapperDeserialized, serDeDeserialized);
//    }
}
