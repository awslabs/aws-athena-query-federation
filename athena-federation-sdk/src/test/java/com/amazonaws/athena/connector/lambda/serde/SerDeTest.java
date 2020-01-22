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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.serde.v24.FederationRequestSerDe;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SerDeTest
{
    private static final Logger logger = LoggerFactory.getLogger(SerDeTest.class);

    private BlockAllocatorImpl allocator;
    private ObjectMapper mapper;
    private JsonFactory jsonFactory;

    @Before
    public void before()
    {
        allocator = new BlockAllocatorImpl();
        mapper = ObjectMapperFactory.create(allocator);
        jsonFactory = new JsonFactory(mapper);
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void test()
            throws Exception
    {
        logger.info("doListSchemas - enter");
        ListSchemasRequest req = new ListSchemasRequest(IdentityUtil.fakeIdentity(), "queryId", "default");

//        assertSerialization(req, new FederationRequestSerDe(mapper, allocator));
    }

    @Test
    public void test2()
            throws Exception
    {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("day");
        partitionCols.add("month");
        partitionCols.add("year");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        BlockAllocatorImpl allocator = new BlockAllocatorImpl();
        constraintsMap.put("day", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 20)), false));

        constraintsMap.put("month", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 2)), false));

        constraintsMap.put("year", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1900)), false));

        GetTableLayoutRequest req = null;
        GetTableLayoutResponse res = null;
        try {

            req = new GetTableLayoutRequest(IdentityUtil.fakeIdentity(), "queryId", "default",
                    new TableName("schema1", "table1"),
                    new Constraints(constraintsMap),
                    tableSchema,
                    partitionCols);
//            assertSerialization(req, new FederationRequestSerDe(mapper, allocator));
        }
        finally {
            req.close();
        }
    }

    private void assertSerialization(MetadataRequest metadataRequest, BaseSerDe object)
    {
        Object actual = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonGenerator jgen = jsonFactory.createGenerator(out);
            object.serialize(jgen, metadataRequest);
            jgen.close();
            logger.info("ouput: " + new String(out.toByteArray()));
            JsonParser jparser = jsonFactory.createParser(new ByteArrayInputStream(out.toByteArray()));
            actual = object.deserialize(jparser);
            jparser.close();
            assertEquals(metadataRequest, actual);
        }
        catch (IOException | AssertionError ex) {
            throw new RuntimeException(ex);
        }
    }
}
