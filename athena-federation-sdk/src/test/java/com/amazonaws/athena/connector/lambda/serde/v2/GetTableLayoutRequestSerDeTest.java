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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDeTest;
import com.fasterxml.jackson.core.JsonEncoding;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GetTableLayoutRequestSerDeTest extends TypedSerDeTest<FederationRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(GetTableLayoutRequestSerDeTest.class);

    @Before
    public void beforeTest()
            throws IOException
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("year", new ArrowType.Int(32, true))
                .addField("month", new ArrowType.Int(32, true))
                .addField("day", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .addField("col3", Types.MinorType.FLOAT8.getType())
                .addField("col4", Types.MinorType.FLOAT8.getType())
                .addField("col5", Types.MinorType.FLOAT8.getType())
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), -10000D)), false));
        constraintsMap.put("col4", EquatableValueSet.newBuilder(allocator, Types.MinorType.FLOAT8.getType(), false, true).add(1.1D).build());
        constraintsMap.put("col5", new AllOrNoneValueSet(Types.MinorType.FLOAT8.getType(), false, true));
        Constraints constraints = new Constraints(constraintsMap);

        expected = new GetTableLayoutRequest(federatedIdentity,
                "test-query-id",
                "test-catalog",
                new TableName("test-schema", "test-table"),
                constraints,
                schema,
                ImmutableSet.of("year", "month", "day"));

        String expectedSerDeFile = utils.getResourceOrFail("serde/v2", "GetTableLayoutRequest.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    @Test
    public void serialize()
            throws Exception
    {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        mapper.writeValue(outputStream, expected);

        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("serialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);
        expected.close();

        logger.info("serialize: exit");
    }

    @Test
    public void deserialize()
            throws IOException
    {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        GetTableLayoutRequest actual = (GetTableLayoutRequest) mapper.readValue(input, FederationRequest.class);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }
}
