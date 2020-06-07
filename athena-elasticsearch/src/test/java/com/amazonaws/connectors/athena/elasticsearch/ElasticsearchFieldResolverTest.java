/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * This class is used to test the ElasticsearchFieldResolver class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchFieldResolverTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchFieldResolverTest.class);

    private Schema mapping;
    private Map<String, Object> document;
    private Map<String, Object> expectedResults = new HashMap<>();
    private ElasticsearchFieldResolver resolver = new ElasticsearchFieldResolver();

    @Before
    public void setUp()
            throws IOException
    {
        mapping = SchemaBuilder.newBuilder()
                .addField(new Field("mytext", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mytext",
                                FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))))
                .addField(new Field("myscaled", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("myscaled",
                                new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                        ImmutableMap.of("scaling_factor", "100")), null))))
                .addField(new Field("mylong", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mylong",
                                FieldType.nullable(Types.MinorType.BIGINT.getType()), null))))
                .addField(new Field("myint", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("myint",
                                FieldType.nullable(Types.MinorType.INT.getType()), null))))
                .addField(new Field("mysingle", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mysingle",
                                FieldType.nullable(Types.MinorType.INT.getType()), null))))
                .addField(new Field("myshort", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("myshort",
                                FieldType.nullable(Types.MinorType.SMALLINT.getType()), null))))
                .addField(new Field("mybyte", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mybyte",
                                FieldType.nullable(Types.MinorType.TINYINT.getType()), null))))
                .addField(new Field("mydouble", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mydouble",
                                FieldType.nullable(Types.MinorType.FLOAT8.getType()), null))))
                .addField(new Field("myfloat", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("myfloat",
                                FieldType.nullable(Types.MinorType.FLOAT4.getType()), null))))
                .addField(new Field("mydate", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mydate",
                                FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null))))
                .addField(new Field("myboolean", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("myboolean",
                                FieldType.nullable(Types.MinorType.BIT.getType()), null))))
                .build();

        document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"mytext\" :\n" +
                        "   [\n" +
                        "    \"My favorite Sci-Fi movie is Interstellar.\",\n" +
                        "    \"My favorite TV comedy show is Seinfeld.\"\n" +
                        "   ],\n" +
                        "  \"myscaled\" :\n" +
                        "   [\n" +
                        "    \"0.888\",\n" +
                        "    0.746\n" +
                        "   ],\n" +
                        "  \"mylong\" :\n" +
                        "   [\n" +
                        "    \"298347394.343\",\n" +
                        "    385793575.34\n" +
                        "   ],\n" +
                        "  \"myint\" :\n" +
                        "   [\n" +
                        "    \"384637.434\",\n" +
                        "    234734.44\n" +
                        "   ],\n" +
                        "  \"mysingle\" : 12345,\n" +
                        "  \"myshort\" :\n" +
                        "   [\n" +
                        "    \"234.5\",\n" +
                        "    344.2\n" +
                        "   ],\n" +
                        "  \"mybyte\" :\n" +
                        "   [\n" +
                        "    \"1.2\",\n" +
                        "    2.4\n" +
                        "   ],\n" +
                        "  \"mydouble\" :\n" +
                        "   [\n" +
                        "    \"10405624\",\n" +
                        "    143534543,\n" +
                        "    \"103434.8643\",\n" +
                        "    14353254.232\n" +
                        "   ],\n" +
                        "  \"myfloat\" :\n" +
                        "   [\n" +
                        "    \"10624\",\n" +
                        "    14543,\n" +
                        "    \"103.83\",\n" +
                        "    13354.32\n" +
                        "   ],\n" +
                        "  \"mydate\" :\n" +
                        "   [\n" +
                        "    \"2020-05-19T10:15:30.456789\",\n" +
                        "    \"2020-05-15T06:49:30.123-05:00\",\n" +
                        "    1589969730789\n" +
                        "   ],\n" +
                        "  \"myboolean\" :\n" +
                        "   [\n" +
                        "    \"true\",\n" +
                        "    \"false\",\n" +
                        "    true,\n" +
                        "    false\n" +
                        "   ]\n" +
                        "}\n", HashMap.class);

        expectedResults.put("mytext", ImmutableList.
                of("My favorite Sci-Fi movie is Interstellar.", "My favorite TV comedy show is Seinfeld."));
        expectedResults.put("myscaled", ImmutableList.of(new Long(89), new Long(75)));
        expectedResults.put("mylong", ImmutableList.of(new Long(298347394), new Long(385793575)));
        expectedResults.put("myint", ImmutableList.of(new Integer(384637), new Integer(234734)));
        expectedResults.put("mysingle", Collections.singletonList(new Integer(12345)));
        expectedResults.put("myshort", ImmutableList.of(new Short("234"), new Short("344")));
        expectedResults.put("mybyte", ImmutableList.of(new Byte("1"), new Byte("2")));
        expectedResults.put("mydouble", ImmutableList.of(new Double(10405624), new Double(143534543),
                new Double(103434.8643), new Double(14353254.232)));
        expectedResults.put("myfloat", ImmutableList.of(new Float(10624), new Float(14543),
                new Float(103.83), new Float(13354.32)));
        expectedResults.put("mydate", ImmutableList.of(new Long("1589883330457"), new Long("1589543370123"),
                new Long("1589969730789")));
        expectedResults.put("myboolean", ImmutableList.of(true, false, true, false));
    }

    /**
     * Test field resolver and type coercion.
     */
    @Test
    public void getFieldValueTest()
    {
        logger.info("getFieldValueTest - enter");

        Map<String, Object> extractedResults = new HashMap<>();

        for (Field field: mapping.getFields()) {
            extractedResults.put(field.getName(), resolver.getFieldValue(field, document));
            logger.info("Extracted: {}", extractedResults.get(field.getName()));
        }

        assertEquals("Extracted values mismatch!", expectedResults, extractedResults);

        logger.info("getFieldValueTest - exit");
    }
}
