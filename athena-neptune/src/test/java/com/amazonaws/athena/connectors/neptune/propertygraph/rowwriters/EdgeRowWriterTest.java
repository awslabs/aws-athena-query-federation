/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connectors.neptune.Constants;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the changed logic in EdgeRowWriter: FieldValueNormalizer usage,
 * valueMap vs project().by() (list vs scalar vs Map), VARCHAR null-first-element handling,
 * and special keys (id, in, out).
 */
public class EdgeRowWriterTest
{
    private static final String ID_FIELD = "id";
    private static final String IN_FIELD = "in";
    private static final String OUT_FIELD = "out";
    private static final String NAME = "name";
    private static final String NAME_UPPER = "NAME";
    private static final String TAGS = "tags";
    private static final String FLAG = "flag";
    private static final String TIMESTAMP = "timestamp";
    private static final String NUMBER_FIELD = "numberField";
    private static final String BIG_INT_FIELD = "bigIntField";
    private static final String FLOAT_FIELD = "floatField";
    private static final String DOUBLE_FIELD = "doubleField";
    private static final String PROPS = "props";
    private static final String ALICE = "alice";
    private static final String BOB = "bob";

    private BlockAllocatorImpl allocator;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        if (allocator != null) {
            allocator.close();
        }
    }

    private Map<String, String> configOptions()
    {
        return Collections.emptyMap();
    }

    private Object writeAndReadOneRow(Schema schema, String fieldName, Map<String, Object> context) throws Exception
    {
        return writeAndReadOneRow(schema, fieldName, context, configOptions());
    }

    private Object writeAndReadOneRow(
            Schema schema,
            String fieldName,
            Map<String, Object> context,
            Map<String, String> configOptions) throws Exception
    {
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder();
        for (Field f : schema.getFields()) {
            EdgeRowWriter.writeRowTemplate(builder, f, configOptions);
        }
        GeneratedRowWriter rowWriter = builder.build();
        try (Block block = allocator.createBlock(schema)) {
            assertTrue(rowWriter.writeRow(block, 0, context));
            block.setRowCount(1);
            FieldReader reader = block.getFieldReaders().stream()
                    .filter(r -> r.getField().getName().equals(fieldName))
                    .findFirst()
                    .orElseThrow();
            reader.setPosition(0);
            if (!reader.isSet()) {
                return null;
            }
            return reader.readObject();
        }
    }

    /**
     * Builds an edge-like context with T.id, Direction.IN, Direction.OUT so that
     * EdgeRowWriter.contextAsMap puts ID, IN, OUT into the map for special key tests.
     * Keys are T.id, T.label, Direction.IN, Direction.OUT (Gremlin tokens), not strings.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> edgeContextWithIdInOut(String edgeId, String inVertexId, String outVertexId)
    {
        Map<Object, Object> ctx = new HashMap<>();
        ctx.put(T.id, edgeId);
        ctx.put(T.label, "e");
        LinkedHashMap<Object, Object> inMap = new LinkedHashMap<>();
        inMap.put(T.id, inVertexId);
        ctx.put(Direction.IN, inMap);
        LinkedHashMap<Object, Object> outMap = new LinkedHashMap<>();
        outMap.put(T.id, outVertexId);
        ctx.put(Direction.OUT, outMap);
        return (Map<String, Object>) (Map<?, ?>) ctx;
    }

    @Test
    public void writeRowTemplate_varchar_specialKeyId_writesEdgeId() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(ID_FIELD).build();
        Map<String, Object> context = edgeContextWithIdInOut("edge-1", "v-in", "v-out");

        Object result = writeAndReadOneRow(schema, ID_FIELD, context);
        assertNotNull(result);
        assertEquals("edge-1", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_specialKeyIn_writesInVertexId() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(IN_FIELD).build();
        Map<String, Object> context = edgeContextWithIdInOut("e2", "vertex-in-id", "vertex-out-id");

        Object result = writeAndReadOneRow(schema, IN_FIELD, context);
        assertNotNull(result);
        assertEquals("vertex-in-id", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_specialKeyOut_writesOutVertexId() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(OUT_FIELD).build();
        Map<String, Object> context = edgeContextWithIdInOut("e3", "v1", "v2");

        Object result = writeAndReadOneRow(schema, OUT_FIELD, context);
        assertNotNull(result);
        assertEquals("v2", result.toString());
    }

    // ---- VARCHAR: valueMap list, scalar, Map, null first, multi-value ----

    @Test
    public void writeRowTemplate_varchar_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(NAME).build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(ALICE);
        context.put(NAME, list);

        Object result = writeAndReadOneRow(schema, NAME, context);
        assertNotNull(result);
        assertEquals(ALICE, result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_scalarString_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(NAME).build();
        Map<String, Object> context = new HashMap<>();
        context.put(NAME, BOB);

        Object result = writeAndReadOneRow(schema, NAME, context);
        assertNotNull(result);
        assertEquals(BOB, result.toString());
    }

    @Test
    public void writeRowTemplate_withDefaultCaseInsensitive_resolvesMixedCaseKey() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(NAME).build();
        Map<String, Object> context = new HashMap<>();
        context.put(NAME_UPPER, ALICE);

        Object result = writeAndReadOneRow(schema, NAME, context);

        assertNotNull(result);
        assertEquals(ALICE, result.toString());
    }

    @Test
    public void writeRowTemplate_withCaseSensitiveConfig_doesNotMatchDifferentCaseKey() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(NAME).build();
        Map<String, Object> context = new HashMap<>();
        context.put(NAME_UPPER, ALICE);
        Map<String, String> config = Collections.singletonMap(Constants.SCHEMA_CASE_INSEN, "false");

        Object result = writeAndReadOneRow(schema, NAME, context, config);

        assertNull(result);
    }

    @Test
    public void writeRowTemplate_varchar_mapFromByValueMap_writesStringRepresentation() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(PROPS).build();
        Map<String, Object> context = new HashMap<>();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "two");
        context.put(PROPS, map);

        Object result = writeAndReadOneRow(schema, PROPS, context);
        assertNotNull(result);
        String str = result.toString();
        assertTrue(str.contains("a=1"));
        assertTrue(str.contains("b=two"));
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_withNullRowWriterBuilder_throwsNullPointerException()
    {
        Field field = SchemaBuilder.newBuilder().addStringField(NAME).build().findField(NAME);
        EdgeRowWriter.writeRowTemplate(null, field, configOptions());
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_withNullField_throwsNullPointerException()
    {
        EdgeRowWriter.writeRowTemplate(
                GeneratedRowWriter.newBuilder(), null, configOptions());
    }

    @Test
    public void writeRowTemplate_varchar_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(NAME).build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(null);
        context.put(NAME, list);

        Object result = writeAndReadOneRow(schema, NAME, context);
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_varchar_multipleValuesWithNull_joinsWithoutNpe() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(TAGS).build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add("x");
        list.add(null);
        list.add("z");
        context.put(TAGS, list);

        Object result = writeAndReadOneRow(schema, TAGS, context);
        assertNotNull(result);
        assertEquals("x;;z", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_nullField_doesNotSetValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField(NAME).build();
        Map<String, Object> context = new HashMap<>();
        context.put(NAME, null);

        Object result = writeAndReadOneRow(schema, NAME, context);
        assertNull(result);
    }

    // ---- BIT ----

    @Test
    public void writeRowTemplate_bit_valueMapListTrue_writesOne() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addBitField(FLAG).build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(true);
        context.put(FLAG, list);

        Object result = writeAndReadOneRow(schema, FLAG, context);
        assertNotNull(result);
        assertTrue((Boolean) result);
    }

    @Test
    public void writeRowTemplate_bit_scalarTrue_writesOne() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addBitField(FLAG).build();
        Map<String, Object> context = new HashMap<>();
        context.put(FLAG, true);

        Object result = writeAndReadOneRow(schema, FLAG, context);
        assertNotNull(result);
        assertTrue((Boolean) result);
    }

    @Test
    public void writeRowTemplate_bit_blankString_doesNotSetValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addBitField(FLAG).build();
        Map<String, Object> context = new HashMap<>();
        context.put(FLAG, "   ");

        Object result = writeAndReadOneRow(schema, FLAG, context);
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_datemilli_longEpoch_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addDateMilliField(TIMESTAMP).build();
        Map<String, Object> context = new HashMap<>();
        context.put(TIMESTAMP, 5000L);

        Object result = writeAndReadOneRow(schema, TIMESTAMP, context);
        assertNotNull(result);
        assertEquals(5000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_datemilli_dateInstance_writesEpochMillis() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addDateMilliField(TIMESTAMP).build();
        Date d = new Date(6000L);
        Map<String, Object> context = new HashMap<>();
        context.put(TIMESTAMP, d);

        Object result = writeAndReadOneRow(schema, TIMESTAMP, context);
        assertNotNull(result);
        assertEquals(6000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_datemilli_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addDateMilliField(TIMESTAMP).build();
        ArrayList<Object> list = new ArrayList<>();
        list.add(7000L);
        Map<String, Object> context = new HashMap<>();
        context.put(TIMESTAMP, list);

        Object result = writeAndReadOneRow(schema, TIMESTAMP, context);
        assertNotNull(result);
        assertEquals(7000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_int_scalar_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addIntField(NUMBER_FIELD).build();
        Map<String, Object> context = new HashMap<>();
        context.put(NUMBER_FIELD, 99);

        Object result = writeAndReadOneRow(schema, NUMBER_FIELD, context);
        assertNotNull(result);
        assertEquals(99, ((Number) result).intValue());
    }

    @Test
    public void writeRowTemplate_int_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addIntField(NUMBER_FIELD).build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(42);
        context.put(NUMBER_FIELD, list);

        Object result = writeAndReadOneRow(schema, NUMBER_FIELD, context);
        assertNotNull(result);
        assertEquals(42, ((Number) result).intValue());
    }

    @Test
    public void writeRowTemplate_bigint_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addBigIntField(BIG_INT_FIELD).build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(999L);
        context.put(BIG_INT_FIELD, list);

        Object result = writeAndReadOneRow(schema, BIG_INT_FIELD, context);
        assertNotNull(result);
        assertEquals(999L, ((Number) result).longValue());
    }

    @Test
    public void writeRowTemplate_float4_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addFloat4Field(FLOAT_FIELD).build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(2.5f);
        context.put(FLOAT_FIELD, list);

        Object result = writeAndReadOneRow(schema, FLOAT_FIELD, context);
        assertNotNull(result);
        assertEquals(2.5f, ((Number) result).floatValue(), 1e-6f);
    }

    @Test
    public void writeRowTemplate_float8_scalar_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addFloat8Field(DOUBLE_FIELD).build();
        Map<String, Object> context = new HashMap<>();
        context.put(DOUBLE_FIELD, 1.5);

        Object result = writeAndReadOneRow(schema, DOUBLE_FIELD, context);
        assertNotNull(result);
        assertEquals(1.5, ((Number) result).doubleValue(), 1e-9);
    }
}
