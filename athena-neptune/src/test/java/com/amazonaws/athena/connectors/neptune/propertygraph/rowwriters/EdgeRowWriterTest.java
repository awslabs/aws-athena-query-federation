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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.arrow.vector.complex.reader.FieldReader;
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
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder();
        for (org.apache.arrow.vector.types.pojo.Field f : schema.getFields()) {
            EdgeRowWriter.writeRowTemplate(builder, f, configOptions());
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
        Schema schema = SchemaBuilder.newBuilder().addStringField("id").build();
        Map<String, Object> context = edgeContextWithIdInOut("edge-1", "v-in", "v-out");

        Object result = writeAndReadOneRow(schema, "id", context);
        assertNotNull(result);
        assertEquals("edge-1", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_specialKeyIn_writesInVertexId() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("in").build();
        Map<String, Object> context = edgeContextWithIdInOut("e2", "vertex-in-id", "vertex-out-id");

        Object result = writeAndReadOneRow(schema, "in", context);
        assertNotNull(result);
        assertEquals("vertex-in-id", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_specialKeyOut_writesOutVertexId() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("out").build();
        Map<String, Object> context = edgeContextWithIdInOut("e3", "v1", "v2");

        Object result = writeAndReadOneRow(schema, "out", context);
        assertNotNull(result);
        assertEquals("v2", result.toString());
    }

    // ---- VARCHAR: valueMap list, scalar, Map, null first, multi-value ----

    @Test
    public void writeRowTemplate_varchar_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("name").build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add("alice");
        context.put("name", list);

        Object result = writeAndReadOneRow(schema, "name", context);
        assertNotNull(result);
        assertEquals("alice", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_scalarString_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("name").build();
        Map<String, Object> context = new HashMap<>();
        context.put("name", "bob");

        Object result = writeAndReadOneRow(schema, "name", context);
        assertNotNull(result);
        assertEquals("bob", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_mapFromByValueMap_writesStringRepresentation() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("props").build();
        Map<String, Object> context = new HashMap<>();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "two");
        context.put("props", map);

        Object result = writeAndReadOneRow(schema, "props", context);
        assertNotNull(result);
        String str = result.toString();
        assertTrue(str.contains("a=1"));
        assertTrue(str.contains("b=two"));
    }

    @Test
    public void writeRowTemplate_varchar_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("name").build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(null);
        context.put("name", list);

        Object result = writeAndReadOneRow(schema, "name", context);
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_varchar_multipleValuesWithNull_joinsWithoutNpe() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("tags").build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add("x");
        list.add(null);
        list.add("z");
        context.put("tags", list);

        Object result = writeAndReadOneRow(schema, "tags", context);
        assertNotNull(result);
        assertEquals("x;;z", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_nullField_doesNotSetValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("name").build();
        Map<String, Object> context = new HashMap<>();
        context.put("name", null);

        Object result = writeAndReadOneRow(schema, "name", context);
        assertNull(result);
    }

    // ---- BIT ----

    @Test
    public void writeRowTemplate_bit_valueMapListTrue_writesOne() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addBitField("flag").build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(true);
        context.put("flag", list);

        Object result = writeAndReadOneRow(schema, "flag", context);
        assertNotNull(result);
        assertTrue((Boolean) result);
    }

    @Test
    public void writeRowTemplate_bit_scalarTrue_writesOne() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addBitField("flag").build();
        Map<String, Object> context = new HashMap<>();
        context.put("flag", true);

        Object result = writeAndReadOneRow(schema, "flag", context);
        assertNotNull(result);
        assertTrue((Boolean) result);
    }

    @Test
    public void writeRowTemplate_datemilli_longEpoch_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addDateMilliField("timestamp").build();
        Map<String, Object> context = new HashMap<>();
        context.put("timestamp", 5000L);

        Object result = writeAndReadOneRow(schema, "timestamp", context);
        assertNotNull(result);
        assertEquals(5000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_datemilli_dateInstance_writesEpochMillis() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addDateMilliField("timestamp").build();
        Date d = new Date(6000L);
        Map<String, Object> context = new HashMap<>();
        context.put("timestamp", d);

        Object result = writeAndReadOneRow(schema, "timestamp", context);
        assertNotNull(result);
        assertEquals(6000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_datemilli_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addDateMilliField("timestamp").build();
        ArrayList<Object> list = new ArrayList<>();
        list.add(7000L);
        Map<String, Object> context = new HashMap<>();
        context.put("timestamp", list);

        Object result = writeAndReadOneRow(schema, "timestamp", context);
        assertNotNull(result);
        assertEquals(7000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_int_scalar_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addIntField("numberField").build();
        Map<String, Object> context = new HashMap<>();
        context.put("numberField", 99);

        Object result = writeAndReadOneRow(schema, "numberField", context);
        assertNotNull(result);
        assertEquals(99, ((Number) result).intValue());
    }

    @Test
    public void writeRowTemplate_int_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addIntField("numberField").build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(42);
        context.put("numberField", list);

        Object result = writeAndReadOneRow(schema, "numberField", context);
        assertNotNull(result);
        assertEquals(42, ((Number) result).intValue());
    }

    @Test
    public void writeRowTemplate_bigint_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addBigIntField("bigIntField").build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(999L);
        context.put("bigIntField", list);

        Object result = writeAndReadOneRow(schema, "bigIntField", context);
        assertNotNull(result);
        assertEquals(999L, ((Number) result).longValue());
    }

    @Test
    public void writeRowTemplate_float4_valueMapList_writesFirstElement() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addFloat4Field("floatField").build();
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(2.5f);
        context.put("floatField", list);

        Object result = writeAndReadOneRow(schema, "floatField", context);
        assertNotNull(result);
        assertEquals(2.5f, ((Number) result).floatValue(), 1e-6f);
    }

    @Test
    public void writeRowTemplate_float8_scalar_writesValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addFloat8Field("doubleField").build();
        Map<String, Object> context = new HashMap<>();
        context.put("doubleField", 1.5);

        Object result = writeAndReadOneRow(schema, "doubleField", context);
        assertNotNull(result);
        assertEquals(1.5, ((Number) result).doubleValue(), 1e-9);
    }
}
