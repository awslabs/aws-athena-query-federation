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
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the changed logic in CustomSchemaRowWriter: FieldValueNormalizer usage,
 * valueMap vs project().by() (list vs scalar vs Map), and VARCHAR null-first-element handling.
 */
public class CustomSchemaRowWriterTest
{
    private static final String FLAG = "flag";
    private static final String TIMESTAMP = "timestamp";
    private static final String NUMBER_FIELD = "numberField";
    private static final String BIG_INT_FIELD = "bigIntField";
    private static final String FLOAT_FIELD = "floatField";
    private static final String DOUBLE_FIELD = "doubleField";
    private static final String NAME = "name";
    private static final String TAGS = "tags";
    private static final String ALL_PROPERTIES = "all_properties";
    private static final String ID = "id";

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
            CustomSchemaRowWriter.writeRowTemplate(builder, f, configOptions());
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

    @Test
    public void writeRowTemplate_varchar_valueMapList_writesFirstElement() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(NAME), NAME, contextWith(NAME, listWith("alice")));
        assertNotNull(result);
        assertEquals("alice", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_scalarString_writesValue() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(NAME), NAME, contextWith(NAME, "bob"));
        assertNotNull(result);
        assertEquals("bob", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_mapFromByValueMap_writesStringRepresentation() throws Exception
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("k1", "v1");
        map.put("k2", 2);
        Object result = writeAndReadOneRow(varcharSchema(ALL_PROPERTIES), ALL_PROPERTIES, contextWith(ALL_PROPERTIES, map));
        assertNotNull(result);
        String str = result.toString();
        assertTrue(str.contains("k1=v1"));
        assertTrue(str.contains("k2=2"));
    }

    @Test
    public void writeRowTemplate_varchar_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(NAME), NAME, contextWith(NAME, listWith(null)));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_varchar_multipleValues_joinsWithSemicolon() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(TAGS), TAGS, contextWith(TAGS, listWith("a", "b", "c")));
        assertNotNull(result);
        assertEquals("a;b;c", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_multipleValuesWithNull_joinsWithoutNpe() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(TAGS), TAGS, contextWith(TAGS, listWith("a", null, "c")));
        assertNotNull(result);
        assertEquals("a;;c", result.toString());
    }

    @Test
    public void writeRowTemplate_varchar_emptyList_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(NAME), NAME, contextWith(NAME, Collections.emptyList()));
        assertNull(result);
    }

    // ---- BIT ----

    @Test
    public void writeRowTemplate_bit_valueMapListTrue_writesOne() throws Exception
    {
        Object result = writeAndReadOneRow(bitSchema(), FLAG, contextWith(FLAG,listWith(true)));
        assertNotNull(result);
        assertTrue((Boolean) result);
    }

    @Test
    public void writeRowTemplate_bit_scalarTrue_writesOne() throws Exception
    {
        Object result = writeAndReadOneRow(bitSchema(), FLAG, contextWith(FLAG,true));
        assertNotNull(result);
        assertTrue((Boolean) result);
    }

    @Test
    public void writeRowTemplate_bit_scalarFalse_writesZero() throws Exception
    {
        Object result = writeAndReadOneRow(bitSchema(), FLAG, contextWith(FLAG,false));
        assertNotNull(result);
        assertEquals(Boolean.FALSE, result);
    }

    @Test
    public void writeRowTemplate_bit_nullField_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bitSchema(), FLAG, contextWith(FLAG,null));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_bit_emptyList_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bitSchema(), FLAG, contextWith(FLAG,Collections.emptyList()));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_bit_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bitSchema(), FLAG, contextWith(FLAG,listWith(null)));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_bit_listWithBlankStringFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bitSchema(), FLAG, contextWith(FLAG,listWith("   ")));
        assertNull(result);
    }

    // ---- DATEMILLI ----

    @Test
    public void writeRowTemplate_datemilli_dateInstance_writesEpochMillis() throws Exception
    {
        Object result = writeAndReadOneRow(datemilliSchema(), TIMESTAMP, contextWith(TIMESTAMP,new Date(1000L)));
        assertNotNull(result);
        assertEquals(1000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_datemilli_longEpoch_writesValue() throws Exception
    {
        Object result = writeAndReadOneRow(datemilliSchema(), TIMESTAMP, contextWith(TIMESTAMP,2000L));
        assertNotNull(result);
        assertEquals(2000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_datemilli_valueMapList_writesFirstElement() throws Exception
    {
        Object result = writeAndReadOneRow(datemilliSchema(), TIMESTAMP, contextWith(TIMESTAMP,listWith(new Date(3000L))));
        assertNotNull(result);
        assertEquals(3000L, ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void writeRowTemplate_datemilli_nullField_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(datemilliSchema(), TIMESTAMP, contextWith(TIMESTAMP,null));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_datemilli_emptyList_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(datemilliSchema(), TIMESTAMP, contextWith(TIMESTAMP,Collections.emptyList()));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_datemilli_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(datemilliSchema(), TIMESTAMP, contextWith(TIMESTAMP,listWith(null)));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_datemilli_listWithBlankStringFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(datemilliSchema(), TIMESTAMP, contextWith(TIMESTAMP,listWith("   ")));
        assertNull(result);
    }

    // ---- INT, BIGINT, FLOAT4, FLOAT8 ----

    @Test
    public void writeRowTemplate_int_scalar_writesValue() throws Exception
    {
        Object result = writeAndReadOneRow(intSchema(), NUMBER_FIELD, contextWith(NUMBER_FIELD,42));
        assertNotNull(result);
        assertEquals(42, ((Number) result).intValue());
    }

    @Test
    public void writeRowTemplate_int_valueMapList_writesFirstElement() throws Exception
    {
        Object result = writeAndReadOneRow(intSchema(), NUMBER_FIELD, contextWith(NUMBER_FIELD,listWith(100)));
        assertNotNull(result);
        assertEquals(100, ((Number) result).intValue());
    }

    @Test
    public void writeRowTemplate_int_nullField_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(intSchema(), NUMBER_FIELD, contextWith(NUMBER_FIELD,null));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_int_emptyList_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(intSchema(), NUMBER_FIELD, contextWith(NUMBER_FIELD,Collections.emptyList()));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_int_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(intSchema(), NUMBER_FIELD, contextWith(NUMBER_FIELD,listWith(null)));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_int_listWithBlankStringFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(intSchema(), NUMBER_FIELD, contextWith(NUMBER_FIELD,listWith("   ")));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_bigint_scalar_writesValue() throws Exception
    {
        Object result = writeAndReadOneRow(bigintSchema(), BIG_INT_FIELD, contextWith(BIG_INT_FIELD,12345L));
        assertNotNull(result);
        assertEquals(12345L, ((Number) result).longValue());
    }

    @Test
    public void writeRowTemplate_bigint_nullField_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bigintSchema(), BIG_INT_FIELD, contextWith(BIG_INT_FIELD,null));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_bigint_emptyList_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bigintSchema(), BIG_INT_FIELD, contextWith(BIG_INT_FIELD,Collections.emptyList()));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_bigint_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bigintSchema(), BIG_INT_FIELD, contextWith(BIG_INT_FIELD,listWith(null)));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_bigint_listWithBlankStringFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(bigintSchema(), BIG_INT_FIELD, contextWith(BIG_INT_FIELD,listWith("   ")));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float4_scalar_writesValue() throws Exception
    {
        Object result = writeAndReadOneRow(float4Schema(), FLOAT_FIELD, contextWith(FLOAT_FIELD,1.5f));
        assertNotNull(result);
        assertEquals(1.5f, ((Number) result).floatValue(), 1e-6f);
    }

    @Test
    public void writeRowTemplate_float4_nullField_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float4Schema(), FLOAT_FIELD, contextWith(FLOAT_FIELD,null));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float4_emptyList_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float4Schema(), FLOAT_FIELD, contextWith(FLOAT_FIELD,Collections.emptyList()));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float4_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float4Schema(), FLOAT_FIELD, contextWith(FLOAT_FIELD,listWith(null)));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float4_listWithBlankStringFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float4Schema(), FLOAT_FIELD, contextWith(FLOAT_FIELD,listWith("   ")));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float8_valueMapList_writesFirstElement() throws Exception
    {
        Object result = writeAndReadOneRow(float8Schema(), DOUBLE_FIELD, contextWith(DOUBLE_FIELD,listWith(3.14)));
        assertNotNull(result);
        assertEquals(3.14, ((Number) result).doubleValue(), 1e-9);
    }

    @Test
    public void writeRowTemplate_float8_nullField_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float8Schema(), DOUBLE_FIELD, contextWith(DOUBLE_FIELD,null));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float8_emptyList_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float8Schema(), DOUBLE_FIELD, contextWith(DOUBLE_FIELD,Collections.emptyList()));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float8_listWithNullFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float8Schema(), DOUBLE_FIELD, contextWith(DOUBLE_FIELD,listWith(null)));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_float8_listWithBlankStringFirstElement_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(float8Schema(), DOUBLE_FIELD, contextWith(DOUBLE_FIELD,listWith("   ")));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_varchar_nullField_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(NAME), NAME, contextWith(NAME, null));
        assertNull(result);
    }

    @Test
    public void writeRowTemplate_varchar_specialKeyId_nullId_doesNotSetValue() throws Exception
    {
        Object result = writeAndReadOneRow(varcharSchema(ID), ID, contextWith("ID", null));
        assertNull(result);
    }

    private static Map<String, Object> contextWith(String fieldName, Object value)
    {
        Map<String, Object> context = new HashMap<>();
        context.put(fieldName, value);
        return context;
    }

    private static List<Object> listWith(Object... elements)
    {
        if (elements == null) {
            return Collections.singletonList(null);
        }
        List<Object> list = new ArrayList<>();
        Collections.addAll(list, elements);
        return list;
    }

    private static Schema bitSchema()
    {
        return SchemaBuilder.newBuilder().addBitField(FLAG).build();
    }

    private static Schema datemilliSchema()
    {
        return SchemaBuilder.newBuilder().addDateMilliField(TIMESTAMP).build();
    }

    private static Schema intSchema()
    {
        return SchemaBuilder.newBuilder().addIntField(NUMBER_FIELD).build();
    }

    private static Schema bigintSchema()
    {
        return SchemaBuilder.newBuilder().addBigIntField(BIG_INT_FIELD).build();
    }

    private static Schema float4Schema()
    {
        return SchemaBuilder.newBuilder().addFloat4Field(FLOAT_FIELD).build();
    }

    private static Schema float8Schema()
    {
        return SchemaBuilder.newBuilder().addFloat8Field(DOUBLE_FIELD).build();
    }

    private static Schema varcharSchema(String fieldName)
    {
        return SchemaBuilder.newBuilder().addStringField(fieldName).build();
    }
}
