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
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.neptune.Constants;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the changed logic in EdgeRowWriter: FieldValueNormalizer usage,
 * valueMap vs project().by() (list vs scalar vs Map), VARCHAR null-first-element handling,
 * and special keys (id, in, out).
 */
@RunWith(MockitoJUnitRunner.class)
public class EdgeRowWriterTest
{
    private BlockAllocatorImpl allocator;
    
    public static final String TEST_FIELD = "testField";
    
    // Field names for edge-context extractor tests (moved from RowWritersComprehensiveTest)
    private static final String EMPTY_FIELD = "emptyField";
    private static final String TESTFIELD = "TESTFIELD";
    private static final String TESTFIELD_LOWER = "testfield";
    private static final String TEST_VALUE = "Test Value";
    private static final String SPACES = "   ";
    
    // Configuration values
    private static final String CASE_INSENSITIVE_TRUE = "true";
    private static final String CASE_INSENSITIVE_FALSE = "false";
    
    @Mock
    private RowWriterBuilder mockRowWriterBuilder;
    
    @Mock
    private Field mockField;
    
    private Map<String, String> configOptions;
    private Map<String, Object> edgeContext;
    
    
    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);
        when(mockField.getName()).thenReturn(TEST_FIELD);
        configOptions = new HashMap<>();
        edgeContext = new HashMap<>();
        edgeContext.put(T.id.toString(), "edge456");
        edgeContext.put(T.label.toString(), "knows");
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
    
    @Test
    public void writeRowTemplate_WithCaseInsensitiveBool_createsBitExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Bool.INSTANCE);
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);
        
        BitExtractor capturedExtractor = captureExtractorAfterWrite(mockField, BitExtractor.class, TEST_FIELD);
        assertNotNull("BitExtractor should not be null", capturedExtractor);
        
        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, Boolean.TRUE);
        NullableBitHolder holder = new NullableBitHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
        
        context.put(TEST_FIELD, "false");
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0, holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithCaseInsensitiveConfigAndUtf8Type_createsVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);
        
        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
        
        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "edge-value");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("edge-value", holder.value);
        
        context.put(TEST_FIELD, 12345);
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("12345", holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithCaseInsensitiveDisabledAndUtf8Type_createsVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_FALSE);
        
        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
        
        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "case-sensitive-key-only");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("case-sensitive-key-only", holder.value);
    }
    @Test
    public void writeRowTemplate_WithNullCaseInsensitiveConfigAndUtf8Type_createsVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        
        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
        
        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "default-config");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("default-config", holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithEmptyConfigOptionsAndUtf8Type_createsVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        
        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
        
        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "empty-config-options");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("empty-config-options", holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithCaseSensitiveConfigAndUtf8Type_doesNotMatchDifferentCaseKey() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_FALSE);
        edgeContext.put(TESTFIELD_LOWER, TEST_VALUE);
        
        VarCharExtractor extractor = captureExtractorAfterWrite(edgeFieldUtf8(TESTFIELD), VarCharExtractor.class, TESTFIELD);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(0, holder.isSet);
    }
    
    @Test
    public void writeRowTemplate_WithCaseInsensitiveConfigAndBigIntType_createsBigIntExtractor() throws Exception
    {
        mockFieldArrowType(new ArrowType.Int(64, true));
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);
        
        BigIntExtractor capturedExtractor = captureExtractorAfterWrite(mockField, BigIntExtractor.class, TEST_FIELD);
        assertNotNull("BigIntExtractor should not be null", capturedExtractor);
        
        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "9876543210");
        NullableBigIntHolder holder = new NullableBigIntHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(9876543210L, holder.value);
        
        context.put(TEST_FIELD, 42L);
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(42L, holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithCaseInsensitiveConfigAndFloat4Type_createsFloat4Extractor() throws Exception
    {
        mockFieldArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);
        
        Float4Extractor capturedExtractor = captureExtractorAfterWrite(mockField, Float4Extractor.class, TEST_FIELD);
        assertNotNull("Float4Extractor should not be null", capturedExtractor);
        
        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "2.5");
        NullableFloat4Holder holder = new NullableFloat4Holder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.5f, holder.value, 0.0001f);
        
        context.put(TEST_FIELD, 1.25f);
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1.25f, holder.value, 0.0001f);
    }
    
    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullConfigOptionsAndUtf8Type_throwsNullPointerException()
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, null);
    }
    
    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullRowWriterBuilderAndUtf8Type_throwsNullPointerException()
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        
        EdgeRowWriter.writeRowTemplate(null, mockField, configOptions);
    }
    
    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullField_throwsNullPointerException()
    {
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, null, configOptions);
    }
    
    @Test
    public void writeRowTemplate_WithEdgeContextAndSpacesInBitField_handlesUnset() throws Exception
    {
        edgeContext.put(EMPTY_FIELD, SPACES);
        
        BitExtractor extractor = captureExtractorAfterWrite(edgeFieldBool(EMPTY_FIELD), BitExtractor.class, EMPTY_FIELD);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(0, holder.isSet);
    }
    
    private void setupSchemaCaseInsensitive(String caseInsensitiveValue)
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, caseInsensitiveValue);
    }
    
    private void mockFieldArrowType(ArrowType arrowType)
    {
        when(mockField.getType()).thenReturn(arrowType);
    }
    
    private <E extends Extractor> E captureExtractorAfterWrite(Field field, Class<E> extractorType, String registeredFieldName)
    {
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<E> captor = ArgumentCaptor.forClass(extractorType);
        verify(mockRowWriterBuilder).withExtractor(eq(registeredFieldName), captor.capture());
        return captor.getValue();
    }
    
    private Field edgeFieldBool(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Bool()), null);
    }
    
    private Field edgeFieldUtf8(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }
    

}
