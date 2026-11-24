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
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.Enums.SpecialKeys;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.tinkerpop.gremlin.structure.T;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the changed logic in VertexRowWriter: FieldValueNormalizer usage,
 * valueMap vs project().by() (list vs scalar vs Map), and VARCHAR null-first-element handling.
 */
@RunWith(MockitoJUnitRunner.class)
public class VertexRowWriterTest
{
    private BlockAllocatorImpl allocator;
    
    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final String NULL_FIELD = "nullField";
    public static final String EMPTY_FIELD = "emptyField";
    public static final String NAME = "name";
    public static final String MULTI_FIELD = "multiField";
    public static final String EMPTY_LIST = "emptyList";
    public static final String NAME_CAPS = "NAME";
    public static final String TESTFIELD = "TESTFIELD";

    // Test values
    private static final String VERTEX_123 = "vertex123";
    private static final String PERSON_LABEL = "person";
    private static final String JOHN_DOE = "John Doe";
    private static final String SEMICOLON_SEPARATED = ";";
    private static final String TEST_VALUE = "Test Value";
    private static final String SPACES = "   ";
    private static final String CASE_INSENSITIVE_TRUE = "true";
    private static final String CASE_INSENSITIVE_FALSE = "false";
    private static final String TESTFIELD_LOWER = "testfield";

    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> vertexContext;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);
        
        configOptions = new HashMap<>();
        vertexContext = new HashMap<>();
        vertexContext.put(T.id.toString(), VERTEX_123);
        vertexContext.put(T.label.toString(), PERSON_LABEL);
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
            VertexRowWriter.writeRowTemplate(builder, f, configOptions());
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
    public void writeRowTemplate_varchar_nullField_doesNotSetValue() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addStringField("name").build();
        Map<String, Object> context = new HashMap<>();
        context.put("name", null);

        Object result = writeAndReadOneRow(schema, "name", context);
        assertNull(result);
    }
    
    @Test
    public void writeRowTemplate_WithBitExtractorAndValidFalse_extractsCorrectValue() throws Exception
    {
        putVertexList(INACTIVE, false);
        BitExtractor extractor = captureExtractorAfterWrite(boolField(INACTIVE), BitExtractor.class);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0, holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithBitExtractorAndEmptyString_handlesEmptyString() throws Exception
    {
        putVertexList(EMPTY_FIELD, SPACES);
        BitExtractor extractor = captureExtractorAfterWrite(boolField(EMPTY_FIELD), BitExtractor.class);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }
    
    @Test
    public void writeRowTemplate_WithVarCharExtractorAndSpecialIDField_handlesSpecialIDField() throws Exception
    {
        Field idField = utf8Field(SpecialKeys.ID.toString().toLowerCase());
        VarCharExtractor extractor = captureExtractorAfterWrite(idField, VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VERTEX_123, holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithVarCharExtractorAndEmptyArrayList_handlesEmptyArrayList() throws Exception
    {
        putVertexList(EMPTY_LIST);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(EMPTY_LIST), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }
    
    @Test
    public void writeRowTemplate_WithContextAsMapAndCaseInsensitive_handlesCaseInsensitive() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);
        putVertexList(NAME, JOHN_DOE);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(NAME_CAPS), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(JOHN_DOE, holder.value);
    }
    
    @Test
    public void writeRowTemplate_WithContextAsMapAndCaseSensitive_handlesCaseSensitive() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_FALSE);
        putVertexList(NAME, JOHN_DOE);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(NAME_CAPS), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }
    
    @Test
    public void writeRowTemplate_WithContextAsMapAndDefaultCaseInsensitive_handlesDefaultCaseInsensitive() throws Exception
    {
        putVertexList(TESTFIELD_LOWER, TEST_VALUE);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(TESTFIELD), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(TEST_VALUE, holder.value);
    }
    
    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullRowWriterBuilder_throwsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(null, utf8Field(NAME), configOptions);
    }
    
    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullField_throwsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, null, configOptions);
    }
    
    private <E extends Extractor> E captureExtractorAfterWrite(Field field, Class<E> extractorType)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<E> captor = ArgumentCaptor.forClass(extractorType);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }
    
    private static Field boolField(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Bool()), null);
    }
    
    private static Field utf8Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }
    
    private void setupSchemaCaseInsensitive(String caseInsensitiveValue)
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, caseInsensitiveValue);
    }
    
    private void putVertexList(String fieldName, Object... elements)
    {
        vertexContext.put(fieldName, new ArrayList<>(Arrays.asList(elements)));
    }
}
