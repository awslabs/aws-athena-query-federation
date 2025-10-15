/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.lark.base.translator;

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.resolver.LarkBaseFieldResolver;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RegistererExtractorTest {

    @Mock
    private GeneratedRowWriter.RowWriterBuilder mockRowWriterBuilder;

    private RegistererExtractor registererExtractor;
    private Map<String, NestedUIType> larkFieldTypeMapping;

    @Before
    public void setUp() {
        larkFieldTypeMapping = new HashMap<>();
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);

        // Make the mock builder chainable
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);
        when(mockRowWriterBuilder.withFieldWriterFactory(any(String.class), any())).thenReturn(mockRowWriterBuilder);
    }

    @Test
    public void testConstructor_withNullMapping() {
        RegistererExtractor extractor = new RegistererExtractor(null);
        assertNotNull(extractor);
    }

    @Test
    public void testConstructor_withEmptyMapping() {
        RegistererExtractor extractor = new RegistererExtractor(new HashMap<>());
        assertNotNull(extractor);
    }

    @Test
    public void testRegisterExtractorsForSchema_withBitField() {
        Field bitField = new Field("checkbox_field", FieldType.nullable(new ArrowType.Bool()), null);
        Schema schema = new Schema(Collections.singletonList(bitField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withExtractor(eq("checkbox_field"), any(BitExtractor.class));
    }

    @Test
    public void testRegisterExtractorsForSchema_withTinyIntField() {
        Field tinyIntField = new Field("rating_field", FieldType.nullable(new ArrowType.Int(8, true)), null);
        Schema schema = new Schema(Collections.singletonList(tinyIntField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withExtractor(eq("rating_field"), any(TinyIntExtractor.class));
    }

    @Test
    public void testRegisterExtractorsForSchema_withVarCharField() {
        Field varCharField = new Field("text_field", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(Collections.singletonList(varCharField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withExtractor(eq("text_field"), any(VarCharExtractor.class));
    }

    @Test
    public void testRegisterExtractorsForSchema_withDecimalField() {
        Field decimalField = new Field("number_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);
        Schema schema = new Schema(Collections.singletonList(decimalField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withExtractor(eq("number_field"), any(DecimalExtractor.class));
    }

    @Test
    public void testRegisterExtractorsForSchema_withDateMilliField() {
        Field dateMilliField = new Field("date_field", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);
        Schema schema = new Schema(Collections.singletonList(dateMilliField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withExtractor(eq("date_field"), any(DateMilliExtractor.class));
    }

    @Test
    public void testRegisterExtractorsForSchema_withTimestampField() {
        Field timestampField = new Field("timestamp_field", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);
        Schema schema = new Schema(Collections.singletonList(timestampField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withExtractor(eq("timestamp_field"), any(BigIntExtractor.class));
    }

    @Test
    public void testRegisterExtractorsForSchema_withListField() {
        Field listField = new Field("list_field", FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        Schema schema = new Schema(Collections.singletonList(listField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("list_field"), any());
    }

    @Test
    public void testRegisterExtractorsForSchema_withStructField() {
        Field structField = new Field("struct_field", FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
            ));
        Schema schema = new Schema(Collections.singletonList(structField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("struct_field"), any());
    }

    @Test
    public void testRegisterExtractorsForSchema_withMultipleFields() {
        List<Field> fields = Arrays.asList(
            new Field("bit_field", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("varchar_field", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("decimal_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null)
        );
        Schema schema = new Schema(fields);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);

        verify(mockRowWriterBuilder).withExtractor(eq("bit_field"), any(BitExtractor.class));
        verify(mockRowWriterBuilder).withExtractor(eq("varchar_field"), any(VarCharExtractor.class));
        verify(mockRowWriterBuilder).withExtractor(eq("decimal_field"), any(DecimalExtractor.class));
    }

    @Test
    public void testTinyIntExtractor_withBooleanTrue() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", true);
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(1, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTinyIntExtractor_withBooleanFalse() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", false);
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTinyIntExtractor_withNumber() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", 5);
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(5, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTinyIntExtractor_withStringTrue() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "true");
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(1, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTinyIntExtractor_withStringFalse() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "false");
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTinyIntExtractor_withStringNumeric() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "42");
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(42, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTinyIntExtractor_withNull() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", null);
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTinyIntExtractor_withInvalidString() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "invalid");
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testBitExtractor_withBooleanTrue() throws Exception {
        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Bool()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", true);
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(context, holder);

        assertEquals(1, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testBitExtractor_withBooleanFalse() throws Exception {
        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Bool()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", false);
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testBitExtractor_withNull() throws Exception {
        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Bool()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", null);
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testBitExtractor_withNonBooleanValue() throws Exception {
        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Bool()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "some string");
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withString() throws Exception {
        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "test value");
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        assertEquals("test value", holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withNull() throws Exception {
        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", null);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withFormulaText() throws Exception {
        larkFieldTypeMapping.put("test_field", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.TEXT));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> textMap = new HashMap<>();
        textMap.put("text", "extracted text");
        List<Object> formulaList = Collections.singletonList(textMap);

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", formulaList);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        assertEquals("extracted text", holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withTextMap() throws Exception {
        larkFieldTypeMapping.put("test_field", new NestedUIType(UITypeEnum.TEXT, null));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> textMap = new HashMap<>();
        textMap.put("text", "map text value");

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", textMap);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        assertEquals("map text value", holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withNonStringValue() throws Exception {
        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", 12345);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        assertEquals("12345", holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDecimalExtractor_withBigDecimal() throws Exception {
        ArgumentCaptor<DecimalExtractor> extractorCaptor = ArgumentCaptor.forClass(DecimalExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DecimalExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", new BigDecimal("123.45"));
        NullableDecimalHolder holder = new NullableDecimalHolder();

        extractor.extract(context, holder);

        assertEquals(new BigDecimal("123.45"), holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDecimalExtractor_withNumber() throws Exception {
        ArgumentCaptor<DecimalExtractor> extractorCaptor = ArgumentCaptor.forClass(DecimalExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DecimalExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", 67.89);
        NullableDecimalHolder holder = new NullableDecimalHolder();

        extractor.extract(context, holder);

        assertEquals(new BigDecimal("67.89"), holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDecimalExtractor_withString() throws Exception {
        ArgumentCaptor<DecimalExtractor> extractorCaptor = ArgumentCaptor.forClass(DecimalExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DecimalExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "999.99");
        NullableDecimalHolder holder = new NullableDecimalHolder();

        extractor.extract(context, holder);

        assertEquals(new BigDecimal("999.99"), holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDecimalExtractor_withEmptyString() throws Exception {
        ArgumentCaptor<DecimalExtractor> extractorCaptor = ArgumentCaptor.forClass(DecimalExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DecimalExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "");
        NullableDecimalHolder holder = new NullableDecimalHolder();

        extractor.extract(context, holder);

        assertEquals(BigDecimal.ZERO, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDecimalExtractor_withNull() throws Exception {
        ArgumentCaptor<DecimalExtractor> extractorCaptor = ArgumentCaptor.forClass(DecimalExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DecimalExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", null);
        NullableDecimalHolder holder = new NullableDecimalHolder();

        extractor.extract(context, holder);

        assertEquals(BigDecimal.ZERO, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDecimalExtractor_withInvalidString() throws Exception {
        ArgumentCaptor<DecimalExtractor> extractorCaptor = ArgumentCaptor.forClass(DecimalExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DecimalExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", "invalid");
        NullableDecimalHolder holder = new NullableDecimalHolder();

        extractor.extract(context, holder);

        assertEquals(BigDecimal.ZERO, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDateMilliExtractor_withZeroValue() throws Exception {
        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", 0);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void testDateMilliExtractor_withMilliseconds() throws Exception {
        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        long testMillis = 1700000000000L; // > 10^10 threshold
        context.put("test_field", testMillis);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(context, holder);

        assertEquals(testMillis, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDateMilliExtractor_withSeconds() throws Exception {
        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        long testSeconds = 1700000000L; // > 100,000 threshold, but < 10^10
        context.put("test_field", testSeconds);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(context, holder);

        assertEquals(testSeconds * 1000, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDateMilliExtractor_withExcelDate() throws Exception {
        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        double excelDate = 44562.5; // January 1, 2022 at noon
        context.put("test_field", excelDate);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(context, holder);

        assertTrue(holder.value > 0);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testDateMilliExtractor_withNull() throws Exception {
        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", null);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void testTimestampMilliExtractor_withMilliseconds() throws Exception {
        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        long testMillis = 1700000000000L;
        context.put("test_field", testMillis);
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(context, holder);

        assertEquals(testMillis, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTimestampMilliExtractor_withSeconds() throws Exception {
        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        long testSeconds = 1700000000L;
        context.put("test_field", testSeconds);
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(context, holder);

        assertEquals(testSeconds * 1000, holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testTimestampMilliExtractor_withNull() throws Exception {
        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", null);
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void testTimestampMilliExtractor_withZero() throws Exception {
        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        context.put("test_field", 0);
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(context, holder);

        assertEquals(0, holder.isSet);
    }

    // Tests for non-Map context scenario (getContextMap returning empty map)
    @Test
    public void testTinyIntExtractor_withNonMapContext() throws Exception {
        ArgumentCaptor<TinyIntExtractor> extractorCaptor = ArgumentCaptor.forClass(TinyIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Int(8, true)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        TinyIntExtractor extractor = extractorCaptor.getValue();
        // Pass a non-Map context (e.g., String)
        String nonMapContext = "not a map";
        NullableTinyIntHolder holder = new NullableTinyIntHolder();

        extractor.extract(nonMapContext, holder);

        // Should default to 0 and isSet=1 (as if value was null)
        assertEquals(0, holder.value);
        assertEquals(1, holder.isSet);
    }

    // Tests for VarChar extractor edge cases
    @Test
    public void testVarCharExtractor_withFormulaTextEmptyList() throws Exception {
        larkFieldTypeMapping.put("test_field", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.TEXT));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        List<Object> emptyList = new ArrayList<>();

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", emptyList);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        // Should fall back to String.valueOf()
        assertEquals("[]", holder.value);
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withFormulaTextMapWithoutText() throws Exception {
        larkFieldTypeMapping.put("test_field", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.TEXT));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> mapWithoutText = new HashMap<>();
        mapWithoutText.put("other_key", "other_value");
        List<Object> formulaList = Collections.singletonList(mapWithoutText);

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", formulaList);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        // Should fall back to String.valueOf() since map doesn't have "text" key
        assertTrue(holder.value.contains("other_key"));
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withFormulaTextNullTextValue() throws Exception {
        larkFieldTypeMapping.put("test_field", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.TEXT));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> mapWithNullText = new HashMap<>();
        mapWithNullText.put("text", null);
        List<Object> formulaList = Collections.singletonList(mapWithNullText);

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", formulaList);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        // Should fall back to String.valueOf() since text value is null
        assertTrue(holder.value.contains("{"));
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_withTextMapNullTextValue() throws Exception {
        larkFieldTypeMapping.put("test_field", new NestedUIType(UITypeEnum.TEXT, null));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> textMap = new HashMap<>();
        textMap.put("text", null);

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", textMap);
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(context, holder);

        // Should fall back to String.valueOf() since text value is null
        assertTrue(holder.value.contains("{"));
        assertEquals(1, holder.isSet);
    }

    @Test
    public void testVarCharExtractor_exceptionHandling() throws Exception {
        larkFieldTypeMapping.put("test_field", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.TEXT));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withExtractor(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Utf8()), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();

        // Create a List with a bad object that throws exception during toString/instanceof check
        Object badObject = new Object() {
            @Override
            public String toString() {
                throw new RuntimeException("Simulated exception in toString");
            }
        };

        List<Object> badList = new ArrayList<>();
        badList.add(badObject);

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", badList);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        holder.isSet = 1; // Set initially to verify it gets reset to 0

        extractor.extract(context, holder);

        // Exception should be caught and isSet should be 0
        assertEquals(0, holder.isSet);
    }

    // Tests for DateMilli extractor exception handling
    @Test
    public void testDateMilliExtractor_exceptionHandling() throws Exception {
        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();

        // Create a Number that throws exception when converted
        Number badNumber = new Number() {
            @Override
            public int intValue() { throw new RuntimeException("Simulated exception"); }
            @Override
            public long longValue() { throw new RuntimeException("Simulated exception"); }
            @Override
            public float floatValue() { throw new RuntimeException("Simulated exception"); }
            @Override
            public double doubleValue() { throw new RuntimeException("Simulated exception"); }
        };

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", badNumber);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        holder.isSet = 1; // Set initially to verify it gets reset to 0

        extractor.extract(context, holder);

        // Exception should be caught and isSet should be 0
        assertEquals(0, holder.isSet);
    }

    // Tests for Timestamp extractor exception handling
    @Test
    public void testTimestampMilliExtractor_exceptionHandling() throws Exception {
        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();

        // Create a Number that throws exception when converted
        Number badNumber = new Number() {
            @Override
            public int intValue() { throw new RuntimeException("Simulated exception"); }
            @Override
            public long longValue() { throw new RuntimeException("Simulated exception"); }
            @Override
            public float floatValue() { throw new RuntimeException("Simulated exception"); }
            @Override
            public double doubleValue() { throw new RuntimeException("Simulated exception"); }
        };

        Map<String, Object> context = new HashMap<>();
        context.put("test_field", badNumber);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        holder.isSet = 1; // Set initially to verify it gets reset to 0

        extractor.extract(context, holder);

        // Exception should be caught and isSet should be 0
        assertEquals(0, holder.isSet);
    }

    @Test
    public void testTimestampMilliExtractor_withExcelDate() throws Exception {
        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        Field field = new Field("test_field", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, new Schema(Collections.singletonList(field)));
        verify(mockRowWriterBuilder).withExtractor(eq("test_field"), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        Map<String, Object> context = new HashMap<>();
        double excelDate = 44562.5; // January 1, 2022 at noon
        context.put("test_field", excelDate);
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(context, holder);

        assertTrue(holder.value > 0);
        assertEquals(1, holder.isSet);
    }

    // Tests for List field writer factory
    @Test
    public void testListFieldWriterFactory_withNullValue() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field listField = new Field("list_field", FieldType.nullable(new ArrowType.List()),
                Collections.singletonList(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        Schema schema = new Schema(Collections.singletonList(listField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("list_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            // FieldWriterFactory.create returns a FieldWriter
            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            Map<String, Object> context = new HashMap<>();
            context.put("list_field", null);

            boolean result = writer.write(context, 0);

            assertTrue(result);
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(null)));
        }
    }

    @Test
    public void testListFieldWriterFactory_withNonListValue() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field listField = new Field("list_field", FieldType.nullable(new ArrowType.List()),
                Collections.singletonList(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        Schema schema = new Schema(Collections.singletonList(listField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("list_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            Map<String, Object> context = new HashMap<>();
            context.put("list_field", "not a list"); // Wrong type

            boolean result = writer.write(context, 0);

            assertFalse(result);
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(null)));
        }
    }

    @Test
    public void testListFieldWriterFactory_withValidList() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field listField = new Field("list_field", FieldType.nullable(new ArrowType.List()),
                Collections.singletonList(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        Schema schema = new Schema(Collections.singletonList(listField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("list_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            List<String> testList = Arrays.asList("item1", "item2", "item3");
            Map<String, Object> context = new HashMap<>();
            context.put("list_field", testList);

            boolean result = writer.write(context, 0);

            assertTrue(result);
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(testList)));
        }
    }

    @Test
    public void testListFieldWriterFactory_withLookupTextTransformation() throws Exception {
        larkFieldTypeMapping.put("lookup_field", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.TEXT));
        registererExtractor = new RegistererExtractor(larkFieldTypeMapping);
        when(mockRowWriterBuilder.withFieldWriterFactory(any(String.class), any())).thenReturn(mockRowWriterBuilder);

        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field listField = new Field("lookup_field", FieldType.nullable(new ArrowType.List()),
                Collections.singletonList(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        Schema schema = new Schema(Collections.singletonList(listField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("lookup_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            // Create list of maps with "text" field
            Map<String, Object> item1 = new HashMap<>();
            item1.put("text", "value1");
            Map<String, Object> item2 = new HashMap<>();
            item2.put("text", "value2");
            List<Object> lookupList = Arrays.asList(item1, item2);

            Map<String, Object> context = new HashMap<>();
            context.put("lookup_field", lookupList);

            boolean result = writer.write(context, 0);

            assertTrue(result);
            // Verify that the list was transformed to List<String>
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), argThat(arg -> {
                if (arg instanceof List<?> list) {
                    return list.size() == 2 && "value1".equals(list.get(0)) && "value2".equals(list.get(1));
                }
                return false;
            })));
        }
    }

    @Test
    public void testListFieldWriterFactory_withException() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field listField = new Field("list_field", FieldType.nullable(new ArrowType.List()),
                Collections.singletonList(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        Schema schema = new Schema(Collections.singletonList(listField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("list_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            // Make BlockUtils throw exception on first call (with list), succeed on second call (with null)
            blockUtilsMock.when(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), argThat(arg -> arg instanceof List)))
                    .thenThrow(new RuntimeException("Simulated exception"));
            blockUtilsMock.when(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(null)))
                    .then(invocation -> null);

            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            List<String> testList = Arrays.asList("item1", "item2");
            Map<String, Object> context = new HashMap<>();
            context.put("list_field", testList);

            boolean result = writer.write(context, 0);

            assertFalse(result);
            // Verify it tried twice - once with the list (threw exception), once with null (succeeded)
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(any(), anyInt(), any(), any()), times(2));
        }
    }

    // Tests for Struct field writer factory
    @Test
    public void testStructFieldWriterFactory_withNullValue() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field structField = new Field("struct_field", FieldType.nullable(new ArrowType.Struct()),
                Arrays.asList(
                        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
                ));
        Schema schema = new Schema(Collections.singletonList(structField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("struct_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            Map<String, Object> context = new HashMap<>();
            context.put("struct_field", null);

            boolean result = writer.write(context, 0);

            assertTrue(result);
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(null)));
        }
    }

    @Test
    public void testStructFieldWriterFactory_withNonMapValue() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field structField = new Field("struct_field", FieldType.nullable(new ArrowType.Struct()),
                Arrays.asList(
                        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
                ));
        Schema schema = new Schema(Collections.singletonList(structField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("struct_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            Map<String, Object> context = new HashMap<>();
            context.put("struct_field", "not a map"); // Wrong type

            boolean result = writer.write(context, 0);

            assertFalse(result);
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(null)));
        }
    }

    @Test
    public void testStructFieldWriterFactory_withValidMap() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field structField = new Field("struct_field", FieldType.nullable(new ArrowType.Struct()),
                Arrays.asList(
                        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
                ));
        Schema schema = new Schema(Collections.singletonList(structField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("struct_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            Map<String, Object> structValue = new HashMap<>();
            structValue.put("name", "test_name");
            structValue.put("value", 42);

            Map<String, Object> context = new HashMap<>();
            context.put("struct_field", structValue);

            boolean result = writer.write(context, 0);

            assertTrue(result);
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(structValue)));
        }
    }

    @Test
    public void testStructFieldWriterFactory_withException() throws Exception {
        ArgumentCaptor<FieldWriterFactory> factoryCaptor = ArgumentCaptor.forClass(FieldWriterFactory.class);
        Field structField = new Field("struct_field", FieldType.nullable(new ArrowType.Struct()),
                Arrays.asList(
                        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)
                ));
        Schema schema = new Schema(Collections.singletonList(structField));

        registererExtractor.registerExtractorsForSchema(mockRowWriterBuilder, schema);
        verify(mockRowWriterBuilder).withFieldWriterFactory(eq("struct_field"), factoryCaptor.capture());

        FieldWriterFactory factory = factoryCaptor.getValue();
        FieldVector mockVector = mock(FieldVector.class);
        Extractor mockExtractor = mock(Extractor.class);

        try (MockedStatic<BlockUtils> blockUtilsMock = mockStatic(BlockUtils.class)) {
            // Make BlockUtils throw exception on first call (with map), succeed on second call (with null)
            blockUtilsMock.when(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), argThat(arg -> arg instanceof Map)))
                    .thenThrow(new RuntimeException("Simulated exception"));
            blockUtilsMock.when(() -> BlockUtils.setComplexValue(eq(mockVector), eq(0), any(LarkBaseFieldResolver.class), eq(null)))
                    .then(invocation -> null);

            FieldWriter writer = factory.create(mockVector, mockExtractor, null);

            Map<String, Object> structValue = new HashMap<>();
            structValue.put("name", "test_name");
            structValue.put("value", 42);

            Map<String, Object> context = new HashMap<>();
            context.put("struct_field", structValue);

            boolean result = writer.write(context, 0);

            assertFalse(result);
            // Verify it tried twice - once with the struct (threw exception), once with null (succeeded)
            blockUtilsMock.verify(() -> BlockUtils.setComplexValue(any(), anyInt(), any(), any()), times(2));
        }
    }
}
