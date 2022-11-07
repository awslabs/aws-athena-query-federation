/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.connectors.athena.deltalake.converter;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.connectors.athena.deltalake.converter.DeltaConverter.castPartitionValue;
import static org.junit.Assert.assertEquals;

public class DeltaConverterTest {

    private Field createField(String name, ArrowType arrowType, boolean nullable) {
        return createField(name, arrowType, nullable, Collections.emptyList());
    }

    private Field createField(String name, ArrowType arrowType, boolean nullable, List<Field> children) {
        return new Field(name, new FieldType(nullable, arrowType, null), children);
    }

    @Test
    public void getArrowSchema() throws IOException {
        File schemaStringFile = new File(getClass().getClassLoader().getResource("schema_test.json").getFile());
        String schemaString = FileUtils.readFileToString(schemaStringFile, "UTF-8");

        Schema arrowSchema = DeltaConverter.getArrowSchema(schemaString);
        Field expectedIntField = createField("a", new ArrowType.Int(32, true), false);
        Field expectedStructField = createField("b", new ArrowType.Struct(), true, Arrays.asList(
            createField("d", new ArrowType.Decimal(22, 4, 128), false)
        ));
        Field expectedListField = createField("c", new ArrowType.List(), true, Arrays.asList(
            createField("element", new ArrowType.Int(32, true), false)
        ));
        Field expectedComplexListField = createField("e", new ArrowType.List(), true, Arrays.asList(
            createField("element", new ArrowType.Struct(), true, Arrays.asList(
                createField("d", new ArrowType.Int(32, true), false)
            ))
        ));
        Field expectedMapField = createField("f", new ArrowType.Map(true), true, Arrays.asList(
            createField("entries", new ArrowType.Struct(), false, Arrays.asList(
                createField("key", new ArrowType.Utf8(), false),
                createField("value", new ArrowType.Utf8(), true)
            ))
        ));


        List<Field> fields = arrowSchema.getFields();

        assertEquals(5, fields.size());
        assertEquals(expectedIntField, fields.get(0));
        assertEquals(expectedStructField, fields.get(1));
        assertEquals(expectedListField, fields.get(2));
        assertEquals(expectedComplexListField, fields.get(3));
        assertEquals(expectedMapField, fields.get(4));
    }

    @Test
    public void getAvroFieldInteger() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "integer");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Int(32, true), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }


    @Test
    public void getAvroFieldString() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "string");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Utf8(), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldLong() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "long");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Int(64, true), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldShort() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "short");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Int(16, true), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldByte() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "byte");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Int(8, true), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldFloat() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "float");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldDouble() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "double");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldBoolean() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "boolean");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Bool(), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldBinary() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "binary");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Binary(), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldDate() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "date");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Date(DateUnit.DAY), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldTimestamp() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "timestamp");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", new ArrowType.Date(DateUnit.MILLISECOND), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }

    @Test
    public void getAvroFieldDecimal() {
        ObjectNode field = JsonNodeFactory.instance.objectNode();
        field.put("name", "a");
        field.put("type", "decimal(21,5)");
        field.put("nullable", "true");
        field.putObject("metadata");
        Field expectedField = createField("a", ArrowType.Decimal.createDecimal(21, 5, 128), true);

        Field avroField = DeltaConverter.getAvroField(field);

        assertEquals(expectedField, avroField);
    }


    @Test
    public void testCastPartitionValue() {
        assertEquals(new Text("test"), castPartitionValue("test", Types.MinorType.VARCHAR.getType()));
        assertEquals(2021, castPartitionValue("2021", Types.MinorType.INT.getType()));
        assertEquals(12.1f, castPartitionValue("12.1", Types.MinorType.FLOAT8.getType()));
        assertEquals(18661, castPartitionValue("2021-02-03", Types.MinorType.DATEDAY.getType()));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 2, 3, 12, 32,27)), castPartitionValue("2021-02-03 12:32:27", Types.MinorType.TIMESTAMPSEC.getType()));
        assertEquals(1, castPartitionValue("true", Types.MinorType.BIT.getType()));
    }
}
