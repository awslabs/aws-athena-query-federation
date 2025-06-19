/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneSchemaUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(NeptuneSchemaUtilsTest.class);

    private static final String COMPONENT_TYPE_VERTEX = "vertex";
    private static final String COMPONENT_TYPE_EDGE = "edge";
    private static final String TABLE_NAME = "test_table";
    private static final String EXPECTED_STRING_TYPE = "Utf8";
    private static final String EXPECTED_INT_TYPE = "Int(32, true)";
    private static final String EXPECTED_BIGINT_TYPE = "Int(64, true)";
    private static final String EXPECTED_BOOLEAN_TYPE = "Bool";
    private static final String EXPECTED_FLOAT_TYPE = "FloatingPoint(SINGLE)";
    private static final String EXPECTED_DOUBLE_TYPE = "FloatingPoint(DOUBLE)";
    private static final String EXPECTED_DATEMILLI_TYPE = "Date(MILLISECOND)";
    private Map<String, Object> testDataMap;
    private Schema schema;

    @Before
    public void setUp()
    {
        logger.info("Setting up test data");
        testDataMap = new HashMap<>();
    }

    @Test
    public void testGetSchemaFromResults_StringType()
    {
        logger.info("Testing String type schema inference");
        testDataMap.put("stringField", "test string");
        testDataMap.put("uuidField", UUID.randomUUID());

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_STRING_TYPE, schema.findField("stringField").getType().toString());
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("uuidField").getType().toString());
        assertEquals(2, schema.getFields().size());
        verifySchemaMetadata(COMPONENT_TYPE_VERTEX);
    }

    @Test
    public void testGetSchemaFromResults_IntegerType()
    {
        logger.info("Testing Integer type schema inference");
        testDataMap.put("intField", 42);
        testDataMap.put("negativeInt", -100);
        testDataMap.put("zeroInt", 0);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_INT_TYPE, schema.findField("intField").getType().toString());
        assertEquals(EXPECTED_INT_TYPE, schema.findField("negativeInt").getType().toString());
        assertEquals(EXPECTED_INT_TYPE, schema.findField("zeroInt").getType().toString());
        assertEquals(3, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_BigIntegerType()
    {
        logger.info("Testing BigInteger type schema inference");
        testDataMap.put("bigIntField", new BigInteger("12345678901234567890"));
        testDataMap.put("largeBigInt", new BigInteger("999999999999999999999999999999"));

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_BIGINT_TYPE, schema.findField("bigIntField").getType().toString());
        assertEquals(EXPECTED_BIGINT_TYPE, schema.findField("largeBigInt").getType().toString());
        assertEquals(2, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_LongType()
    {
        logger.info("Testing Long type schema inference");
        testDataMap.put("longField", 9223372036854775807L);
        testDataMap.put("negativeLong", -9223372036854775808L);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_BIGINT_TYPE, schema.findField("longField").getType().toString());
        assertEquals(EXPECTED_BIGINT_TYPE, schema.findField("negativeLong").getType().toString());
        assertEquals(2, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_BooleanType()
    {
        logger.info("Testing Boolean type schema inference");
        testDataMap.put("trueField", true);
        testDataMap.put("falseField", false);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_BOOLEAN_TYPE, schema.findField("trueField").getType().toString());
        assertEquals(EXPECTED_BOOLEAN_TYPE, schema.findField("falseField").getType().toString());
        assertEquals(2, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_FloatType()
    {
        logger.info("Testing Float type schema inference");
        testDataMap.put("floatField", 3.14f);
        testDataMap.put("negativeFloat", -2.718f);
        testDataMap.put("zeroFloat", 0.0f);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_FLOAT_TYPE, schema.findField("floatField").getType().toString());
        assertEquals(EXPECTED_FLOAT_TYPE, schema.findField("negativeFloat").getType().toString());
        assertEquals(EXPECTED_FLOAT_TYPE, schema.findField("zeroFloat").getType().toString());
        assertEquals(3, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_DoubleType()
    {
        logger.info("Testing Double type schema inference");
        testDataMap.put("doubleField", 3.14159265359);
        testDataMap.put("negativeDouble", -2.71828182846);
        testDataMap.put("zeroDouble", 0.0);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_DOUBLE_TYPE, schema.findField("doubleField").getType().toString());
        assertEquals(EXPECTED_DOUBLE_TYPE, schema.findField("negativeDouble").getType().toString());
        assertEquals(EXPECTED_DOUBLE_TYPE, schema.findField("zeroDouble").getType().toString());
        assertEquals(3, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_DateType()
    {
        logger.info("Testing Date type schema inference");
        Date currentDate = new Date();
        Date pastDate = new Date(System.currentTimeMillis() - 86400000); // 1 day ago

        testDataMap.put("currentDate", currentDate);
        testDataMap.put("pastDate", pastDate);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_DATEMILLI_TYPE, schema.findField("currentDate").getType().toString());
        assertEquals(EXPECTED_DATEMILLI_TYPE, schema.findField("pastDate").getType().toString());
        assertEquals(2, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_ListType()
    {
        logger.info("Testing List type schema inference");
        List<String> stringList = new ArrayList<>();
        stringList.add("item1");
        stringList.add("item2");

        List<Integer> intList = new ArrayList<>();
        intList.add(1);
        intList.add(2);
        intList.add(3);

        testDataMap.put("stringList", stringList);
        testDataMap.put("intList", intList);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        // Lists should default to the type of their first element
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("stringList").getType().toString());
        assertEquals(EXPECTED_INT_TYPE, schema.findField("intList").getType().toString());
        assertEquals(2, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_EmptyList()
    {
        logger.info("Testing Empty List type schema inference");
        List<Object> emptyList = new ArrayList<>();
        emptyList.add(new Object());
        testDataMap.put("List", emptyList);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        // list should default to VARCHAR
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("List").getType().toString());
        assertEquals(1, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_NullValue()
    {
        logger.info("Testing null value schema inference");
        testDataMap.put("nullField", null);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        // Null values should default to VARCHAR
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("nullField").getType().toString());
        assertEquals(1, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_UnknownType()
    {
        logger.info("Testing unknown type schema inference");
        Object unknownObject = new Object();
        testDataMap.put("unknownField", unknownObject);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        // Unknown types should default to VARCHAR
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("unknownField").getType().toString());
        assertEquals(1, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_MixedTypes()
    {
        logger.info("Testing mixed types schema inference");
        testDataMap.put("stringField", "test");
        testDataMap.put("intField", 42);
        testDataMap.put("doubleField", 3.14);
        testDataMap.put("boolField", true);
        testDataMap.put("dateField", new Date());
        testDataMap.put("nullField", null);

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(EXPECTED_STRING_TYPE, schema.findField("stringField").getType().toString());
        assertEquals(EXPECTED_INT_TYPE, schema.findField("intField").getType().toString());
        assertEquals(EXPECTED_DOUBLE_TYPE, schema.findField("doubleField").getType().toString());
        assertEquals(EXPECTED_BOOLEAN_TYPE, schema.findField("boolField").getType().toString());
        assertEquals(EXPECTED_DATEMILLI_TYPE, schema.findField("dateField").getType().toString());
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("nullField").getType().toString());
        assertEquals(6, schema.getFields().size());
    }

    @Test
    public void testGetSchemaFromResults_EdgeComponentType()
    {
        logger.info("Testing edge component type");
        testDataMap.put("edgeField", "edge value");

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_EDGE, TABLE_NAME);

        assertEquals(1, schema.getFields().size());
        verifySchemaMetadata(COMPONENT_TYPE_EDGE);
    }

    @Test
    public void testGetSchemaFromResults_EmptyMap()
    {
        logger.info("Testing empty map");
        Map<String, Object> emptyMap = new HashMap<>();

        schema = NeptuneSchemaUtils.getSchemaFromResults(emptyMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(0, schema.getFields().size());
        verifySchemaMetadata(COMPONENT_TYPE_VERTEX);
    }

    @Test
    public void testGetSchemaFromResults_UnmodifiableMap()
    {
        logger.info("Testing unmodifiable map");
        testDataMap.put("field1", "value1");
        testDataMap.put("field2", 42);

        Map<String, Object> unmodifiableMap = Collections.unmodifiableMap(testDataMap);

        schema = NeptuneSchemaUtils.getSchemaFromResults(unmodifiableMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(2, schema.getFields().size());
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("field1").getType().toString());
        assertEquals(EXPECTED_INT_TYPE, schema.findField("field2").getType().toString());
    }

    @Test
    public void testGetSchemaFromResults_SpecialCharactersInFieldNames()
    {
        logger.info("Testing special characters in field names");
        testDataMap.put("field_with_underscore", "value1");
        testDataMap.put("field-with-dash", "value2");
        testDataMap.put("fieldWithCamelCase", "value3");
        testDataMap.put("FIELD_WITH_UPPERCASE", "value4");

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(4, schema.getFields().size());
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("field_with_underscore").getType().toString());
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("field-with-dash").getType().toString());
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("fieldWithCamelCase").getType().toString());
        assertEquals(EXPECTED_STRING_TYPE, schema.findField("FIELD_WITH_UPPERCASE").getType().toString());
    }

    @Test
    public void testGetSchemaFromResults_LargeNumbers()
    {
        logger.info("Testing large numbers");
        testDataMap.put("maxInt", Integer.MAX_VALUE);
        testDataMap.put("minInt", Integer.MIN_VALUE);
        testDataMap.put("maxLong", Long.MAX_VALUE);
        testDataMap.put("minLong", Long.MIN_VALUE);
        testDataMap.put("largeBigInt", new BigInteger("9999999999999999999999999999999999999999"));

        schema = NeptuneSchemaUtils.getSchemaFromResults(testDataMap, COMPONENT_TYPE_VERTEX, TABLE_NAME);

        assertEquals(5, schema.getFields().size());
        assertEquals(EXPECTED_INT_TYPE, schema.findField("maxInt").getType().toString());
        assertEquals(EXPECTED_INT_TYPE, schema.findField("minInt").getType().toString());
        assertEquals(EXPECTED_BIGINT_TYPE, schema.findField("maxLong").getType().toString());
        assertEquals(EXPECTED_BIGINT_TYPE, schema.findField("minLong").getType().toString());
        assertEquals(EXPECTED_BIGINT_TYPE, schema.findField("largeBigInt").getType().toString());
    }

    private void verifySchemaMetadata(String expectedComponentType)
    {
        assertNotNull("Schema should not be null", schema);
        assertEquals("Component type should match", expectedComponentType,
                schema.getCustomMetadata().get(Constants.SCHEMA_COMPONENT_TYPE));
        assertEquals("Table name should match", NeptuneSchemaUtilsTest.TABLE_NAME,
                schema.getCustomMetadata().get(Constants.SCHEMA_GLABEL));
    }
}
