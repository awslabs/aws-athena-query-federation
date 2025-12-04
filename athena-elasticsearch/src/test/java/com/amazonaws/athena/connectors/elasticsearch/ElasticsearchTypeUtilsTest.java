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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class is used to test the ElasticsearchTypeUtils class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchTypeUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTypeUtilsTest.class);
    private ElasticsearchTypeUtils typeUtils = new ElasticsearchTypeUtils();
    private Schema mapping;

    /**
     * Test the VARCHAR extractor to extract string values.
     * @throws Exception
     */
    @Test
    public void makeVarCharExtractor_withDocument_extractsVarCharValues()
            throws Exception
    {
        logger.info("makeVarCharExtractor_withDocument_extractsVarCharValues - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("mytext", Types.MinorType.VARCHAR.getType())
                .addField("mytextlist", Types.MinorType.VARCHAR.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
            "{\n" +
                    "  \"mytext\" : \"My favorite Sci-Fi movie is Interstellar.\",\n" +
                    "  \"mytextlist\" : [\n" +
                    "    \"Hey, this is an array!\",\n" +
                    "    \"Wasn't expecting this!\"\n" +
                    "  ]\n" +
                    "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "mytext", "My favorite Sci-Fi movie is Interstellar.",
                "mytextlist", "[Hey, this is an array!, Wasn't expecting this!]");
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeVarCharExtractor_withDocument_extractsVarCharValues - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeVarCharExtractor_withDocument_extractsVarCharValues - exit");
    }

    /**
     * Test the BIGINT extractor to extract long and scaled_float values.
     * @throws Exception
     */
    @Test
    public void makeBigIntExtractor_withDocument_extractsBigIntValues()
            throws Exception
    {
        logger.info("makeBigIntExtractor_withDocument_extractsBigIntValues - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField(new Field("myscaledfloat",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10.51")), null))
                .addField(new Field("myscaledstring",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "100.0")), null))
                .addField(new Field("myscaledfloatlist",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10")), null))
                .addField(new Field("myscaledstringlist",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10.0")), null))
                .addField("mylong", Types.MinorType.BIGINT.getType())
                .addField("mylongstring", Types.MinorType.BIGINT.getType())
                .addField("mylonglist", Types.MinorType.BIGINT.getType())
                .addField("mylongstringlist", Types.MinorType.BIGINT.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"myscaledfloat\" : 0.666,\n" +
                        "  \"myscaledstring\" : \"0.999\",\n" +
                        "  \"myscaledfloatlist\" : [\n" +
                        "    0.777,\n" +
                        "    0.888\n" +
                        "  ],\n" +
                        "  \"myscaledstringlist\" : [\n" +
                        "    \"0.5\",\n" +
                        "    \"0.2\"\n" +
                        "  ],\n" +
                        "  \"mylong\" : 1234567.8910,\n" +
                        "  \"mylongstring\" : \"54345.55\",\n" +
                        "  \"mylonglist\" : [\n" +
                        "    2374637.342,\n" +
                        "    1000304594\n" +
                        "  ],\n" +
                        "  \"mylongstringlist\" : [\n" +
                        "    \"0945857834.33\",\n" +
                        "    \"33433535\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = new HashMap<>();
        expectedResults.put("myscaledfloat", new Long(7));
        expectedResults.put("myscaledstring", new Long(100));
        expectedResults.put("myscaledfloatlist", new Long(8));
        expectedResults.put("myscaledstringlist", new Long(5));
        expectedResults.put("mylong", new Long(1234567));
        expectedResults.put("mylongstring", new Long(54345));
        expectedResults.put("mylonglist", new Long(2374637));
        expectedResults.put("mylongstringlist", new Long(945857834));
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeBigIntExtractor_withDocument_extractsBigIntValues - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeBigIntExtractor_withDocument_extractsBigIntValues - exit");
    }

    /**
     * Test the INT extractor to extract integer values.
     * @throws Exception
     */
    @Test
    public void makeIntExtractor_withDocument_extractsIntValues()
            throws Exception
    {
        logger.info("makeIntExtractor_withDocument_extractsIntValues - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("myint", Types.MinorType.INT.getType())
                .addField("myintstring", Types.MinorType.INT.getType())
                .addField("myintlist", Types.MinorType.INT.getType())
                .addField("myintstringlist", Types.MinorType.INT.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"myint\" : 5329347.8910,\n" +
                        "  \"myintstring\" : \"0479374.55\",\n" +
                        "  \"myintlist\" : [\n" +
                        "    472394.342,\n" +
                        "    1000304594\n" +
                        "  ],\n" +
                        "  \"myintstringlist\" : [\n" +
                        "    \"34875934.33\",\n" +
                        "    \"33433535\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "myint", new Integer(5329347),
                "myintstring", new Integer(479374),
                "myintlist", new Integer(472394),
                "myintstringlist", new Integer(34875934));
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeIntExtractor_withDocument_extractsIntValues - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeIntExtractor_withDocument_extractsIntValues - exit");
    }

    /**
     * Test the SMALLINT extractor to extract short values.
     * @throws Exception
     */
    @Test
    public void makeSmallIntExtractor_withDocument_extractsSmallIntValues()
            throws Exception
    {
        logger.info("makeSmallIntExtractor_withDocument_extractsSmallIntValues - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("myshort", Types.MinorType.SMALLINT.getType())
                .addField("myshortstring", Types.MinorType.SMALLINT.getType())
                .addField("myshortlist", Types.MinorType.SMALLINT.getType())
                .addField("myshortstringlist", Types.MinorType.SMALLINT.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"myshort\" : 123.5,\n" +
                        "  \"myshortstring\" : \"055.55\",\n" +
                        "  \"myshortlist\" : [\n" +
                        "    543.342,\n" +
                        "    1000304594\n" +
                        "  ],\n" +
                        "  \"myshortstringlist\" : [\n" +
                        "    \"334.33\",\n" +
                        "    \"33433535\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "myshort", new Short((short)123),
                "myshortstring", new Short((short)55),
                "myshortlist", new Short((short) 543),
                "myshortstringlist", new Short((short) 334));
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeSmallIntExtractor_withDocument_extractsSmallIntValues - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeSmallIntExtractor_withDocument_extractsSmallIntValues - exit");
    }

    /**
     * Test the TINYINT extractor to extract byte values.
     * @throws Exception
     */
    @Test
    public void makeTinyIntExtractor_withDocument_extractsTinyIntValues()
            throws Exception
    {
        logger.info("makeTinyIntExtractor_withDocument_extractsTinyIntValues - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .addField("mybytestring", Types.MinorType.TINYINT.getType())
                .addField("mybytelist", Types.MinorType.TINYINT.getType())
                .addField("mybytestringlist", Types.MinorType.TINYINT.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"mybyte\" : 5,\n" +
                        "  \"mybytestring\" : \"6.5\",\n" +
                        "  \"mybytelist\" : [\n" +
                        "    1.5,\n" +
                        "    2\n" +
                        "  ],\n" +
                        "  \"mybytestringlist\" : [\n" +
                        "    \"3.3\",\n" +
                        "    \"4\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "mybyte", new Byte((byte)5),
                "mybytestring", new Byte((byte)6),
                "mybytelist", new Byte((byte) 1),
                "mybytestringlist", new Byte((byte) 3));
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeTinyIntExtractor_withDocument_extractsTinyIntValues - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeTinyIntExtractor_withDocument_extractsTinyIntValues - exit");
    }

    /**
     * Test the FLOAT8 extractor to extract double values.
     * @throws Exception
     */
    @Test
    public void makeFloat8Extractor_withDocument_extractsFloat8Values()
            throws Exception
    {
        logger.info("makeFloat8Extractor_withDocument_extractsFloat8Values - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .addField("mydoublestring", Types.MinorType.FLOAT8.getType())
                .addField("mydoublelist", Types.MinorType.FLOAT8.getType())
                .addField("mydoublestringlist", Types.MinorType.FLOAT8.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"mydouble\" : 529388745.45784,\n" +
                        "  \"mydoublestring\" : \"923643764.2325\",\n" +
                        "  \"mydoublelist\" : [\n" +
                        "    65,\n" +
                        "    2\n" +
                        "  ],\n" +
                        "  \"mydoublestringlist\" : [\n" +
                        "    \"10\",\n" +
                        "    \"4\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "mydouble", new Double(529388745.45784),
                "mydoublestring", new Double(923643764.2325),
                "mydoublelist", new Double(65.0),
                "mydoublestringlist", new Double(10.0));
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeFloat8Extractor_withDocument_extractsFloat8Values - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeFloat8Extractor_withDocument_extractsFloat8Values - exit");
    }

    /**
     * Test the FLOAT4 extractor to extract float values.
     * @throws Exception
     */
    @Test
    public void makeFloat4Extractor_withDocument_extractsFloat4Values()
            throws Exception
    {
        logger.info("makeFloat4Extractor_withDocument_extractsFloat4Values - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("myfloat", Types.MinorType.FLOAT4.getType())
                .addField("myfloatstring", Types.MinorType.FLOAT4.getType())
                .addField("myfloatlist", Types.MinorType.FLOAT4.getType())
                .addField("myfloatstringlist", Types.MinorType.FLOAT4.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"myfloat\" : 529.84,\n" +
                        "  \"myfloatstring\" : \"764.25\",\n" +
                        "  \"myfloatlist\" : [\n" +
                        "    23,\n" +
                        "    2\n" +
                        "  ],\n" +
                        "  \"myfloatstringlist\" : [\n" +
                        "    \"45\",\n" +
                        "    \"4\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "myfloat", new Float(529.84),
                "myfloatstring", new Float(764.25),
                "myfloatlist", new Float(23.0),
                "myfloatstringlist", new Float(45.0));
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeFloat4Extractor_withDocument_extractsFloat4Values - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeFloat4Extractor_withDocument_extractsFloat4Values - exit");
    }

    /**
     * Test the DATEMILLI extractor to extract timestamp values in milliseconds.
     * @throws Exception
     */
    @Test
    public void makeDateMilliExtractor_withDocument_extractsDateMilliValues()
            throws Exception
    {
        logger.info("makeDateMilliExtractor_withDocument_extractsDateMilliValues - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("mydate", Types.MinorType.DATEMILLI.getType())
                .addField("mydatestring", Types.MinorType.DATEMILLI.getType())
                .addField("mydatelist", Types.MinorType.DATEMILLI.getType())
                .addField("mydatestringlist", Types.MinorType.DATEMILLI.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"mydate\" : 1589796930124,\n" +
                        "  \"mydatestring\" : \"2020-05-19T10:15:30.456789\",\n" +
                        "  \"mydatelist\" : [\n" +
                        "    1589969730789,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  \"mydatestringlist\" : [\n" +
                        "    \"2020-05-15T06:49:30.123-05:00\",\n" +
                        "    \"0\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "mydate", new Long("1589796930124"),
                "mydatestring", new Long("1589883330457"),
                "mydatelist", new Long("1589969730789"),
                "mydatestringlist", new Long("1589543370123"));
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeDateMilliExtractor_withDocument_extractsDateMilliValues - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeDateMilliExtractor_withDocument_extractsDateMilliValues - exit");
    }

    /**
     * Test the BIT extractor to extract boolean values.
     * @throws Exception
     */
    @Test
    public void makeBitExtractor_withDocument_extractsBitValues()
            throws Exception
    {
        logger.info("makeBitExtractor_withDocument_extractsBitValues - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField("myboolean", Types.MinorType.BIT.getType())
                .addField("mybooleanstring", Types.MinorType.BIT.getType())
                .addField("mybooleanlist", Types.MinorType.BIT.getType())
                .addField("mybooleanstringlist", Types.MinorType.BIT.getType())
                .build();

        Map <String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"myboolean\" : true,\n" +
                        "  \"mybooleanstring\" : \"false\",\n" +
                        "  \"mybooleanlist\" : [\n" +
                        "    false,\n" +
                        "    true\n" +
                        "  ],\n" +
                        "  \"mybooleanstringlist\" : [\n" +
                        "    \"true\",\n" +
                        "    \"false\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        Map<String, Object> expectedResults = ImmutableMap.of(
                "myboolean", 1,
                "mybooleanstring", 0,
                "mybooleanlist", 0,
                "mybooleanstringlist", 1);
        Map<String, Object> extractedResults = testField(mapping, document);
        logger.info("makeBitExtractor_withDocument_extractsBitValues - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);

        logger.info("makeBitExtractor_withDocument_extractsBitValues - exit");
    }

    /**
     * Uses the correct field extractor to extract values from a document.
     * @param mapping is the metadata definitions of the document being processed.
     * @param document contains the values to be extracted.
     * @return a map of the field names and their associated values extracted from the document.
     * @throws Exception
     */
    private Map<String, Object> testField(Schema mapping, Map<String, Object> document)
            throws Exception
    {
        Map<String, Object> results = new HashMap<>();
        for (Field field : mapping.getFields()) {
            Extractor extractor = typeUtils.makeExtractor(field);
            Object value = extractValue(extractor, field, document);
            if (value != null) {
                results.put(field.getName(), value);
            }
        }

        return results;
    }

    /**
     * Extracts a value from a document using the appropriate extractor and holder.
     * @param extractor the extractor to use
     * @param field the field being extracted
     * @param document the document containing the data
     * @return the extracted value, or null if extraction failed
     * @throws Exception
     */
    private Object extractValue(Extractor extractor, Field field, Map<String, Object> document)
            throws Exception
    {
        if (extractor instanceof VarCharExtractor) {
            NullableVarCharHolder holder = new NullableVarCharHolder();
            ((VarCharExtractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof BigIntExtractor) {
            NullableBigIntHolder holder = new NullableBigIntHolder();
            ((BigIntExtractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof IntExtractor) {
            NullableIntHolder holder = new NullableIntHolder();
            ((IntExtractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof SmallIntExtractor) {
            NullableSmallIntHolder holder = new NullableSmallIntHolder();
            ((SmallIntExtractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof TinyIntExtractor) {
            NullableTinyIntHolder holder = new NullableTinyIntHolder();
            ((TinyIntExtractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof Float8Extractor) {
            NullableFloat8Holder holder = new NullableFloat8Holder();
            ((Float8Extractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof Float4Extractor) {
            NullableFloat4Holder holder = new NullableFloat4Holder();
            ((Float4Extractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof DateMilliExtractor) {
            NullableDateMilliHolder holder = new NullableDateMilliHolder();
            ((DateMilliExtractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        else if (extractor instanceof BitExtractor) {
            NullableBitHolder holder = new NullableBitHolder();
            ((BitExtractor) extractor).extract(document, holder);
            assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
            return holder.value;
        }
        return null;
    }

    @Test
    public void makeDateMilliExtractor_withLocalDateTimeFormat_parsesAndSetsHolder()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydatestringlist", Types.MinorType.DATEMILLI.getType())
                .build();

        Map<String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"mydatestringlist\" : [\n" +
                        "    \"2020-05-15T06:49:30.123\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        DateMilliExtractor extractor = (DateMilliExtractor) typeUtils.makeExtractor(mapping.getFields().get(0));
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(document, holder);

        assertEquals("Should parse local date time format", 1, holder.isSet);
    }

    @Test
    public void makeDateMilliExtractor_withInvalidDateFormat_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydatestringlist", Types.MinorType.DATEMILLI.getType())
                .build();

        Map<String, Object> document = new ObjectMapper().readValue(
                "{\n" +
                        "  \"mydatestringlist\" : [\n" +
                        "    \"invalid-date-format\"\n" +
                        "  ]\n" +
                        "}\n", HashMap.class);

        DateMilliExtractor extractor = (DateMilliExtractor) typeUtils.makeExtractor(mapping.getFields().get(0));
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(document, holder);

        assertEquals("Should set isSet to 0 for invalid date format", 0, holder.isSet);
    }

    @Test
    public void makeFactory_withUnsupportedFieldType_throwsAthenaConnectorException()
    {
        Field field = new Field("test", FieldType.nullable(Types.MinorType.INTERVALDAY.getType()), null);

        try {
            typeUtils.makeFactory(field);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException ex) {
            assertTrue("Exception message should contain is not supported",
                    ex.getMessage().contains("is not supported"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }

    @Test
    public void makeDateMilliExtractor_withZonedDateTime_parsesEpochSecondsAndNanoSeconds()
            throws Exception
    {
        String dateTimeString = "2020-05-15T06:49:30.123456789+05:00";
        long expectedMillis = java.time.ZonedDateTime.parse(dateTimeString).toInstant().toEpochMilli();

        mapping = SchemaBuilder.newBuilder()
                .addField("mydate", Types.MinorType.DATEMILLI.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("mydate", dateTimeString);

        Field dateField = mapping.findField("mydate");
        if (dateField == null) {
            fail("Field mydate not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(dateField);
        assertTrue("makeExtractor should return DateMilliExtractor for DATEMILLI field", extractor instanceof DateMilliExtractor);

        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        ((DateMilliExtractor) extractor).extract(document, holder);

        assertEquals("Should successfully parse ZonedDateTime", 1, holder.isSet);
        assertEquals("DateMilliHolder value should match parsed epoch millis", expectedMillis, holder.value);
    }

    @Test
    public void makeDateMilliExtractor_withInvalidDateTimeFormatInList_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydate", Types.MinorType.DATEMILLI.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<String> invalidDateList = new ArrayList<>();
        invalidDateList.add("invalid-date-format-xyz");
        document.put("mydate", invalidDateList);

        Field dateField = mapping.findField("mydate");
        if (dateField == null) {
            fail("Field mydate not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(dateField);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        ((DateMilliExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 on parse error", 0, holder.isSet);
    }

    @Test
    public void makeDateMilliExtractor_withInvalidDateTimeFormatString_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydate", Types.MinorType.DATEMILLI.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("mydate", "completely-invalid-date-format-xyz-123");

        Field dateField = mapping.findField("mydate");
        if (dateField == null) {
            fail("Field mydate not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(dateField);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        ((DateMilliExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 when both ZonedDateTime and LocalDateTime parsing fail", 0, holder.isSet);
    }

    @Test
    public void makeDateMilliExtractor_withNonStringNonNumberNonList_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydate", Types.MinorType.DATEMILLI.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("mydate", new HashMap<>());

        Field dateField = mapping.findField("mydate");
        if (dateField == null) {
            fail("Field mydate not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(dateField);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        ((DateMilliExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeDateMilliExtractor_withListContainingNonStringNonNumber_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydate", Types.MinorType.DATEMILLI.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("mydate", invalidList);

        Field dateField = mapping.findField("mydate");
        if (dateField == null) {
            fail("Field mydate not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(dateField);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        ((DateMilliExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeBigIntExtractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mybigint", Types.MinorType.BIGINT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("mybigint", new HashMap<>());

        Field field = mapping.findField("mybigint");
        if (field == null) {
            fail("Field mybigint not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        ((BigIntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeScaledFloatExtractor_withListContainingUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField(new Field("myscaled", new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                        ImmutableMap.of("scaling_factor", "10.0")), null))
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new Object());
        document.put("myscaled", invalidList);

        Field field = mapping.findField("myscaled");
        if (field == null) {
            fail("Field myscaled not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        ((BigIntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeScaledFloatExtractor_withListContainingMap_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField(new Field("myscaled", new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                        ImmutableMap.of("scaling_factor", "10.0")), null))
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("myscaled", invalidList);

        Field field = mapping.findField("myscaled");
        if (field == null) {
            fail("Field myscaled not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        ((BigIntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeBitExtractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myboolean", Types.MinorType.BIT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("myboolean", new HashMap<>());

        Field field = mapping.findField("myboolean");
        if (field == null) {
            fail("Field myboolean not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableBitHolder holder = new NullableBitHolder();
        ((BitExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeBitExtractor_withListContainingUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myboolean", Types.MinorType.BIT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("myboolean", invalidList);

        Field field = mapping.findField("myboolean");
        if (field == null) {
            fail("Field myboolean not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableBitHolder holder = new NullableBitHolder();
        ((BitExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeVarCharExtractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mytext", Types.MinorType.VARCHAR.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("mytext", new Object());

        Field field = mapping.findField("mytext");
        if (field == null) {
            fail("Field mytext not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        ((VarCharExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeIntExtractor_withListContainingUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myinteger", Types.MinorType.INT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("myinteger", invalidList);

        Field field = mapping.findField("myinteger");
        if (field == null) {
            fail("Field myinteger not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableIntHolder holder = new NullableIntHolder();
        ((IntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeIntExtractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myinteger", Types.MinorType.INT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("myinteger", new HashMap<>());

        Field field = mapping.findField("myinteger");
        if (field == null) {
            fail("Field myinteger not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableIntHolder holder = new NullableIntHolder();
        ((IntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeSmallIntExtractor_withListContainingUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myshort", Types.MinorType.SMALLINT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("myshort", invalidList);

        Field field = mapping.findField("myshort");
        if (field == null) {
            fail("Field myshort not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableSmallIntHolder holder = new NullableSmallIntHolder();
        ((SmallIntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeSmallIntExtractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myshort", Types.MinorType.SMALLINT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("myshort", new HashMap<>());

        Field field = mapping.findField("myshort");
        if (field == null) {
            fail("Field myshort not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableSmallIntHolder holder = new NullableSmallIntHolder();
        ((SmallIntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeTinyIntExtractor_withListContainingUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("mybyte", invalidList);

        Field field = mapping.findField("mybyte");
        if (field == null) {
            fail("Field mybyte not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableTinyIntHolder holder = new NullableTinyIntHolder();
        ((TinyIntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeTinyIntExtractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("mybyte", new HashMap<>());

        Field field = mapping.findField("mybyte");
        if (field == null) {
            fail("Field mybyte not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableTinyIntHolder holder = new NullableTinyIntHolder();
        ((TinyIntExtractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeFloat8Extractor_withListContainingUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("mydouble", invalidList);

        Field field = mapping.findField("mydouble");
        if (field == null) {
            fail("Field mydouble not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableFloat8Holder holder = new NullableFloat8Holder();
        ((Float8Extractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeFloat8Extractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("mydouble", new HashMap<>());

        Field field = mapping.findField("mydouble");
        if (field == null) {
            fail("Field mydouble not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableFloat8Holder holder = new NullableFloat8Holder();
        ((Float8Extractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }

    @Test
    public void makeFloat4Extractor_withListContainingUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myfloat", Types.MinorType.FLOAT4.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        List<Object> invalidList = new ArrayList<>();
        invalidList.add(new HashMap<>());
        document.put("myfloat", invalidList);

        Field field = mapping.findField("myfloat");
        if (field == null) {
            fail("Field myfloat not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableFloat4Holder holder = new NullableFloat4Holder();
        ((Float4Extractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported list element type", 0, holder.isSet);
    }

    @Test
    public void makeFloat4Extractor_withUnsupportedType_setsIsSetToZero()
            throws Exception
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("myfloat", Types.MinorType.FLOAT4.getType())
                .build();

        Map<String, Object> document = new HashMap<>();
        document.put("myfloat", new HashMap<>());

        Field field = mapping.findField("myfloat");
        if (field == null) {
            fail("Field myfloat not found in schema");
        }
        Extractor extractor = typeUtils.makeExtractor(field);
        NullableFloat4Holder holder = new NullableFloat4Holder();
        ((Float4Extractor) extractor).extract(document, holder);

        assertEquals("Should set isSet to 0 for unsupported type", 0, holder.isSet);
    }
}
