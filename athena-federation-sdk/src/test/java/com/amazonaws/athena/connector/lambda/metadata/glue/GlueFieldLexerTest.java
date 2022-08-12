package com.amazonaws.athena.connector.lambda.metadata.glue;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.*;

public class GlueFieldLexerTest
{
    private static final Logger logger = LoggerFactory.getLogger(GlueFieldLexerTest.class);

    @Test
    public void basicLexTest()
    {
        logger.info("basicLexTest: enter");
        String input = "ARRAY<STRING>";
        Field field = GlueFieldLexer.lex("testField", input);
        assertEquals("testField", field.getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(field.getChildren().get(0).getType()));

        logger.info("basicLexTest: exit");
    }

    @Test
    public void baseLexTest()
    {
        logger.info("baseLexTest: enter");
        String input = "INT";
        Field field = GlueFieldLexer.lex("testField", input);
        assertEquals("testField", field.getName());
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(0, field.getChildren().size());

        logger.info("baseLexTest: exit");
    }

    @Test
    public void lexTest()
    {
        logger.info("lexTest: enter");

        String input = "STRUCT <  street_address: STRUCT <    street_number: INT,    street_name: STRING,    street_type: STRING  >,  country: STRING,  postal_code: ARRAY<STRING>>";

        Field field = GlueFieldLexer.lex("testField", input);

        logger.info("lexTest: {}", field);
        assertEquals("testField", field.getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(3, field.getChildren().size());

        List<Field> level1 = field.getChildren();
        assertEquals("street_address", level1.get(0).getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(level1.get(0).getType()));
        assertEquals(3, level1.get(0).getChildren().size());

        List<Field> level2 = level1.get(0).getChildren();
        assertEquals("street_number", level2.get(0).getName());
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(level2.get(0).getType()));
        assertEquals(0, level2.get(0).getChildren().size());
        assertEquals("street_name", level2.get(1).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level2.get(1).getType()));
        assertEquals(0, level2.get(1).getChildren().size());
        assertEquals("street_type", level2.get(2).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level2.get(2).getType()));
        assertEquals(0, level2.get(2).getChildren().size());

        assertEquals("country", level1.get(1).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level1.get(1).getType()));
        assertEquals(0, level1.get(1).getChildren().size());

        assertEquals("postal_code", level1.get(2).getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(level1.get(2).getType()));
        assertEquals(1, level1.get(2).getChildren().size());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level1.get(2).getChildren().get(0).getType()));

        logger.info("lexTest: exit");
    }

    @Test
    public void arrayOfStructLexComplexTest()
    {
        logger.info("arrayOfStructLexComplexTest: enter");

        Field field = GlueFieldLexer.lex("namelist", "ARRAY<STRUCT<last:STRING,mi:STRING,first:STRING>>");

        logger.info("lexTest: {}", field);

        assertEquals("namelist", field.getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(field.getType()));
        Field child = field.getChildren().get(0);
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(child.getType()));
        assertEquals(3, child.getChildren().size());
        assertEquals("last", child.getChildren().get(0).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(child.getChildren().get(0).getType()));
        assertEquals("mi", child.getChildren().get(1).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(child.getChildren().get(1).getType()));
        assertEquals("first", child.getChildren().get(2).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(child.getChildren().get(2).getType()));

        logger.info("arrayOfStructLexComplexTest: exit");
    }

    @Test
    public void nestedArrayLexComplexTest()
    {
        logger.info("nestedArrayLexComplexTest: enter");

        Field field = GlueFieldLexer.lex("namelist", "ARRAY<ARRAY<STRUCT<last:STRING,mi:STRING,first:STRING, aliases:ARRAY<STRING>>>>");

        logger.info("lexTest: {}", field);

        assertEquals("namelist", field.getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(field.getType()));

        Field level1 = field.getChildren().get(0);
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(level1.getType()));

        Field level2 = level1.getChildren().get(0);
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(level2.getType()));
        assertEquals(4, level2.getChildren().size());
        assertEquals("last", level2.getChildren().get(0).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level2.getChildren().get(0).getType()));
        assertEquals("mi", level2.getChildren().get(1).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level2.getChildren().get(1).getType()));
        assertEquals("first", level2.getChildren().get(2).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level2.getChildren().get(2).getType()));
        assertEquals("aliases", level2.getChildren().get(3).getName());

        Field level3 = level2.getChildren().get(3);
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(level3.getType()));
        assertEquals("aliases", level3.getChildren().get(0).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level3.getChildren().get(0).getType()));

        logger.info("nestedArrayLexComplexTest: exit");
    }

    @Test
    public void multiArrayStructLexComplexTest()
    {
        logger.info("multiArrayStructLexComplexTest: enter");

        Field field = GlueFieldLexer.lex("movie_info", "STRUCT<actors:ARRAY<STRING>,genre:ARRAY<STRING>>");

        logger.info("lexTest: {}", field);

        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals("movie_info", field.getName());
        assertEquals(2, field.getChildren().size());

        Field array1 = field.getChildren().get(0);
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(array1.getType()));
        assertEquals("actors", array1.getChildren().get(0).getName());

        Field array2 = field.getChildren().get(1);
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(array2.getType()));
        assertEquals("genre", array2.getChildren().get(0).getName());

        logger.info("multiArrayStructLexComplexTest: exit");
    }

    @Test
    public void lexListOfStructTest()
    {
        logger.info("lexListOfStructTest: enter");

        String input = "ARRAY<STRUCT<time:timestamp, measure_value\\:\\:double:double>>";

        Field field = GlueFieldLexer.lex("testField", input);

        logger.info("lexListOfStructTest: {}", field);
        assertEquals("testField", field.getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(1, field.getChildren().size());

        List<Field> level1 = field.getChildren();
        assertEquals("testField", level1.get(0).getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(level1.get(0).getType()));
        assertEquals(2, level1.get(0).getChildren().size());

        List<Field> level2 = level1.get(0).getChildren();
        assertEquals("time", level2.get(0).getName());
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(level2.get(0).getType()));
        assertEquals(0, level2.get(0).getChildren().size());
        assertEquals("measure_value::double", level2.get(1).getName());
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(level2.get(1).getType()));

        logger.info("lexListOfStructTest: exit");
    }

    @Test
    public void lexStructListChildAndStructChildTest()
    {
        String input = "struct<somearrfield:array<set<string>>,mapinner:struct<numberset_deep:set<bigint>>>";
        Field field = GlueFieldLexer.lex("testAsdf", input);
        logger.info("lexStructListChildAndStructChildTest: {}", field);

        assertEquals("testAsdf", field.getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(2, field.getChildren().size());

        List<Field> level1 = field.getChildren();
        assertEquals("somearrfield", level1.get(0).getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(level1.get(0).getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(level1.get(0).getChildren().get(0).getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(level1.get(0).getChildren().get(0).getChildren().get(0).getType()));

        assertEquals("mapinner", level1.get(1).getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(level1.get(1).getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(level1.get(1).getChildren().get(0).getType()));
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(level1.get(1).getChildren().get(0).getChildren().get(0).getType()));
    }

    @Test
    public void lexExtraInnerClosingFieldsTest()
    {
        // Unfortunately we are on Java 11 so we don't have block quotes
        String input = "struct<" +
            "somearrfield0:array<set<struct<somefield:string,someset:set<string>>>>," +
            "mapinner0:struct<numberset_deep:set<bigint>>," +
            "somearrfield1:array<set<struct<somefield:string,someset:set<string>>>>," +
            "mapinner1:struct<numberset_deep:set<bigint>>," +
            "somearrfield2:array<set<struct<somefield:string,someset:set<string>>>>," +
            "mapinner2:struct<numberset_deep:set<bigint>> >";

        Field field = GlueFieldLexer.lex("testAsdf2", input);
        logger.info("lexExtraInnerClosingFieldsTest: {}", field);

        assertEquals("testAsdf2", field.getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(6, field.getChildren().size());

        List<Field> level1 = field.getChildren();

        for (int i = 0; i < 3; ++i) {
            int somearrFieldIdx = i * 2;
            Field somearrField = level1.get(somearrFieldIdx);
            assertEquals("somearrfield" + i, somearrField.getName());
            assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(somearrField.getType()));
            assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(somearrField.getChildren().get(0).getType()));
            assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(somearrField.getChildren().get(0).getChildren().get(0).getType()));

            Field innerAsdfStruct = somearrField.getChildren().get(0).getChildren().get(0);
            assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(innerAsdfStruct.getChildren().get(0).getType()));
            assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(innerAsdfStruct.getChildren().get(1).getType()));
            assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(innerAsdfStruct.getChildren().get(1).getChildren().get(0).getType()));

            int mapinnerIdx = (i*2) + 1;
            Field mapinnerField = level1.get(mapinnerIdx);
            assertEquals("mapinner" + i, mapinnerField.getName());
            assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(mapinnerField.getType()));
            assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(mapinnerField.getChildren().get(0).getType()));
            assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(mapinnerField.getChildren().get(0).getChildren().get(0).getType()));
        }

    }

    @Test
    public void basicLexDecimalTest()
    {
        logger.info("basicLexDecimalTest: enter");
        String input1 = "DECIMAL(13,7)";
        Field field1 = GlueFieldLexer.lex("testField1", input1);
        // 128 bits is the default for Arrow Decimal if not specified
        assertEquals("testField1: Decimal(13, 7, 128)", field1.toString());

        String input2 = "DECIMAL(13,7,8)";
        Field field2 = GlueFieldLexer.lex("testField2", input2);
        assertEquals("testField2: Decimal(13, 7, 8)", field2.toString());

        String input3 = "DECIMAL";
        Field field3 = GlueFieldLexer.lex("testField3", input3);
        // This is what the default params are
        assertEquals("testField3: Decimal(38, 18, 128)", field3.toString());
    }

    @Test
    public void lexExtraInnerClosingFieldsDecimalsTest()
    {
        // Unfortunately we are on Java 11 so we don't have block quotes
        String input = "struct<" +
            "somearrfield0:array<set<struct<somefield:decimal(38,9),someset:set<decimal(11,7)>>>>," +
            "mapinner0:struct<numberset_deep:set<decimal(23,3,8)>>," +
            "somearrfield1:array<set<struct<somefield:decimal(38,9),someset:set<decimal(11,7)>>>>," +
            "mapinner1:struct<numberset_deep:set<decimal(23,3,16)>>," +
            "somearrfield2:array<set<struct<somefield:decimal(38,9),someset:set<decimal(11,7)>>>>," +
            "mapinner2:struct<numberset_deep:set<decimal(23,3,32)>>>";

        Field field = GlueFieldLexer.lex("testAsdf2", input);

        String expectedFieldToString = "testAsdf2: " +
            "Struct<somearrfield0: List<somearrfield0: List<somearrfield0: Struct<somefield: Decimal(38, 9, 128), someset: List<someset: Decimal(11, 7, 128)>>>>, mapinner0: Struct<numberset_deep: List<numberset_deep: Decimal(23, 3, 8)>>, " +
            "somearrfield1: List<somearrfield1: List<somearrfield1: Struct<somefield: Decimal(38, 9, 128), someset: List<someset: Decimal(11, 7, 128)>>>>, mapinner1: Struct<numberset_deep: List<numberset_deep: Decimal(23, 3, 16)>>, " +
            "somearrfield2: List<somearrfield2: List<somearrfield2: Struct<somefield: Decimal(38, 9, 128), someset: List<someset: Decimal(11, 7, 128)>>>>, mapinner2: Struct<numberset_deep: List<numberset_deep: Decimal(23, 3, 32)>>>";

        // Just directly compare against the string, its pointless to write code to compare the fields individually
        assertEquals(expectedFieldToString, field.toString());
    }

    @Test
    public void lexMapTest()
    {
        // Disable this test if MAP_DISABLED
        if (GlueFieldLexer.MAP_DISABLED) {
            return;
        }

        // complex case
        String input = "map<array<int>, map<string, string>>";
        Field field = GlueFieldLexer.lex("SomeMap", input);
        String expectedFieldToString = "SomeMap: Map(false)<ENTRIES: Struct<key: List<key: Int(32, true)> not null, value: Map(false)<ENTRIES: Struct<key: Utf8 not null, value: Utf8> not null>> not null>";
        assertEquals(expectedFieldToString, field.toString());

        // Extra complex case
        String innerStruct = "struct<" +
            "somearrfield0:array<set<struct<somefield:decimal(38,9),someset:set<decimal(11,7)>>>>," +
            "mapinner0:struct<numberset_deep:set<decimal(23,3,8)>>" +
            ">";
        String input2 = "map<" + innerStruct + ", map<string, " + innerStruct + ">>";
        Field field2 = GlueFieldLexer.lex("SomeMap2", input2);
        String expectedFieldToString2 = "SomeMap2: Map(false)<ENTRIES: Struct<key: Struct<somearrfield0: List<somearrfield0: List<somearrfield0: Struct<somefield: Decimal(38, 9, 128), someset: List<someset: Decimal(11, 7, 128)>>>>, mapinner0: Struct<numberset_deep: List<numberset_deep: Decimal(23, 3, 8)>>> not null, value: Map(false)<ENTRIES: Struct<key: Utf8 not null, value: Struct<somearrfield0: List<somearrfield0: List<somearrfield0: Struct<somefield: Decimal(38, 9, 128), someset: List<someset: Decimal(11, 7, 128)>>>>, mapinner0: Struct<numberset_deep: List<numberset_deep: Decimal(23, 3, 8)>>>> not null>> not null>";
        assertEquals(expectedFieldToString2, field2.toString());
    }
}

