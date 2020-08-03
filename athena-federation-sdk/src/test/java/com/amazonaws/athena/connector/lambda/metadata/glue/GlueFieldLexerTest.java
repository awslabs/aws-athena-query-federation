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
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.*;

public class GlueFieldLexerTest
{
    private static final Logger logger = LoggerFactory.getLogger(GlueFieldLexerTest.class);

    private static final String INPUT1 = "STRUCT <  street_address: STRUCT <    street_number: INT,    street_name: STRING,    street_type: STRING  >,  country: STRING,  postal_code: ARRAY<STRING>>";

    private static final String INPUT2 = "ARRAY<STRING>";

    private static final String INPUT3 = "INT";

    private static final String INPUT4 = "ARRAY<STRUCT<last:STRING,mi:STRING,first:STRING>>";

    private static final String INPUT5 = "ARRAY<ARRAY<STRUCT<last:STRING,mi:STRING,first:STRING, aliases:ARRAY<STRING>>>>";

    @Test
    public void basicLexTest()
    {
        logger.info("basicLexTest: enter");

        Field field = GlueFieldLexer.lex("testField", INPUT2);
        assertEquals("testField", field.getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(field.getChildren().get(0).getType()));

        logger.info("basicLexTest: exit");
    }

    @Test
    public void baseLexTest()
    {
        logger.info("baseLexTest: enter");

        Field field = GlueFieldLexer.lex("testField", INPUT3);
        assertEquals("testField", field.getName());
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(field.getType()));
        assertEquals(0, field.getChildren().size());

        logger.info("baseLexTest: exit");
    }

    @Test
    public void lexTest()
    {
        logger.info("lexTest: enter");

        Field field = GlueFieldLexer.lex("testField", INPUT1);

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
    public void arrayOfStructLexComplexTest() {
        logger.info("arrayOfStructLexComplexTest: enter");

        Field field = GlueFieldLexer.lex("namelist", INPUT4);

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
    public void nestedArrayLexComplexTest() {
        logger.info("nestedArrayLexComplexTest: enter");

        Field field = GlueFieldLexer.lex("namelist", INPUT5);

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
}
