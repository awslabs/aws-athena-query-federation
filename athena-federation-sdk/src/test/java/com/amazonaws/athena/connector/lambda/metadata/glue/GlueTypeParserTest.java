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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class GlueTypeParserTest
{
    private static final Logger logger = LoggerFactory.getLogger(GlueTypeParserTest.class);

    @Test
    public void parseTest()
    {
        logger.info("parseTest: enter");
        String input = "STRUCT <  street_address: STRUCT <    street_number: INT,    street_name: STRING,    street_type: STRING  >,  country: STRING,  postal_code: ARRAY<STRING>>";
        List<GlueTypeParser.Token> expectedTokens = new ArrayList<>();
        expectedTokens.add(new GlueTypeParser.Token("STRUCT", GlueTypeParser.FIELD_START, 8));
        expectedTokens.add(new GlueTypeParser.Token("street_address", GlueTypeParser.FIELD_DIV, 25));
        expectedTokens.add(new GlueTypeParser.Token("STRUCT", GlueTypeParser.FIELD_START, 34));
        expectedTokens.add(new GlueTypeParser.Token("street_number", GlueTypeParser.FIELD_DIV, 52));
        expectedTokens.add(new GlueTypeParser.Token("INT", GlueTypeParser.FIELD_SEP, 57));
        expectedTokens.add(new GlueTypeParser.Token("street_name", GlueTypeParser.FIELD_DIV, 73));
        expectedTokens.add(new GlueTypeParser.Token("STRING", GlueTypeParser.FIELD_SEP, 81));
        expectedTokens.add(new GlueTypeParser.Token("street_type", GlueTypeParser.FIELD_DIV, 97));
        expectedTokens.add(new GlueTypeParser.Token("STRING", GlueTypeParser.FIELD_END, 107));
        expectedTokens.add(new GlueTypeParser.Token("", GlueTypeParser.FIELD_SEP, 108));
        expectedTokens.add(new GlueTypeParser.Token("country", GlueTypeParser.FIELD_DIV, 118));
        expectedTokens.add(new GlueTypeParser.Token("STRING", GlueTypeParser.FIELD_SEP, 126));
        expectedTokens.add(new GlueTypeParser.Token("postal_code", GlueTypeParser.FIELD_DIV, 140));
        expectedTokens.add(new GlueTypeParser.Token("ARRAY", GlueTypeParser.FIELD_START, 147));
        expectedTokens.add(new GlueTypeParser.Token("STRING", GlueTypeParser.FIELD_END, 154));
        expectedTokens.add(new GlueTypeParser.Token("", GlueTypeParser.FIELD_END, 155));
        GlueTypeParser parser = new GlueTypeParser(input);
        int pos = 0;
        while (parser.hasNext()) {
            GlueTypeParser.Token next = parser.next();
            logger.info("parseTest: {} => {}", next.getValue(), next.getMarker());
            assertEquals(expectedTokens.get(pos++), next);
        }
        logger.info("parseTest: exits");
    }

    @Test
    public void parseTestSimple()
    {
        logger.info("parseTestSimple: enter");
        String input = "string";
        List<GlueTypeParser.Token> expectedTokens = new ArrayList<>();
        expectedTokens.add(new GlueTypeParser.Token("string", null, 6));
        GlueTypeParser parser = new GlueTypeParser(input);
        int pos = 0;
        while (parser.hasNext()) {
            GlueTypeParser.Token next = parser.next();
            logger.info("parseTest: {} => {}", next.getValue(), next.getMarker());
            assertEquals(expectedTokens.get(pos++), next);
        }
        logger.info("parseTestSimple: exits");
    }

    @Test
    public void parseTestComplex()
    {
        logger.info("parseTestComplex: enter");
        String input = "ARRAY<STRUCT<time:timestamp, measure_value\\:\\:double:double>>";
        List<GlueTypeParser.Token> expectedTokens = new ArrayList<>();
        expectedTokens.add(new GlueTypeParser.Token("ARRAY", GlueTypeParser.FIELD_START, 6));
        expectedTokens.add(new GlueTypeParser.Token("STRUCT", GlueTypeParser.FIELD_START, 13));
        expectedTokens.add(new GlueTypeParser.Token("time", GlueTypeParser.FIELD_DIV, 18));
        expectedTokens.add(new GlueTypeParser.Token("timestamp", GlueTypeParser.FIELD_SEP, 28));
        expectedTokens.add(new GlueTypeParser.Token("measure_value::double", GlueTypeParser.FIELD_DIV, 53));
        expectedTokens.add(new GlueTypeParser.Token("double", GlueTypeParser.FIELD_END, 60));
        expectedTokens.add(new GlueTypeParser.Token("", GlueTypeParser.FIELD_END, 61));
        GlueTypeParser parser = new GlueTypeParser(input);
        int pos = 0;
        while (parser.hasNext()) {
            GlueTypeParser.Token next = parser.next();
            logger.info("parseTest: {} => {} @ {}", next.getValue(), next.getMarker(), next.getPos());
            assertEquals(expectedTokens.get(pos++), next);
        }
        logger.info("parseTestSimple: exits");
    }
}
