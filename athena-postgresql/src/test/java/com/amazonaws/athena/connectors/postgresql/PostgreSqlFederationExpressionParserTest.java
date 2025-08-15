/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql;

import org.junit.Before;
import org.junit.Test;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PostgreSqlFederationExpressionParserTest
{
    private PostgreSqlFederationExpressionParser parser;
    private static final String QUOTE_CHAR = "\"";

    @Before
    public void setup() {
        parser = new PostgreSqlFederationExpressionParser(QUOTE_CHAR);
    }

    @Test
    public void testWriteArrayConstructorClause() {
        List<String> arguments = Arrays.asList("'value1'", "'value2'", "'value3'");
        String result = parser.writeArrayConstructorClause(new ArrowType.Utf8(), arguments);
        assertEquals("'value1', 'value2', 'value3'", result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithNumbers() {
        List<String> arguments = Arrays.asList("1", "2", "3");
        String result = parser.writeArrayConstructorClause(new ArrowType.Int(32, true), arguments);
        assertEquals("1, 2, 3", result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithEmptyList() {
        List<String> arguments = List.of();
        String result = parser.writeArrayConstructorClause(new ArrowType.Utf8(), arguments);
        assertEquals("", result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithSingleElement() {
        List<String> arguments = List.of("'single'");
        String result = parser.writeArrayConstructorClause(new ArrowType.Utf8(), arguments);
        assertEquals("'single'", result);
    }
}
