/*-
 * #%L
 * athena-cloudera-hive
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
package com.amazonaws.athena.connectors.cloudera;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_QUOTE_CHARACTER;
import static org.junit.Assert.assertEquals;

public class HiveFederationExpressionParserTest
{
    private final HiveFederationExpressionParser parser = new HiveFederationExpressionParser(HIVE_QUOTE_CHARACTER);

    @Test
    public void writeArrayConstructorClause_WithMultipleElements_ReturnsCommaSeparatedString()
    {
        String result = parser.writeArrayConstructorClause(
            new ArrowType.Int(32, true),
            Arrays.asList("1", "2", "3")
        );
        assertEquals("1, 2, 3", result);
    }

    @Test
    public void writeArrayConstructorClause_WithSingleElement_ReturnsSingleElement()
    {
        String result = parser.writeArrayConstructorClause(
            new ArrowType.Utf8(),
            Collections.singletonList("'test'")
        );
        assertEquals("'test'", result);
    }

    @Test
    public void writeArrayConstructorClause_WithEmptyList_ReturnsEmptyString()
    {
        String result = parser.writeArrayConstructorClause(
            new ArrowType.Bool(),
            Collections.emptyList()
        );
        assertEquals("", result);
    }
}
