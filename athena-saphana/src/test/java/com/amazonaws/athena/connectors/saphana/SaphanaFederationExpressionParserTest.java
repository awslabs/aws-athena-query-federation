/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SaphanaFederationExpressionParserTest
{
    private final SaphanaFederationExpressionParser parser = new SaphanaFederationExpressionParser("\"");
    private final ArrowType type = new ArrowType.Utf8();
    private static final String TEST_VALUE_1 = "value1";

    @Test
    public void writeArrayConstructorClause_withEmptyList_returnsEmptyString()
    {
        String result = parser.writeArrayConstructorClause(type, Collections.emptyList());
        assertEquals("", result);
    }

    @Test
    public void writeArrayConstructorClause_withSingleElement_returnsElement()
    {
        String result = parser.writeArrayConstructorClause(type, Collections.singletonList(TEST_VALUE_1));
        assertEquals(TEST_VALUE_1, result);
    }

    @Test
    public void writeArrayConstructorClause_withMultipleElements_returnsCommaSeparatedValues()
    {
        List<String> arguments = Arrays.asList(TEST_VALUE_1, "value2", "value3");
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals("value1, value2, value3", result);
    }

    @Test
    public void writeArrayConstructorClause_withNumericValues_returnsCommaSeparatedNumbers()
    {
        ArrowType type = new ArrowType.Int(64, true);
        List<String> arguments = Arrays.asList("100", "200", "300");
 
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals("100, 200, 300", result);
    }

    @Test(expected = NullPointerException.class)
    public void writeArrayConstructorClause_withNullArguments_throwsNullPointerException()
    {
        parser.writeArrayConstructorClause(type, null);
    }

    @Test
    public void writeArrayConstructorClause_withSingleWhitespaceValue_returnsValueAsIs()
    {
        String result = parser.writeArrayConstructorClause(type, Collections.singletonList("  "));
        assertEquals("  ", result);
    }
}