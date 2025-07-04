/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static com.amazonaws.athena.connectors.db2.Db2Constants.QUOTE_CHARACTER;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class Db2FederationExpressionParserTest
{
    private static final String VALUE_1 = "value1";
    
    @Mock
    private ArrowType mockArrowType;
    
    private Db2FederationExpressionParser parser;

    @Before
    public void setup()
    {
        parser = new Db2FederationExpressionParser(QUOTE_CHARACTER);
    }

    @Test
    public void writeArrayConstructorClause_whenSingleArgument_returnsSingleArgument()
    {
        assertEquals(VALUE_1, parser.writeArrayConstructorClause(mockArrowType, Collections.singletonList(VALUE_1)));
    }

    @Test
    public void writeArrayConstructorClause_whenMultipleArguments_returnsCommaSeparatedArguments()
    {
        assertEquals("value1, value2, value3", 
            parser.writeArrayConstructorClause(mockArrowType, Arrays.asList(VALUE_1, "value2", "value3")));
    }

    @Test
    public void writeArrayConstructorClause_whenEmptyArguments_returnsEmptyString()
    {
        assertEquals("", parser.writeArrayConstructorClause(mockArrowType, Collections.emptyList()));
    }

    @Test
    public void writeArrayConstructorClause_whenArgumentsHaveSpaces_returnsCommaSeparatedArgumentsWithSpaces()
    {
        assertEquals("value 1, value 2", 
            parser.writeArrayConstructorClause(mockArrowType, Arrays.asList("value 1", "value 2")));
    }

    @Test
    public void writeArrayConstructorClause_whenArgumentsHaveSpecialCharacters_returnsCommaSeparatedArgumentsWithSpecialCharacters()
    {
        assertEquals("value#1, value$2, value@3", 
            parser.writeArrayConstructorClause(mockArrowType, Arrays.asList("value#1", "value$2", "value@3")));
    }
}
