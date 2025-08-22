/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery.qpt;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BigQueryQueryPassthroughTest {

    private final BigQueryQueryPassthrough bigQueryQueryPassthrough = new BigQueryQueryPassthrough();

    @Test
    public void testGetFunctionSchema() {
        assertEquals("system", bigQueryQueryPassthrough.getFunctionSchema());
    }

    @Test
    public void testGetFunctionName() {
        assertEquals("query", bigQueryQueryPassthrough.getFunctionName());
    }

    @Test
    public void testGetFunctionArguments() {
        assertEquals(List.of("QUERY"), bigQueryQueryPassthrough.getFunctionArguments());
    }

}