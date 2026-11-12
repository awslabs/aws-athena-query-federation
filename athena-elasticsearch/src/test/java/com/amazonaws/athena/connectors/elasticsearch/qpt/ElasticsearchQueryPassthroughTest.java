/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.athena.connectors.elasticsearch.qpt;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This class is used to test the ElasticsearchQueryPassthrough class.
 */
public class ElasticsearchQueryPassthroughTest
{
    private static final String EXPECTED_SCHEMA_NAME = "system";
    private static final String EXPECTED_FUNCTION_NAME = "query";
    private static final String SCHEMA_ARG = "SCHEMA";
    private static final String INDEX_ARG = "INDEX";
    private static final String QUERY_ARG = "QUERY";

    private ElasticsearchQueryPassthrough queryPassthrough;

    @Before
    public void setUp()
    {
        queryPassthrough = new ElasticsearchQueryPassthrough();
    }

    @Test
    public void getFunctionSchema_returnsSystemSchema()
    {
        String schema = queryPassthrough.getFunctionSchema();

        assertEquals("Schema should be 'system'", EXPECTED_SCHEMA_NAME, schema);
    }

    @Test
    public void getFunctionName_returnsQueryName()
    {
        String functionName = queryPassthrough.getFunctionName();

        assertEquals("Function name should be 'query'", EXPECTED_FUNCTION_NAME, functionName);
    }

    @Test
    public void getFunctionArguments_returnsExpectedArguments()
    {
        List<String> arguments = queryPassthrough.getFunctionArguments();

        assertNotNull("getFunctionArguments() should not return null", arguments);
        assertEquals("Should have 3 arguments", 3, arguments.size());
        assertEquals("First argument should be SCHEMA", SCHEMA_ARG, arguments.get(0));
        assertEquals("Second argument should be INDEX", INDEX_ARG, arguments.get(1));
        assertEquals("Third argument should be QUERY", QUERY_ARG, arguments.get(2));
    }

    @Test
    public void getLogger_returnsLogger()
    {
        Logger logger = queryPassthrough.getLogger();

        assertNotNull("getLogger() should not return null", logger);
    }
}
