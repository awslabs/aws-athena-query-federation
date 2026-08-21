/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

public class HbaseQueryPassthroughTest
{
    private static final String SYSTEM_QUERY = "SYSTEM.QUERY";
    private static final String DATABASE = "DATABASE";
    private static final String COLLECTION = "COLLECTION";
    private static final String FILTER = "FILTER";
    private static final String TEST_DB = "test_db";
    private static final String TEST_COLLECTION = "test_collection";
    private static final String TEST_FILTER = "test_filter";
    private static final String MISSING_ARGUMENT_MESSAGE = "Missing Query Passthrough Argument: ";
    private static final String SIGNATURE_MISMATCH_MESSAGE = "Function Signature doesn't match implementation's";

    private final HbaseQueryPassthrough queryPassthrough = new HbaseQueryPassthrough();

    @Test
    public void getFunctionSchema_withDefault_returnsSystem()
    {
        assertEquals("Function schema should be system", "system", queryPassthrough.getFunctionSchema());
    }

    @Test
    public void getFunctionName_withDefault_returnsQuery()
    {
        assertEquals("Function name should be query", "query", queryPassthrough.getFunctionName());
    }

    @Test
    public void getFunctionSignature_withDefault_returnsSystemQuery()
    {
        assertEquals("Function signature should be SYSTEM.QUERY", SYSTEM_QUERY, queryPassthrough.getFunctionSignature());
    }

    @Test
    public void getFunctionArguments_withDefault_returnsThreeArguments()
    {
        List<String> arguments = queryPassthrough.getFunctionArguments();
        assertNotNull("Arguments should not be null", arguments);
        assertEquals("Should have 3 arguments", 3, arguments.size());
        assertEquals("Arguments should match expected list", Arrays.asList(DATABASE, COLLECTION, FILTER), arguments);
    }

    @Test
    public void verify_withMissingDatabase_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments, MISSING_ARGUMENT_MESSAGE + DATABASE);
    }

    @Test
    public void verify_withMissingCollection_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments, MISSING_ARGUMENT_MESSAGE + COLLECTION);
    }

    @Test
    public void verify_withMissingFilter_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments, MISSING_ARGUMENT_MESSAGE + FILTER);
    }

    @Test
    public void verify_withWrongFunctionSignature_throwsIllegalArgumentExceptionForSignatureMismatch()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "WRONG.SIGNATURE");
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments, SIGNATURE_MISMATCH_MESSAGE);
    }

    @Test
    public void verify_withMissingFunctionSignature_throwsIllegalArgumentExceptionForMissingSchemaFunction()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments, SIGNATURE_MISMATCH_MESSAGE);
    }

    private void assertVerifyThrowsIllegalArgumentException(Map<String, String> engineQptArguments, String expectedMessageSubstring)
    {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                queryPassthrough.verify(engineQptArguments));
        assertTrue("Exception message should contain: " + expectedMessageSubstring,
                ex.getMessage() != null && ex.getMessage().contains(expectedMessageSubstring));
    }
}

