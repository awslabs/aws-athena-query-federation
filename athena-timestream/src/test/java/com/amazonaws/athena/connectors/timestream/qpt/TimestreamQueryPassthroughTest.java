/*-
 * #%L
 * athena-timestream
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
package com.amazonaws.athena.connectors.timestream.qpt;

import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TimestreamQueryPassthroughTest
{
    private final TimestreamQueryPassthrough queryPassthrough = new TimestreamQueryPassthrough();

    @Test
    public void customConnectorVerifications_validSelectQuery_shouldNotThrowException()
    {
        Map<String, String> args = new HashMap<>();
        args.put(TimestreamQueryPassthrough.QUERY, "SELECT * FROM \"database\".\"table\"");

        queryPassthrough.customConnectorVerifications(args);
        assertTrue("Valid SELECT query should not throw exception", true);
    }

    @Test
    public void customConnectorVerifications_validWithQuery_shouldNotThrowException()
    {
        Map<String, String> args = new HashMap<>();
        args.put(TimestreamQueryPassthrough.QUERY, "WITH temp AS (SELECT * FROM \"database\".\"table\") SELECT * FROM temp");

        queryPassthrough.customConnectorVerifications(args);
        assertTrue("Valid WITH query should not throw exception", true);
    }


    @Test
    public void customConnectorVerifications_queryWithLastUpdateField_shouldNotThrowException()
    {
        Map<String, String> args = new HashMap<>();
        args.put(TimestreamQueryPassthrough.QUERY, "SELECT last_update, status FROM \"database\".\"table\"");

        queryPassthrough.customConnectorVerifications(args);
        assertTrue("Query with last_update field should not throw exception (word boundary prevents false positive)", true);
    }


    @Test
    public void customConnectorVerifications_queryWithCreateTimeSeriesFunction_shouldNotThrowException()
    {
        Map<String, String> args = new HashMap<>();
        args.put(TimestreamQueryPassthrough.QUERY,
                "SELECT CREATE_TIME_SERIES(time, value) AS time_series FROM \"database\".\"table\"");

        queryPassthrough.customConnectorVerifications(args);
        assertTrue("Query with CREATE_TIME_SERIES function should not throw exception (word boundary prevents false positive)", true);
    }


    @Test(expected = UnsupportedOperationException.class)
    public void customConnectorVerifications_queryStartsWithInsert_shouldThrowUnsupportedOperationException()
    {
        Map<String, String> args = new HashMap<>();
        args.put(TimestreamQueryPassthrough.QUERY, "INSERT INTO \"database\".\"table\" (col1, col2) VALUES ('val1', 'val2')");

        queryPassthrough.customConnectorVerifications(args);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void customConnectorVerifications_queryContainsUpdateKeyword_shouldThrowUnsupportedOperationException()
    {
        Map<String, String> args = new HashMap<>();
        args.put(TimestreamQueryPassthrough.QUERY, "SELECT * FROM \"database\".\"table\" WHERE type = 'UPDATE'");

        queryPassthrough.customConnectorVerifications(args);
    }

    @Test
    public void customConnectorVerifications_complexQueryWithAllValidCases_shouldNotThrowException()
    {
        Map<String, String> args = new HashMap<>();
        args.put(TimestreamQueryPassthrough.QUERY,
                "WITH filtered_data AS (" +
                "  SELECT " +
                "    time, " +
                "    last_update, " +
                "    created_at, " +
                "    CREATE_TIME_SERIES(time, value) AS time_series " +
                "  FROM \"database\".\"table\" " +
                "  WHERE created_at > '2024-01-01' " +
                "    AND last_update IS NOT NULL" +
                ") " +
                "SELECT " +
                "  time, " +
                "  last_update, " +
                "  time_series " +
                "FROM filtered_data " +
                "ORDER BY last_update DESC");

        queryPassthrough.customConnectorVerifications(args);
        assertTrue("Complex query with CTE, keyword-like fields, and Timestream functions should not throw exception", true);
    }

    @Test
    public void getFunctionArguments_whenCalled_shouldReturnQueryArgument()
    {
        assertNotNull("getFunctionArguments should not return null", queryPassthrough.getFunctionArguments());
        assertEquals("getFunctionArguments should return list with size 1", 1, queryPassthrough.getFunctionArguments().size());
        assertEquals("getFunctionArguments should contain 'QUERY'", "QUERY", queryPassthrough.getFunctionArguments().get(0));
    }
}
