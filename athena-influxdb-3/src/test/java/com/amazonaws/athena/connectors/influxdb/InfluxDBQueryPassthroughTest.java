/*-
 * #%L
 * athena-influxdb
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
package com.amazonaws.athena.connectors.influxdb;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InfluxDBQueryPassthroughTest
{
    private final InfluxDBQueryPassthrough signature = new InfluxDBQueryPassthrough();

    @Test
    public void testSignature()
    {
        assertEquals("system", signature.getFunctionSchema());
        assertEquals("query", signature.getFunctionName());
        assertEquals("SYSTEM.QUERY", signature.getFunctionSignature());
        assertTrue(signature.getFunctionArguments().contains("DATABASE"));
        assertTrue(signature.getFunctionArguments().contains("QUERY"));
    }

    private Map<String, String> validArgs()
    {
        final Map<String, String> args = new HashMap<>();
        args.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        args.put(InfluxDBQueryPassthrough.DATABASE, "mydb");
        args.put(InfluxDBQueryPassthrough.QUERY, "SELECT * FROM cpu");
        return args;
    }

    @Test
    public void testVerifyAcceptsValidArguments()
    {
        signature.verify(validArgs()); // should not throw
    }

    @Test
    public void testVerifyRejectsWrongFunctionSignature()
    {
        final Map<String, String> args = validArgs();
        args.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "OTHER.FUNC");
        try {
            signature.verify(args);
            fail("expected verify to reject a mismatched function signature");
        }
        catch (final RuntimeException expected) {
            // ok
        }
    }

    @Test
    public void testVerifyRejectsMissingArgument()
    {
        final Map<String, String> args = validArgs();
        args.remove(InfluxDBQueryPassthrough.QUERY);
        try {
            signature.verify(args);
            fail("expected verify to reject missing QUERY argument");
        }
        catch (final RuntimeException expected) {
            // ok
        }
    }

    @Test
    public void testVerifyDatabaseNameArgument()
    {
        final Map<String, String> args = validArgs();

        // Expect pass default.
        String databaseName = "test-db";
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        signature.customConnectorVerifications(args);

        // Expect pass with 64 characters.
        databaseName = "a".repeat(64);
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        signature.customConnectorVerifications(args);

        // Expect pass with all allowed characters.
        databaseName = "test-db/-_0123456789";
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        signature.customConnectorVerifications(args);

        // Expected failure when starting with _.
        databaseName = "_test-db";
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        try {
            signature.customConnectorVerifications(args);
            fail("expected verify to reject database name argument starting with _");
        }
        catch (final RuntimeException expected) {
            // ok
        }

        // Expected failure when starting with -. Names must start with a number or a letter.
        databaseName = "-test-db";
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        try {
            signature.customConnectorVerifications(args);
            fail("expected verify to reject database name argument starting with -");
        }
        catch (final RuntimeException expected) {
            // ok
        }

        // Expected failure when starting with /. Names must start with a number or a letter.
        databaseName = "/test-db";
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        try {
            signature.customConnectorVerifications(args);
            fail("expected verify to reject database name argument starting with /");
        }
        catch (final RuntimeException expected) {
            // ok
        }

        // Expected failure when longer than 64 characters.
        databaseName = "a".repeat(65);
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        try {
            signature.customConnectorVerifications(args);
            fail("expected verify to reject database name longer than 64 characters");
        }
        catch (final RuntimeException expected) {
            // ok
        }

        // Expected failure when including disallowed characters.
        databaseName = "test-db\\!$?";
        args.put(InfluxDBQueryPassthrough.DATABASE, databaseName);
        try {
            signature.customConnectorVerifications(args);
            fail("expected verify to reject database name with disallowed characters");
        }
        catch (final RuntimeException expected) {
            // ok
        }
    }
}
