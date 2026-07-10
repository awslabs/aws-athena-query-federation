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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration test that runs against a local InfluxDB 3 Core container.
 */
public class InfluxDbLocalIntegrationTest
{
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account",
            Collections.<String, String>emptyMap(), Collections.<String>emptyList(),
            Collections.<String, String>emptyMap());

    private BlockAllocator allocator;
    private InfluxDbMetadataHandler handler;
    private static Map<String, String> configOptions;
    @SuppressWarnings("rawtypes")
    private static GenericContainer influxDBV3Container;
    private static String influxDBV3Token;
    private static String host = "http://localhost:8181";
    private static String database = "testdb";

    @BeforeClass
    @SuppressWarnings({"resource", "rawtypes"})
    public static void startContainer() throws Exception
    {
        // InfluxDB v3 core setup.
        influxDBV3Container = new GenericContainer(DockerImageName.parse("influxdb:3-core"))
                .withExposedPorts(8181)
                .withCommand(
                        "influxdb3", "serve",
                        "--node-id=my-node-0",
                        "--object-store=file",
                        "--data-dir=/var/lib/influxdb3/data")
                .waitingFor(Wait.forLogMessage(".*startup time.*", 1));
        influxDBV3Container.start();

        // withExposedPorts maps 8181 to a random host port; build the URL from the
        // container.
        host = "http://" + influxDBV3Container.getHost() + ":" + influxDBV3Container.getMappedPort(8181);

        // Create an admin token.
        final ExecResult tokenCreationResult = influxDBV3Container.execInContainer(
                "influxdb3", "create", "token", "--admin");
        if (tokenCreationResult.getExitCode() == 1) {
            throw new RuntimeException("Failed to create new admin token in InfluxDB v3 container");
        }

        // Get admin token.
        // Tokens are emitted inside an ANSI-colored message. Strip the escape codes,
        // then extract the token.
        final Pattern ansiEscape = Pattern.compile("\\x1B(?:[@-Z\\\\-_]|\\[[0-?]*[ -/]*[@-~])");
        final String decoded = tokenCreationResult.getStdout();
        final String cleanOutput = ansiEscape.matcher(decoded).replaceAll("");

        final Matcher tokenMatcher = Pattern.compile("Token:\\s*(\\S+)").matcher(cleanOutput);
        if (!tokenMatcher.find()) {
            throw new RuntimeException("Unable to extract InfluxDB v3 token from container logs");
        }
        influxDBV3Token = tokenMatcher.group(1);

        // Create test database.
        final ExecResult databaseCreationResult = influxDBV3Container.execInContainer(
                "influxdb3", "create", "database", "--token", influxDBV3Token, database);
        if (databaseCreationResult.getExitCode() == 1) {
            throw new RuntimeException("Failed to create database " + database);
        }

        seedTestData();

        // Set config options that are necessary for the connector.
        configOptions = new HashMap<>();
        configOptions.put("INFLUXDB3_HOST_URL", host);
        configOptions.put("INFLUXDB3_AUTH_TOKEN", influxDBV3Token);
        configOptions.put("influxdb_database", database);
        configOptions.put("spill_bucket", "test-bucket");
        configOptions.put("spill_prefix", "test-prefix");
    }

    private static void seedTestData() throws Exception
    {
        try (InfluxDBClient writeClient = InfluxDBClient.getInstance(host, influxDBV3Token.toCharArray(), database)) {
            final Instant now = Instant.now();
            writeClient.writePoints(List.of(
                    Point.measurement("cpu")
                            .setTag("host", "server1")
                            .setField("usage_idle", 95.0)
                            .setTimestamp(now),
                    Point.measurement("mem")
                            .setTag("host", "server1")
                            .setField("used_percent", 42.0)
                            .setTimestamp(now)));

            // InfluxDB 3 buffers writes briefly; poll until cpu/mem are queryable.
            final String probe = "SELECT table_name FROM information_schema.tables "
                    + "WHERE table_schema = 'iox' AND table_name IN ('cpu', 'mem')";
            for (int attempt = 0; attempt < 20; attempt++) {
                try (Stream<Object[]> rows = writeClient.query(probe)) {
                    if (rows.count() >= 2) {
                        return;
                    }
                }
            }
            Thread.sleep(250);
        }
        throw new RuntimeException("Seeded cpu/mem tables did not become queryable in time");
    }

    @Before
    public void setUpHandler()
    {
        allocator = new BlockAllocatorImpl();
        handler = new InfluxDbMetadataHandler(configOptions);
    }

    @After
    public void tearDownHandler()
    {
        if (allocator != null) {
            allocator.close();
        }
    }

    @AfterClass
    public static void stopContainer()
    {
        if (influxDBV3Container != null) {
            influxDBV3Container.close();
        }
    }

    @Test
    public void testListSchemasDefault() throws Exception
    {
        final ListSchemasResponse response = handler.doListSchemaNames(
                allocator,
                new ListSchemasRequest(IDENTITY, "queryId", "catalog"));

        System.out.println("Schemas: " + response.getSchemas());
        assertFalse("Should return at least one schema", response.getSchemas().isEmpty());
        assertTrue("Should contain testdb", response.getSchemas().contains("testdb"));
    }

    @Test
    public void testListSchemasMultiple() throws Exception
    {
        // Set up config options, leaving out "influxdb_database". Doing this causes
        // all databases to be discoverable.
        final HashMap<String, String> newConfigOptions = new HashMap<>();
        newConfigOptions.put("INFLUXDB3_HOST_URL", host);
        newConfigOptions.put("INFLUXDB3_AUTH_TOKEN", influxDBV3Token);
        newConfigOptions.put("spill_bucket", "test-bucket");
        newConfigOptions.put("spill_prefix", "test-prefix");
        handler = new InfluxDbMetadataHandler(newConfigOptions);

        final String newDatabase = "extratestdb";
        final ExecResult databaseCreationResult = influxDBV3Container.execInContainer(
                "influxdb3", "create", "database", "--token", influxDBV3Token, newDatabase);
        if (databaseCreationResult.getExitCode() == 1) {
            throw new RuntimeException("Failed to create database " + newDatabase);
        }

        final ListSchemasResponse response = handler.doListSchemaNames(
                allocator,
                new ListSchemasRequest(IDENTITY, "queryId", "catalog"));

        System.out.println("Schemas: " + response.getSchemas());
        assertFalse("Should return at least two schemas", response.getSchemas().isEmpty());
        assertTrue("Should contain " + database, response.getSchemas().contains(database));
        assertTrue("Should contain " + newDatabase, response.getSchemas().contains(newDatabase));
    }

    @Test
    public void testListTables() throws Exception
    {
        final ListTablesResponse response = handler.doListTables(
                allocator,
                new ListTablesRequest(IDENTITY, "queryId", "catalog", "testdb",
                        null, ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE));

        System.out.println("Tables: " + response.getTables());
        assertFalse("Should return at least one table", response.getTables().isEmpty());

        final boolean hasCpu = response.getTables().stream()
                .anyMatch(t -> "cpu".equals(t.getTableName()));
        final boolean hasMem = response.getTables().stream()
                .anyMatch(t -> "mem".equals(t.getTableName()));
        assertTrue("Should contain cpu table", hasCpu);
        assertTrue("Should contain mem table", hasMem);
    }

    @Test
    public void testGetTableSchema() throws Exception
    {
        final GetTableResponse response = handler.doGetTable(
                allocator,
                new GetTableRequest(IDENTITY, "queryId", "catalog",
                        new TableName("testdb", "cpu"), Collections.emptyMap()));

        System.out.println("Schema for cpu:");
        for (final Field field : response.getSchema().getFields()) {
            final Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
            System.out.println("  " + field.getName() + " -> " + type);
        }

        assertTrue("Should have fields", response.getSchema().getFields().size() > 0);

        // Verify expected columns exist with correct types
        final Map<String, Types.MinorType> columns = new HashMap<>();
        for (final Field f : response.getSchema().getFields()) {
            columns.put(f.getName(), Types.getMinorTypeForArrowType(f.getType()));
        }

        assertTrue("Should have 'time' column", columns.containsKey("time"));
        assertEquals("time should be TIMESTAMPMILLITZ", Types.MinorType.TIMESTAMPMILLITZ, columns.get("time"));

        assertTrue("Should have 'host' column", columns.containsKey("host"));
        assertEquals("host (tag) should be VARCHAR", Types.MinorType.VARCHAR, columns.get("host"));

        assertTrue("Should have 'usage_idle' column", columns.containsKey("usage_idle"));
        assertEquals("usage_idle should be FLOAT8", Types.MinorType.FLOAT8, columns.get("usage_idle"));
    }
}
