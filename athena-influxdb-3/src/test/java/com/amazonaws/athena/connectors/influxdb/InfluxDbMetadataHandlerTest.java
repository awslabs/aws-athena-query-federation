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
import com.amazonaws.athena.connectors.influxdb.InfluxDbConnectionFactory.DatabaseInfo;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfluxDbMetadataHandlerTest
{
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account",
            Collections.<String, String>emptyMap(), Collections.<String>emptyList(),
            Collections.<String, String>emptyMap());
    private BlockAllocator allocator;
    private InfluxDbConnectionFactory mockFactory;
    private InfluxDBClient mockClient;
    private InfluxDbMetadataHandler handler;

    @Before
    public void setUp() throws Exception
    {
        allocator = new BlockAllocatorImpl();
        mockFactory = mock(InfluxDbConnectionFactory.class);
        mockClient = mock(InfluxDBClient.class);
        when(mockFactory.getClient(anyString())).thenReturn(mockClient);
        when(mockFactory.getClient(isNull())).thenReturn(mockClient);
        // The real executeWithTokenRetry runs the query against a client from getClient. For these
        // tests, run the caller's lambda directly against the mock client so the query stubs apply.
        when(mockFactory.executeWithTokenRetry(any(), any())).thenAnswer(invocation -> {
            final InfluxDbConnectionFactory.InfluxDbQuery<?> query = invocation.getArgument(1);
            return query.run(mockClient);
        });

        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        config.put("influxdb_database", "testdb");

        handler = new InfluxDbMetadataHandler(
                mockFactory,
                new com.amazonaws.athena.connector.lambda.security.LocalKeyFactory(),
                mock(software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.class),
                mock(software.amazon.awssdk.services.athena.AthenaClient.class),
                "test-bucket",
                "test-prefix",
                config);
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testDoListSchemaNamesDefault() throws Exception
    {
        final ListSchemasResponse response = handler.doListSchemaNames(
                allocator,
                new ListSchemasRequest(IDENTITY, "queryId", "catalog"));

        assertEquals(1, response.getSchemas().size());
        assertTrue(response.getSchemas().contains("testdb"));
    }

    @Test
    public void testDoListSchemaNamesMultiple() throws Exception
    {
        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");

        handler = new InfluxDbMetadataHandler(
                mockFactory,
                new com.amazonaws.athena.connector.lambda.security.LocalKeyFactory(),
                mock(software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.class),
                mock(software.amazon.awssdk.services.athena.AthenaClient.class),
                "test-bucket",
                "test-prefix",
                config);

        when(mockFactory.listDatabases()).thenReturn(List.of(new DatabaseInfo("testdb"), new DatabaseInfo("metrics")));

        final ListSchemasResponse response = handler.doListSchemaNames(
                allocator,
                new ListSchemasRequest(IDENTITY, "queryId", "catalog"));

        assertEquals(2, response.getSchemas().size());
        assertTrue(response.getSchemas().contains("testdb"));
        assertTrue(response.getSchemas().contains("metrics"));
    }

    @Test
    public void testDoListSchemaNamesLowercaseConfiguredDatabase() throws Exception
    {
        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        config.put("influxdb_database", "MyDatabase");

        handler = new InfluxDbMetadataHandler(
                mockFactory,
                new com.amazonaws.athena.connector.lambda.security.LocalKeyFactory(),
                mock(software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.class),
                mock(software.amazon.awssdk.services.athena.AthenaClient.class),
                "test-bucket",
                "test-prefix",
                config);

        final ListSchemasResponse response = handler.doListSchemaNames(
                allocator,
                new ListSchemasRequest(IDENTITY, "queryId", "catalog"));

        // Athena should receive the lowercased schema name.
        assertEquals(1, response.getSchemas().size());
        assertTrue(response.getSchemas().contains("mydatabase"));
        // Athena should never receive the original, mixed-case value.
        assertFalse(response.getSchemas().contains("MyDatabase"));
    }

    @Test
    public void testDoListTables() throws Exception
    {
        final Object[][] rows = {new Object[]{"cpu"}, new Object[]{"mem"}};
        when(mockClient.query(anyString())).thenReturn(Stream.of(rows));

        final ListTablesResponse response = handler.doListTables(
                allocator,
                new ListTablesRequest(IDENTITY, "queryId", "catalog", "mydb", null,
                        ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE));

        assertEquals(2, response.getTables().size());
        final java.util.List<TableName> tables = new java.util.ArrayList<>(response.getTables());
        assertEquals("cpu", tables.get(0).getTableName());
        assertEquals("mem", tables.get(1).getTableName());
    }

    @Test
    public void testDoGetTable() throws Exception {
        when(mockFactory.resolveTableName(anyString(), any())).thenReturn("cpu");
        when(mockFactory.resolveDatabase(anyString())).thenReturn("mydb");
        final Object[][] columnRows = {
                new Object[] { "time", "TIMESTAMP" },
                new Object[] { "host", "VARCHAR" },
                new Object[] { "usage_idle", "DOUBLE" }
        };
        // First call resolves table name, second call gets columns
        when(mockClient.query(anyString(), anyMap()))
                .thenReturn(Stream.of(columnRows));

        final GetTableResponse response = handler.doGetTable(
                allocator,
                new GetTableRequest(IDENTITY, "queryId", "catalog", new TableName("mydb", "cpu"),
                        Collections.emptyMap()));

        assertEquals(3, response.getSchema().getFields().size());
        assertEquals("time", response.getSchema().getFields().get(0).getName());
        assertEquals("host", response.getSchema().getFields().get(1).getName());
        assertEquals("usage_idle", response.getSchema().getFields().get(2).getName());
    }

    @Test
    public void testClampedSplitCount()
    {
        // Within range is returned as-is.
        assertEquals(4, handlerWithSplitCount("4").clampedSplitCount());
        // Above the max is capped.
        assertEquals(16, handlerWithSplitCount("100").clampedSplitCount());
        // Zero and negative are floored to a single split.
        assertEquals(1, handlerWithSplitCount("0").clampedSplitCount());
        assertEquals(1, handlerWithSplitCount("-5").clampedSplitCount());
        // Missing config falls back to the default.
        assertEquals(8, handlerWithSplitCount(null).clampedSplitCount());
        // Non-numeric config falls back to the default.
        assertEquals(8, handlerWithSplitCount("abc").clampedSplitCount());
    }

    private InfluxDbMetadataHandler handlerWithSplitCount(final String count)
    {
        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        if (count != null) {
            config.put("query_parallelism_count", count);
        }
        return new InfluxDbMetadataHandler(
                mockFactory,
                new com.amazonaws.athena.connector.lambda.security.LocalKeyFactory(),
                mock(software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.class),
                mock(software.amazon.awssdk.services.athena.AthenaClient.class),
                "test-bucket",
                "test-prefix",
                config);
    }

    @Test
    public void testToArrowType()
    {
        assertEquals(Types.MinorType.BIGINT, InfluxDbMetadataHandler.toArrowType("BIGINT"));
        assertEquals(Types.MinorType.FLOAT8, InfluxDbMetadataHandler.toArrowType("DOUBLE"));
        assertEquals(Types.MinorType.BIT, InfluxDbMetadataHandler.toArrowType("BOOLEAN"));
        assertEquals(Types.MinorType.TIMESTAMPMILLITZ, InfluxDbMetadataHandler.toArrowType("TIMESTAMP"));
        assertEquals(Types.MinorType.VARCHAR, InfluxDbMetadataHandler.toArrowType("UNKNOWN_TYPE"));
    }
}
