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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.influxdb.InfluxDBConnectionFactory.DatabaseInfo;
import com.influxdb.v3.client.InfluxDBClient;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfluxDBMetadataHandlerTest
{
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account",
            Collections.<String, String>emptyMap(), Collections.<String>emptyList(),
            Collections.<String, String>emptyMap());
    private BlockAllocator allocator;
    private InfluxDBConnectionFactory mockFactory;
    private InfluxDBClient mockClient;
    private InfluxDBMetadataHandler handler;

    @Before
    public void setUp() throws Exception
    {
        allocator = new BlockAllocatorImpl();
        mockFactory = mock(InfluxDBConnectionFactory.class);
        mockClient = mock(InfluxDBClient.class);
        when(mockFactory.getClient(anyString())).thenReturn(mockClient);
        when(mockFactory.getClient(isNull())).thenReturn(mockClient);
        // The real executeWithTokenRetry runs the query against a client from getClient. For these
        // tests, run the caller's lambda directly against the mock client so the query stubs apply.
        when(mockFactory.executeWithTokenRetry(any(), any())).thenAnswer(invocation -> {
            final InfluxDBConnectionFactory.InfluxDBQuery<?> query = invocation.getArgument(1);
            return query.run(mockClient);
        });

        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        config.put("influxdb_database", "testdb");

        handler = new InfluxDBMetadataHandler(
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

        handler = new InfluxDBMetadataHandler(
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

        handler = new InfluxDBMetadataHandler(
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

    private InfluxDBMetadataHandler handlerWithSplitCount(final String count)
    {
        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        if (count != null) {
            config.put("query_parallelism_count", count);
        }
        return new InfluxDBMetadataHandler(
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
        assertEquals(Types.MinorType.BIGINT, InfluxDBMetadataHandler.toArrowType("BIGINT"));
        assertEquals(Types.MinorType.FLOAT8, InfluxDBMetadataHandler.toArrowType("DOUBLE"));
        assertEquals(Types.MinorType.BIT, InfluxDBMetadataHandler.toArrowType("BOOLEAN"));
        assertEquals(Types.MinorType.TIMESTAMPMILLITZ, InfluxDBMetadataHandler.toArrowType("TIMESTAMP"));
        assertEquals(Types.MinorType.VARCHAR, InfluxDBMetadataHandler.toArrowType("UNKNOWN_TYPE"));
    }

    @Test
    public void testEnhancePartitionSchemaAddsTimeColumnsOnlyWhenParallelismEnabled()
    {
        final SchemaBuilder onBuilder = SchemaBuilder.newBuilder();
        handlerWithParallelism("true", "4").enhancePartitionSchema(onBuilder,
                mock(GetTableLayoutRequest.class));
        final Schema on = onBuilder.build();
        assertTrue(on.findField("time_lower") != null && on.findField("time_upper") != null);

        final SchemaBuilder offBuilder = SchemaBuilder.newBuilder();
        handlerWithParallelism("false", "4").enhancePartitionSchema(offBuilder,
                mock(GetTableLayoutRequest.class));
        assertTrue(offBuilder.build().getFields().isEmpty());
    }

    @Test
    public void testExtractTimeRangeReturnsNullWithoutBounds()
    {
        final Constraints empty = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);
        assertNull(InfluxDBMetadataHandler.extractTimeRange(empty));
    }

    @Test
    public void testGetPartitionsWritesSingleUnboundedPartitionWhenNoTimeRange() throws Exception
    {
        final InfluxDBMetadataHandler pHandler = handlerWithParallelism("true", "8");
        final Schema partSchema = SchemaBuilder.newBuilder()
                        .addField("time_lower", Types.MinorType.BIGINT.getType())
                        .addField("time_upper", Types.MinorType.BIGINT.getType())
                        .build();
        final Block block = allocator.createBlock(partSchema);
        block.constrain(ConstraintEvaluator.emptyEvaluator());

        final AtomicInteger written = new AtomicInteger();
        final BlockWriter writer = mock(BlockWriter.class);
        Mockito.doAnswer(inv -> {
            final BlockWriter.RowWriter rw = inv.getArgument(0);
            written.set(rw.writeRows(block, 0));
            return null;
        }).when(writer).writeRows(org.mockito.ArgumentMatchers.any());

        final Constraints noBounds = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);
        final GetTableLayoutRequest request =
                new GetTableLayoutRequest(IDENTITY, "q", "catalog",
                        new TableName("mydb", "cpu"), noBounds, partSchema, Collections.emptySet());
        final QueryStatusChecker checker = mock(QueryStatusChecker.class);

        pHandler.getPartitions(writer, request, checker);
        // No time predicate -> exactly one unbounded partition.
        assertEquals(1, written.get());
    }

    @Test
    public void testDoGetSplitsWithTimeBoundsCreatesSplitPerPartition() throws Exception
    {
        final Schema partSchema = SchemaBuilder.newBuilder()
                        .addField("time_lower", Types.MinorType.BIGINT.getType())
                        .addField("time_upper", Types.MinorType.BIGINT.getType())
                        .build();
        final Block partitions = allocator.createBlock(partSchema);
        partitions.constrain(ConstraintEvaluator.emptyEvaluator());
        partitions.setValue("time_lower", 0, 1000L);
        partitions.setValue("time_upper", 0, 2000L);
        partitions.setValue("time_lower", 1, 2000L);
        partitions.setValue("time_upper", 1, 3000L);
        partitions.setRowCount(2);

        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);
        final GetSplitsRequest request = new GetSplitsRequest(IDENTITY, "q", "catalog",
                        new TableName("mydb", "cpu"), partitions, List.of("time_lower", "time_upper"), constraints, null);

        final GetSplitsResponse response = handler.doGetSplits(allocator, request);
        assertEquals(2, response.getSplits().size());
        assertTrue(response.getSplits().stream()
                .allMatch(s -> s.getProperty("time_lower") != null && s.getProperty("time_upper") != null));
    }

    @Test
    public void testDoGetSplitsWithoutTimeColumnsCreatesSingleSplit() throws Exception
    {
        final Schema partSchema = SchemaBuilder.newBuilder()
                        .addField("partitionId", Types.MinorType.INT.getType())
                        .build();
        final Block partitions = allocator.createBlock(partSchema);
        partitions.constrain(ConstraintEvaluator.emptyEvaluator());
        partitions.setValue("partitionId", 0, 1);
        partitions.setRowCount(1);

        final Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(),
                Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, null, null);
        final GetSplitsRequest request = new GetSplitsRequest(IDENTITY, "q", "catalog",
                        new TableName("mydb", "cpu"), partitions, Collections.emptyList(), constraints, null);

        final GetSplitsResponse response = handler.doGetSplits(allocator, request);
        assertEquals(1, response.getSplits().size());
    }

    private InfluxDBMetadataHandler handlerWithParallelism(final String enabled, final String count)
    {
        final Map<String, String> config = new HashMap<>();
        config.put("spill_bucket", "test-bucket");
        config.put("spill_prefix", "test-prefix");
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "test-token");
        config.put("enable_query_parallelism", enabled);
        config.put("query_parallelism_count", count);
        return new InfluxDBMetadataHandler(
                mockFactory,
                new LocalKeyFactory(),
                mock(SecretsManagerClient.class),
                mock(AthenaClient.class),
                "test-bucket",
                "test-prefix",
                config);
    }

    @Test
    public void testDoGetDataSourceCapabilitiesIncludesQueryPassthrough()
    {
        final com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse response =
                handler.doGetDataSourceCapabilities(allocator,
                        new com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest(
                                IDENTITY, "queryId", "catalog"));
        assertTrue(response.getCapabilities().containsKey("SYSTEM.QUERY"));
    }

    @Test
    public void testDoGetQueryPassthroughSchemaDerivesSchemaFromResult() throws Exception
    {
        try (BufferAllocator arrow = new RootAllocator()) {
            final Schema resultSchema = new Schema(List.of(
                    new Field("host", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("usage_idle", FieldType.nullable(
                            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                    new Field("time", FieldType.nullable(
                            new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")), null)));
            final VectorSchemaRoot root = VectorSchemaRoot.create(resultSchema, arrow);
            when(mockClient.queryBatches(anyString())).thenReturn(Stream.of(root).onClose(root::close));

            final Map<String, String> args = new HashMap<>();
            args.put("schemaFunctionName", "SYSTEM.QUERY");
            args.put(InfluxDBQueryPassthrough.DATABASE, "mydb");
            args.put(InfluxDBQueryPassthrough.QUERY, "SELECT host, usage_idle, time FROM cpu");

            final GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator,
                    new GetTableRequest(IDENTITY, "queryId", "catalog", new TableName("system", "query"), args));

            final Schema derived = response.getSchema();
            assertEquals(Types.MinorType.VARCHAR,
                    Types.getMinorTypeForArrowType(derived.findField("host").getType()));
            assertEquals(Types.MinorType.FLOAT8,
                    Types.getMinorTypeForArrowType(derived.findField("usage_idle").getType()));
            // Nanosecond source timestamp is normalized to millisecond UTC.
            assertEquals(Types.MinorType.TIMESTAMPMILLITZ,
                    Types.getMinorTypeForArrowType(derived.findField("time").getType()));
        }
    }
}
