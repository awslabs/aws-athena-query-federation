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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.influxdb.InfluxDbConnectionFactory.DatabaseInfo;
import com.google.common.collect.ImmutableMap;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.SOURCE_TYPE;

public class InfluxDbMetadataHandler
        extends
            MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbMetadataHandler.class);

    private final InfluxDbConnectionFactory connectionFactory;

    public InfluxDbMetadataHandler(final Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        this.connectionFactory = new InfluxDbConnectionFactory(configOptions, this);
    }

    @VisibleForTesting
    protected InfluxDbMetadataHandler(
            final InfluxDbConnectionFactory connectionFactory,
            final EncryptionKeyFactory keyFactory,
            final SecretsManagerClient secretsManager,
            final AthenaClient athena,
            final String spillBucket,
            final String spillPrefix,
            final Map<String, String> configOptions)
    {
        super(keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.connectionFactory = connectionFactory;
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(final BlockAllocator allocator,
            final GetDataSourceCapabilitiesRequest request)
    {
        final ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();

        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON));
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .map(sf -> sf.getFunctionName().getFunctionName())
                                .toArray(String[]::new))));

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    public ListSchemasResponse doListSchemaNames(final BlockAllocator allocator, final ListSchemasRequest request)
            throws Exception
    {
        logger.info("doListSchemaNames: catalog={}", request.getCatalogName());
        final Set<String> schemas = new HashSet<>();
        final String defaultDb = this.configOptions.getOrDefault("influxdb_database", "");
        // If the influxdb_database configuration option has been specified, scope to a
        // single database.
        if (!defaultDb.isEmpty()) {
            // Return lowercased for Athena, but the ConnectionFactory resolves
            // back to the original case via the configured influxdb_database value.
            schemas.add(defaultDb.toLowerCase(Locale.ROOT));
        }
        // Get all databases.
        else {
            for (final DatabaseInfo databaseInfo : connectionFactory.listDatabases()) {
                if (databaseInfo != null && databaseInfo.name != null) {
                    schemas.add(databaseInfo.name.toLowerCase(Locale.ROOT));
                }
            }
        }
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    @Override
    public ListTablesResponse doListTables(final BlockAllocator allocator, final ListTablesRequest request)
            throws Exception
    {
        logger.info("doListTables: catalog={}, schema={}", request.getCatalogName(), request.getSchemaName());
        final List<TableName> tables = new ArrayList<>();
        try (InfluxDBClient client = connectionFactory
                .getClient(connectionFactory.resolveDatabase(request.getSchemaName()))) {
            final String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox'";
            try (Stream<Object[]> stream = client.query(sql)) {
                stream.forEach(row -> {
                    final String originalName = String.valueOf(row[0]);
                    // Athena requires lowercase identifiers, but we store the original
                    // case in table properties so we can resolve it later.
                    tables.add(new TableName(request.getSchemaName(), originalName.toLowerCase(Locale.ROOT)));
                });
            }
        }
        return new ListTablesResponse(request.getCatalogName(), tables, null);
    }

    @Override
    public GetTableResponse doGetTable(final BlockAllocator allocator, final GetTableRequest request)
            throws Exception
    {
        logger.info("doGetTable: catalog={}, table={}", request.getCatalogName(), request.getTableName());
        final String resolvedDb = connectionFactory.resolveDatabase(request.getTableName().getSchemaName());
        final String resolvedTable = connectionFactory.resolveTableName(resolvedDb, request.getTableName());
        final SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        // Store the original case-sensitive table name so the RecordHandler can use it
        schemaBuilder.addMetadata("originalTableName", resolvedTable);
        try (InfluxDBClient client = connectionFactory
                .getClient(resolvedDb)) {
            final Map<String, Object> parameters = Map.of("table_name", request.getTableName().getTableName().toLowerCase(Locale.ROOT));
            final String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE lower(table_name) = $table_name";
            try (Stream<Object[]> stream = client.query(sql, parameters)) {
                stream.forEach(row -> {
                    final String colName = String.valueOf(row[0]).toLowerCase(Locale.ROOT);
                    final String dataType = String.valueOf(row[1]).toUpperCase(Locale.ROOT);
                    final Types.MinorType minorType = toArrowType(dataType);
                    if (minorType == Types.MinorType.TIMESTAMPMILLITZ) {
                        schemaBuilder.addField(colName,
                                new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(
                                        org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC"));
                    }
                    else {
                        schemaBuilder.addField(colName, minorType.getType());
                    }
                });
            }
        }
        final Schema schema = schemaBuilder.build();
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest request,
            final QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        // No partitioning — single partition
        blockWriter.writeRows((block, rowNum) -> {
            block.setValue("partition", rowNum, "0");
            return 1;
        });
    }

    @Override
    public GetSplitsResponse doGetSplits(final BlockAllocator allocator, final GetSplitsRequest request)
            throws Exception
    {
        // Single split — InfluxDB handles parallelism internally
        final Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey()).build();
        return new GetSplitsResponse(request.getCatalogName(), split);
    }

    static Types.MinorType toArrowType(final String influxType)
    {
        // InfluxDB 3 (DataFusion) returns types like:
        // "Dictionary(Int32, Utf8)" for tags
        // "Timestamp(ns)" or "Timestamp(Nanosecond, None)" for time
        // "Float64" for float fields
        // "Int64" for integer fields
        // "Boolean" for boolean fields
        // "Utf8" for string fields
        final String upper = influxType.toUpperCase(Locale.ROOT);
        if (upper.startsWith("DICTIONARY")) {
            return Types.MinorType.VARCHAR;
        }
        if (upper.startsWith("TIMESTAMP")) {
            return Types.MinorType.TIMESTAMPMILLITZ;
        }
        switch (upper) {
            case "FLOAT64" :
            case "DOUBLE" :
            case "FLOAT8" :
                return Types.MinorType.FLOAT8;
            case "INT64" :
            case "BIGINT" :
            case "INTEGER" :
            case "INT" :
            case "UINT64" :
                return Types.MinorType.BIGINT;
            case "BOOLEAN" :
            case "BOOL" :
                return Types.MinorType.BIT;
            case "UTF8" :
            case "VARCHAR" :
            case "STRING" :
                return Types.MinorType.VARCHAR;
            default :
                return Types.MinorType.VARCHAR;
        }
    }
}
