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
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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
import com.amazonaws.athena.connector.util.PaginationHelper;
import com.amazonaws.athena.connectors.influxdb.InfluxDBConnectionFactory.DatabaseInfo;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
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

import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.ENABLE_QUERY_PARALLELISM;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.PART_TIME_LOWER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.PART_TIME_UPPER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.QUERY_PARALLELISM_COUNT;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.SOURCE_TYPE;

public class InfluxDBMetadataHandler
        extends
            MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBMetadataHandler.class);

    // Split-count tuning for time-based query parallelism.
    private static final int DEFAULT_SPLIT_COUNT = 8;
    private static final int MIN_SPLIT_COUNT = 1;
    private static final int MAX_SPLIT_COUNT = 16;

    private final InfluxDBConnectionFactory connectionFactory;
    private final InfluxDBQueryPassthrough queryPassthrough = new InfluxDBQueryPassthrough();

    public InfluxDBMetadataHandler(final Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        this.connectionFactory = new InfluxDBConnectionFactory(configOptions, this);
    }

    @VisibleForTesting
    protected InfluxDBMetadataHandler(
            final InfluxDBConnectionFactory connectionFactory,
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

        // Advertise the system.query(DATABASE, QUERY) passthrough (unless disabled via config).
        queryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    /**
     * Derives the result schema for a query passthrough by running the native query and inspecting the Arrow schema of
     * its first result batch. The InfluxDB Arrow types are mapped to the same Athena types this connector produces for
     * normal reads, so the RecordHandler's value conversion is identical for passthrough and non-passthrough reads.
     */
    @Override
    public GetTableResponse doGetQueryPassthroughSchema(final BlockAllocator allocator, final GetTableRequest request)
            throws Exception
    {
        if (!request.isQueryPassthrough()) {
            throw new IllegalArgumentException("doGetQueryPassthroughSchema called without query passthrough arguments");
        }
        queryPassthrough.verify(request.getQueryPassthroughArguments());
        queryPassthrough.customConnectorVerifications(request.getQueryPassthroughArguments());
        final String database = request.getQueryPassthroughArguments().get(InfluxDBQueryPassthrough.DATABASE);
        final String query = request.getQueryPassthroughArguments().get(InfluxDBQueryPassthrough.QUERY);
        logger.info("doGetQueryPassthroughSchema: database={}", database);

        final Schema schema = connectionFactory.executeWithTokenRetry(database, client -> {
            final SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
            try (Stream<VectorSchemaRoot> batches = client.queryBatches(query)) {
                final java.util.Optional<VectorSchemaRoot> first = batches.findFirst();
                if (first.isEmpty()) {
                    throw new IllegalStateException(
                            "Query passthrough returned no result schema; cannot infer columns for: " + query);
                }
                for (final Field field : first.get().getSchema().getFields()) {
                    addPassthroughField(schemaBuilder, field);
                }
            }
            return schemaBuilder.build();
        });
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    /**
     * Maps a column from the InfluxDB Flight result's Arrow schema to the Athena field type this connector uses for
     * normal reads (timestamps normalized to millisecond UTC; tags/strings to VARCHAR; integers to BIGINT).
     */
    private static void addPassthroughField(final SchemaBuilder schemaBuilder, final Field field)
    {
        final String name = field.getName();
        switch (Types.getMinorTypeForArrowType(field.getType())) {
            case TIMESTAMPNANOTZ:
            case TIMESTAMPMICROTZ:
            case TIMESTAMPMILLITZ:
            case TIMESTAMPSECTZ:
            case TIMESTAMPNANO:
            case TIMESTAMPMICRO:
            case TIMESTAMPMILLI:
            case TIMESTAMPSEC:
            case DATEMILLI:
            case DATEDAY:
                schemaBuilder.addField(name, new ArrowType.Timestamp(
                        org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC"));
                break;
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
                schemaBuilder.addField(name, Types.MinorType.BIGINT.getType());
                break;
            case FLOAT8:
            case FLOAT4:
                schemaBuilder.addField(name, Types.MinorType.FLOAT8.getType());
                break;
            case BIT:
                schemaBuilder.addField(name, Types.MinorType.BIT.getType());
                break;
            default:
                // Tags (dictionary-encoded Utf8), Utf8, and anything else, render as VARCHAR.
                schemaBuilder.addField(name, Types.MinorType.VARCHAR.getType());
                break;
        }
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
        final String resolvedDB = connectionFactory.resolveDatabase(request.getSchemaName());
        final String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox'";
        connectionFactory.executeWithTokenRetry(resolvedDB, client -> {
            // Clear in case the query is retried after a token refresh (auth errors precede any
            // rows, so this is normally a no-op, but it keeps a retry from duplicating entries).
            tables.clear();
            try (Stream<Object[]> stream = client.query(sql)) {
                stream.forEach(row -> {
                    final String originalName = String.valueOf(row[0]);
                    // Athena requires lowercase identifiers, but we store the original
                    // case in table properties so we can resolve it later.
                    tables.add(new TableName(request.getSchemaName(), originalName.toLowerCase(Locale.ROOT)));
                });
            }
            return null;
        });
        // Manual (in-memory) pagination: InfluxDB returns the full table list in one query, and Athena
        // may page a large catalog in via repeated calls using the returned continuation token.
        return PaginationHelper.manualPagination(tables, request.getNextToken(), request.getPageSize(),
                request.getCatalogName());
    }

    @Override
    public GetTableResponse doGetTable(final BlockAllocator allocator, final GetTableRequest request)
            throws Exception
    {
        logger.info("doGetTable: catalog={}, table={}", request.getCatalogName(), request.getTableName());
        final String resolvedDB = connectionFactory.resolveDatabase(request.getTableName().getSchemaName());
        final String resolvedTable = connectionFactory.resolveTableName(resolvedDB, request.getTableName());
        final SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        // Store the original case-sensitive table name so the RecordHandler can use it
        schemaBuilder.addMetadata("caseSensitiveTableName", resolvedTable);
        schemaBuilder.addMetadata("resolvedDatabaseName", resolvedDB);
        final Map<String, Object> parameters = Map.of("table_name", request.getTableName().getTableName().toLowerCase(Locale.ROOT));
        final String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE lower(table_name) = $table_name";
        final List<Object[]> columns = connectionFactory.executeWithTokenRetry(resolvedDB, client -> {
            try (Stream<Object[]> stream = client.query(sql, parameters)) {
                return stream.collect(java.util.stream.Collectors.toList());
            }
        });
        for (final Object[] row : columns) {
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
        }
        final Schema schema = schemaBuilder.build();
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    /**
     * This method can be used to add additional fields to the schema of our
     * partition response. Athena
     * expects each partitions in the response to have a column corresponding to
     * your partition columns.
     * You can choose to add additional columns to that response which Athena will
     * ignore but will pass
     * on to you when it call GetSplits(...) for each partition.
     *
     * @param partitionSchemaBuilder The SchemaBuilder you can use to add additional
     *                               columns and metadata to the
     *                               partitions response.
     * @param request                The GetTableLayoutResquest that triggered this
     *                               call.
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        if (parallelismEnabled()) {
            partitionSchemaBuilder.addField(PART_TIME_UPPER, Types.MinorType.BIGINT.getType());
            partitionSchemaBuilder.addField(PART_TIME_LOWER, Types.MinorType.BIGINT.getType());
        }
    }

    public boolean parallelismEnabled()
    {
        return Boolean.parseBoolean(configOptions.getOrDefault(ENABLE_QUERY_PARALLELISM, "false"));
    }

    /**
     * Returns the number of time-based splits to generate, read from
     * {@link InfluxDBConstants#QUERY_PARALLELISM_COUNT} and clamped to a safe
     * range. Kept modest because the backend is a single InfluxDB instance and
     * too many concurrent split queries can overload it. Missing, blank, or
     * non-numeric config falls back to the default; values outside the range
     * are clamped (so 0 or negative becomes a single split).
     */
    @VisibleForTesting
    int clampedSplitCount()
    {
        int requested = DEFAULT_SPLIT_COUNT;
        final String configured = configOptions.get(QUERY_PARALLELISM_COUNT);
        if (configured != null && !configured.isBlank()) {
            try {
                requested = Integer.parseInt(configured.trim());
            }
            catch (final NumberFormatException e) {
                logger.warn("Invalid {} value '{}'; falling back to default {}",
                        QUERY_PARALLELISM_COUNT, configured, DEFAULT_SPLIT_COUNT);
                requested = DEFAULT_SPLIT_COUNT;
            }
        }
        return Math.max(MIN_SPLIT_COUNT, Math.min(requested, MAX_SPLIT_COUNT));
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest request, final QueryStatusChecker queryStatusChecker) throws Exception
    {
        final Long[] bounds = extractTimeRange(request.getConstraints());
        if (bounds == null) {
            // Single-partition fallback: one bucket with no time bound. The
            // RowWriter is invoked once, so it must write every row it needs.
            blockWriter.writeRows((block, rowNum) -> {
                block.setValue(PART_TIME_LOWER, rowNum, null);
                block.setValue(PART_TIME_UPPER, rowNum, null);
                return 1;
            });
            return;
        }

        final long min = bounds[0];
        final long max = bounds[1];
        // Never create more buckets than there are milliseconds in the range.
        final int buckets = (int) Math.max(1L, Math.min(clampedSplitCount(), max - min));
        final long width = Math.max(1L, (max - min) / buckets);
        blockWriter.writeRows((block, rowNum) -> {
            int written = 0;
            for (int i = 0; i < buckets; i++) {
                final long low = min + (long) i * width;
                if (low >= max) {
                    break;
                }
                // Half-open [low, high). The final bucket extends one millisecond
                // past max so the max timestamp is captured (readWithConstraint
                // applies `< high`).
                final long high = (i == buckets - 1) ? max + 1 : Math.min(max, min + (long) (i + 1) * width);
                block.setValue(PART_TIME_LOWER, rowNum + written, low);
                block.setValue(PART_TIME_UPPER, rowNum + written, high);
                written++;
            }
            return written;
        });
    }

    @Override
    public GetSplitsResponse doGetSplits(final BlockAllocator allocator, final GetSplitsRequest request)
            throws Exception
    {
        final Block partitions = request.getPartitions();
        // We only want to split queries when the user has included a lower and upper time bound.
        final boolean hasBounds = hasField(partitions, PART_TIME_LOWER) && hasField(partitions, PART_TIME_UPPER);
        final FieldReader lowReader = hasBounds ? partitions.getFieldReader(PART_TIME_LOWER) : null;
        final FieldReader highReader = hasBounds ? partitions.getFieldReader(PART_TIME_UPPER) : null;

        final Set<Split> splits = new HashSet<>();
        for (int i = 0; i < partitions.getRowCount(); i++) {
            final Split.Builder builder = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey());
            if (hasBounds) {
                lowReader.setPosition(i);
                highReader.setPosition(i);
                // Null bounds mark the single-partition fallback (no time filter).
                if (lowReader.isSet() && highReader.isSet()) {
                    builder.add(PART_TIME_LOWER, String.valueOf(lowReader.readLong()));
                    builder.add(PART_TIME_UPPER, String.valueOf(highReader.readLong()));
                }
            }
            splits.add(builder.build());
        }

        if (splits.isEmpty()) {
            // Defensive: Athena requires at least one split to read.
            splits.add(Split.newBuilder(makeSpillLocation(request), makeEncryptionKey()).build());
        }

        return new GetSplitsResponse(request.getCatalogName(), splits);
    }

    private static boolean hasField(final Block block, final String fieldName)
    {
        for (final Field field : block.getSchema().getFields()) {
            if (field.getName().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    static Long[] extractTimeRange(Constraints constraints)
    {
        ValueSet valueSet = constraints.getSummary() == null ? null : constraints.getSummary().get("time");
        if (!(valueSet instanceof SortedRangeSet) || valueSet.isNone()) {
            return null;
        }

        if (!(valueSet.getType() instanceof ArrowType.Timestamp)) {
            return null;
        }
        final ArrowType.Timestamp tsType = (ArrowType.Timestamp) valueSet.getType();

        Range span = ((SortedRangeSet) valueSet).getSpan();
        if (span.getLow().isLowerUnbounded() || span.getHigh().isUpperUnbounded()) {
            return null;
        }

        long min = InfluxDBQueryBuilder.constraintEpochMillis(span.getLow().getValue(), tsType);
        long max = InfluxDBQueryBuilder.constraintEpochMillis(span.getHigh().getValue(), tsType);
        if (min >= max) {
            return null;
        }

        return new Long[] { min, max };
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
