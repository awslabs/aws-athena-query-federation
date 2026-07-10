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
import com.amazonaws.athena.connectors.influxdb.InfluxDbConnectionFactory.DatabaseInfo;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
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

import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.ENABLE_QUERY_PARALLELISM;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.PART_TIME_LOWER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.PART_TIME_UPPER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.QUERY_PARALLELISM_COUNT;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.SOURCE_TYPE;

public class InfluxDbMetadataHandler
        extends
            MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbMetadataHandler.class);

    // Split-count tuning for time-based query parallelism.
    private static final int DEFAULT_SPLIT_COUNT = 8;
    private static final int MIN_SPLIT_COUNT = 1;
    private static final int MAX_SPLIT_COUNT = 16;

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
        final String resolvedDb = connectionFactory.resolveDatabase(request.getSchemaName());
        final String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox'";
        connectionFactory.executeWithTokenRetry(resolvedDb, client -> {
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
        schemaBuilder.addMetadata("resolvedDatabaseName", resolvedDb);
        final Map<String, Object> parameters = Map.of("table_name", request.getTableName().getTableName().toLowerCase(Locale.ROOT));
        final String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE lower(table_name) = $table_name";
        final List<Object[]> columns = connectionFactory.executeWithTokenRetry(resolvedDb, client -> {
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
     * {@link InfluxDbConstants#QUERY_PARALLELISM_COUNT} and clamped to a safe
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

        long min = InfluxDbQueryBuilder.constraintEpochMillis(span.getLow().getValue(), tsType);
        long max = InfluxDbQueryBuilder.constraintEpochMillis(span.getHigh().getValue(), tsType);
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
