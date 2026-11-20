/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

/**
 * Abstracts JDBC record handler and provides common reusable split records handling.
 */
public abstract class JdbcRecordHandler
        extends RecordHandler
        implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordHandler.class);
    private final JdbcConnectionFactory jdbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private static final String CLICKHOUSE_DB = "clickhouse";
    // Query-passthrough argument: when set to a true-ish value ("true", case-insensitive), the direct query's results
    // are written to S3 and its ResultSet-derived schema is exported to a sidecar object (see doReadRecords /
    // writePassThroughQueryUsingResultSchema). Any other value, or absence, selects the normal read path.
    public static final String EXPORT_SCHEMA = "ExportSchema";
    // Suffix for the sidecar object that carries the ResultSet-derived Arrow schema for pass through query spills.
    private static final String SCHEMA_SUFFIX = ".schema";
    // Label mixed into the split's nonce to derive the sidecar's own AES-GCM nonce (see schemaSidecarEncryptionKey).
    public static final String SCHEMA_SIDECAR_KEY_LABEL = "schema-sidecar";
    // Arrow minor types BlockUtils.setValue can populate; anything else is exported as VARCHAR by deriveExportSchema.
    private static final Set<Types.MinorType> EXPORTABLE_MINOR_TYPES = EnumSet.of(
            Types.MinorType.BIT, Types.MinorType.TINYINT, Types.MinorType.SMALLINT, Types.MinorType.INT,
            Types.MinorType.BIGINT, Types.MinorType.UINT1, Types.MinorType.UINT2, Types.MinorType.UINT4,
            Types.MinorType.UINT8, Types.MinorType.FLOAT4, Types.MinorType.FLOAT8, Types.MinorType.DECIMAL,
            Types.MinorType.VARCHAR, Types.MinorType.VARBINARY, Types.MinorType.DATEDAY, Types.MinorType.DATEMILLI,
            Types.MinorType.TIMESTAMPMILLITZ, Types.MinorType.TIMESTAMPMICROTZ);

    protected final JdbcQueryPassthrough queryPassthrough = new JdbcQueryPassthrough();

    /**
     * Used only by Multiplexing handler. All invocations will be delegated to respective database handler.
     */
    protected JdbcRecordHandler(String sourceType, java.util.Map<String, String> configOptions)
    {
        super(sourceType, configOptions);
        this.jdbcConnectionFactory = null;
        this.databaseConnectionConfig = null;
    }

    protected JdbcRecordHandler(
        S3Client amazonS3,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        DatabaseConnectionConfig databaseConnectionConfig,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig.getEngine(), configOptions);
        this.jdbcConnectionFactory = Validate.notNull(jdbcConnectionFactory, "jdbcConnectionFactory must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
    }

    protected JdbcConnectionFactory getJdbcConnectionFactory()
    {
        return jdbcConnectionFactory;
    }

    protected DatabaseConnectionConfig getDatabaseConnectionConfig()
    {
        return databaseConnectionConfig;
    }

    @Override
    public String getDatabaseConnectionSecret()
    {
        DatabaseConnectionConfig databaseConnectionConfig = getDatabaseConnectionConfig();
        if (Objects.nonNull(databaseConnectionConfig)) {
            return databaseConnectionConfig.getSecret();
        }
        return null;
    }

    /**
     * Intercepts the query-passthrough export path (the {@code ExportSchema} marker). The results are
     * written to S3 in the SDK spill format (schema-less, AES-GCM encrypted, {@code <spillKey>.N} naming) with the
     * same credential path the base spiller uses, so the only bespoke object is the {@code <spillKey>.schema} sidecar
     * carrying the schema derived from the live result at read time. The response advertises the written data objects
     * as spill locations and carries that same derived schema. For all other reads it delegates to the base.
     */
    @Override
    public RecordResponse doReadRecords(BlockAllocator allocator, ReadRecordsRequest request) throws Exception
    {
        if (!isExportSchemaRequest(request.getConstraints())) {
            return super.doReadRecords(allocator, request);
        }
        // Same checks buildQueryPassthroughSql applies on the normal passthrough path: the function signature must
        // match this connector's and QUERY must be present and non-empty.
        queryPassthrough.verify(request.getConstraints().getQueryPassthroughArguments());

        List<SpillLocation> spillLocations = new ArrayList<>();
        // The row source (live JDBC vs a pre-exported artifact) is an overridable detail; the response contract is
        // the same: a schema derived from the actual result at read time, the written spill locations, and a
        // <spillKey>.schema sidecar. We never rely on request.getSchema() here.
        Schema derivedSchema = exportResultSetToSpill(request, allocator, spillLocations);

        return new RemoteReadRecordsResponse(request.getCatalogName(), derivedSchema, spillLocations,
                request.getSplit().getEncryptionKey());
    }

    /**
     * @return true when the request is a query-passthrough read whose {@link #EXPORT_SCHEMA} argument is "true"
     * (case-insensitive). Presence alone is not enough: {@code ExportSchema=false} selects the normal read path.
     */
    protected static boolean isExportSchemaRequest(Constraints constraints)
    {
        Map<String, String> queryPassthroughArguments = constraints.getQueryPassthroughArguments();
        return queryPassthroughArguments != null && Boolean.parseBoolean(queryPassthroughArguments.get(EXPORT_SCHEMA));
    }

    /**
     * Produces the export results as SDK spill objects plus a {@code <spillKey>.schema} sidecar, and returns the
     * schema derived from the actual result at read time (faithful types; no connector-specific normalization). The
     * default implementation runs the query over JDBC and derives the schema from {@link java.sql.ResultSetMetaData}.
     * Connectors that can materialize results more efficiently (e.g. Snowflake's S3 {@code COPY INTO} export) may
     * override this to read the pre-exported artifact instead, as long as they still derive the schema from the
     * actual result, spill the rows, write the sidecar, and append the written data-object locations to
     * {@code spillLocations}.
     */
    protected Schema exportResultSetToSpill(ReadRecordsRequest request, BlockAllocator allocator, List<SpillLocation> spillLocations)
            throws Exception
    {
        String directQuery = request.getConstraints().getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);
        return writePassThroughQueryUsingResultSchema(request, directQuery, allocator, spillLocations);
    }

    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.info("{}: Catalog: {}, table {}, splits {}", readRecordsRequest.getQueryId(), readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                readRecordsRequest.getSplit().getProperties());
        try (Connection connection = this.jdbcConnectionFactory.getConnection(getCredentialProvider(getRequestOverrideConfig(readRecordsRequest)))) {
            String databaseProductName = connection.getMetaData().getDatabaseProductName();

            // clickhouse does not support disabling auto-commit
            if (!CLICKHOUSE_DB.equalsIgnoreCase(databaseProductName)) {
                connection.setAutoCommit(false); // For consistency. This is needed to be false to enable streaming for some database types.
            }

            enableCaseSensitivelyLookUpSession(connection); // For certain connectors, we require to apply session config first to enable case

            try (PreparedStatement preparedStatement = buildSplitSql(connection, readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSchema(), readRecordsRequest.getConstraints(), readRecordsRequest.getSplit());
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();
                Map<String, String> colNameRemapping = getColumnNameRemapping(readRecordsRequest);

                GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints());
                for (Field next : readRecordsRequest.getSchema().getFields()) {
                    if (next.getType() instanceof ArrowType.List) {
                        rowWriterBuilder.withFieldWriterFactory(next.getName(), makeFactory(next));
                    }
                    else {
                        rowWriterBuilder.withExtractor(next.getName(), makeExtractor(next, resultSet, partitionValues, colNameRemapping));
                    }
                }

                GeneratedRowWriter rowWriter = rowWriterBuilder.build();
                int rowsReturnedFromDatabase = 0;
                while (resultSet.next()) {
                    if (!queryStatusChecker.isQueryRunning()) {
                        return;
                    }
                    blockSpiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, resultSet) ? 1 : 0);
                    rowsReturnedFromDatabase++;
                }
                LOGGER.info("{} rows returned by database.", rowsReturnedFromDatabase);

                // clickhouse does not support commit/rollback, so skip commit() for clickhouse
                if (!CLICKHOUSE_DB.equalsIgnoreCase(databaseProductName)) {
                    connection.commit();
                }
                disableCaseSensitivelyLookUpSession(connection); // For certain connectors, we require to apply session config first to enable case
            }
        }
    }

    /**
     * Executes the export query and writes its results to S3 via the SDK {@link S3BlockSpiller} (which handles
     * credentials, encryption, {@code <spillKey>.N} naming and location tracking); the only bespoke object is the
     * {@code <spillKey>.schema} sidecar carrying the schema derived from the live {@link java.sql.ResultSetMetaData}. The
     * derived schema uses faithful Arrow types (via {@link JdbcToArrowUtils}) with no connector-specific
     * normalization, and {@link BlockUtils#setValue} populates the vectors (it covers the full type range, including
     * timestamp-with-timezone). Written data-object locations are appended to {@code spillLocations}; the sidecar is
     * intentionally NOT added (it is not a data block).
     *
     * @return the Arrow schema derived from the live result (the schema the data was written with).
     */
    protected Schema writePassThroughQueryUsingResultSchema(ReadRecordsRequest request, String passThroughQuery, BlockAllocator allocator,
                                                            List<SpillLocation> spillLocations)
            throws Exception
    {
        S3SpillLocation spillLocation = (S3SpillLocation) request.getSplit().getSpillLocation();
        SpillConfig spillConfig = buildExportSpillConfig(request);
        S3Client s3Client = resolveScopedS3Client(request);

        LOGGER.info("writePassThroughQueryUsingResultSchema: executing export query; writing records via spiller to s3://{}/{}.N and schema to {}{}",
                spillLocation.getBucket(), spillLocation.getKey(), spillLocation.getKey(), SCHEMA_SUFFIX);

        // This separate-schema export targets plain Arrow readers, so disable Presto/Athena timezone packing for
        // this thread: timestamp-with-tz values are written as plain epoch millis (cleared in finally below).
        DateTimeFormatterUtil.overrideTimezonePacking(false);
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider(getRequestOverrideConfig(request)));
                QueryStatusChecker queryStatusChecker = createQueryStatusChecker(request)) {
            String databaseProductName = connection.getMetaData().getDatabaseProductName();
            // Same session handling as readWithConstraint: ClickHouse rejects setAutoCommit(false) and commit().
            boolean supportsTransactions = !CLICKHOUSE_DB.equalsIgnoreCase(databaseProductName);
            if (supportsTransactions) {
                connection.setAutoCommit(false); // enable server-side streaming for large result sets
            }
            enableCaseSensitivelyLookUpSession(connection);

            try (PreparedStatement preparedStatement = connection.prepareStatement(passThroughQuery);
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                // Derive the Arrow schema from the ResultSet itself (faithful column names AND types).
                // Overridable so connectors can adjust the type mapping (e.g. preserve NUMBER(p,0) precision).
                Schema derivedSchema = deriveExportSchema(resultSet);

                // Write the schema sidecar BEFORE any record is written, so a reader can discover the schema
                // as soon as the first data object appears (and even if the row-writing loop later fails).
                writeSchemaSidecar(s3Client, spillLocation, request.getSplit().getEncryptionKey(), derivedSchema, allocator);

                try (ConstraintEvaluator evaluator = new ConstraintEvaluator(allocator, derivedSchema, request.getConstraints());
                        S3BlockSpiller spiller = new S3BlockSpiller(s3Client, spillConfig, allocator, derivedSchema, evaluator, configOptions)) {
                    // Populate the Arrow vectors directly with BlockUtils.setValue, which already handles the full
                    // type range (including timestamp-with-timezone) that arbitrary export results can contain.
                    long rowsReturnedFromDatabase = 0;
                    // Resolve the Arrow output vectors by position (schema order) and read each cell by 1-based
                    // column index -- both avoid a per-cell lookup by column name. The derived schema's field order
                    // matches the ResultSet column order, so vector c corresponds to column c+1.
                    final int fieldCount = derivedSchema.getFields().size();
                    // Columns exported as VARCHAR (either genuinely textual or mapped there by deriveExportSchema
                    // because the JDBC type has no Arrow/BlockUtils equivalent) are read with getString so the
                    // driver's textual form is used rather than toString() of an opaque driver object.
                    final boolean[] readAsString = new boolean[fieldCount];
                    for (int c = 0; c < fieldCount; c++) {
                        readAsString[c] = derivedSchema.getFields().get(c).getType() instanceof ArrowType.Utf8;
                    }
                    while (resultSet.next()) {
                        if (!queryStatusChecker.isQueryRunning()) {
                            LOGGER.info("writePassThroughQueryUsingResultSchema: query {} is no longer running; stopping export after {} row(s)",
                                    request.getQueryId(), rowsReturnedFromDatabase);
                            break;
                        }
                        spiller.writeRows((Block block, int rowNum) -> {
                            List<FieldVector> vectors = block.getFieldVectors();
                            for (int c = 0; c < fieldCount; c++) {
                                Object value = readAsString[c] ? resultSet.getString(c + 1) : resultSet.getObject(c + 1);
                                BlockUtils.setValue(vectors.get(c), rowNum, value);
                            }
                            return 1;
                        });
                        rowsReturnedFromDatabase++;
                    }
                    if (supportsTransactions) {
                        connection.commit();
                    }
                    disableCaseSensitivelyLookUpSession(connection);

                    if (spiller.spilled()) {
                        spillLocations.addAll(spiller.getSpillLocations());
                    }

                    LOGGER.info("writePassThroughQueryUsingResultSchema: wrote {} row(s) across {} spill object(s) + 1 schema sidecar under s3://{}/{}",
                            rowsReturnedFromDatabase, spillLocations.size(), spillLocation.getBucket(), spillLocation.getKey());
                    return derivedSchema;
                }
            }
        }
        finally {
            DateTimeFormatterUtil.clearTimezonePackingOverride();
        }
    }

    /**
     * Derives the Arrow schema for the pass-through export from the live {@link ResultSet}. The default uses the
     * faithful JDBC-to-Arrow mapping ({@link JdbcToArrowUtils}) column by column, and falls back to VARCHAR for any
     * column that cannot be exported faithfully:
     * <ul>
     * <li>JDBC types arrow-jdbc does not map ({@code OTHER}, {@code JAVA_OBJECT}, {@code SQLXML}, {@code ROWID}, ...;
     * e.g. Postgres {@code uuid}/{@code jsonb}, Oracle intervals);</li>
     * <li>Arrow types {@link BlockUtils#setValue} cannot populate ({@code TIME}, {@code ARRAY}, {@code STRUCT},
     * non-TZ timestamps, and so on);</li>
     * <li>{@code DECIMAL}/{@code NUMERIC} columns whose reported precision is 0 (unconstrained {@code numeric}), which
     * cannot be represented as an Arrow decimal without rounding.</li>
     * </ul>
     * Such columns are written with {@link ResultSet#getString} so the driver's textual form is preserved. Connectors
     * may override to adjust the mapping further, for example to preserve the declared precision of integer-like
     * {@code NUMBER(p,0)} columns by mapping them to {@code Decimal(p,0)}.
     *
     * @param resultSet the live result set whose {@link java.sql.ResultSetMetaData} drives the schema
     * @return the Arrow schema the export data will be written with
     */
    protected Schema deriveExportSchema(ResultSet resultSet) throws SQLException
    {
        ResultSetMetaData metadata = resultSet.getMetaData();
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        List<Field> fields = new ArrayList<>(metadata.getColumnCount());
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            JdbcFieldInfo fieldInfo = new JdbcFieldInfo(metadata, column);
            String name = metadata.getColumnLabel(column);
            boolean nullable = metadata.isNullable(column) != ResultSetMetaData.columnNoNulls;
            ArrowType arrowType = exportArrowTypeOrNull(fieldInfo, utc);
            if (arrowType == null) {
                LOGGER.info("deriveExportSchema: column {} (jdbc type {} / {}) has no faithful Arrow mapping; exporting as VARCHAR",
                        name, fieldInfo.getJdbcType(), fieldInfo.getTypeName());
                arrowType = ArrowType.Utf8.INSTANCE;
            }
            fields.add(new Field(name, new FieldType(nullable, arrowType, null), null));
        }
        return new Schema(fields);
    }

    /**
     * @return the Arrow type arrow-jdbc assigns to the column when {@link BlockUtils#setValue} can populate a vector
     * of that type, otherwise null.
     */
    private static ArrowType exportArrowTypeOrNull(JdbcFieldInfo fieldInfo, Calendar calendar)
    {
        int jdbcType = fieldInfo.getJdbcType();
        if ((jdbcType == java.sql.Types.DECIMAL || jdbcType == java.sql.Types.NUMERIC) && fieldInfo.getPrecision() <= 0) {
            return null;
        }
        ArrowType arrowType;
        try {
            arrowType = JdbcToArrowUtils.getArrowTypeFromJdbcType(fieldInfo, calendar);
        }
        catch (UnsupportedOperationException | IllegalArgumentException ex) {
            return null;
        }
        if (arrowType == null) {
            return null;
        }
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);
        return EXPORTABLE_MINOR_TYPES.contains(minorType) ? arrowType : null;
    }

    /**
     * Builds the SpillConfig for the export: forces every batch out to S3 ({@code maxInlineBlockBytes=0}) so the
     * result is always spill objects (never inline). Uses asynchronous spilling ({@code numSpillThreads>0}) so each
     * block's encrypt + S3 PUT runs on a background thread and overlaps with fetching/encoding the next block,
     * instead of blocking the main row loop on every block boundary. In-flight memory is bounded by
     * (numSpillThreads + queue) blocks * maxBlockBytes, which stays well within the Lambda memory budget.
     */
    protected SpillConfig buildExportSpillConfig(ReadRecordsRequest request)
    {
        return SpillConfig.newBuilder()
                .withSpillLocation(request.getSplit().getSpillLocation())
                .withMaxBlockBytes(getSpillConfig(request).getMaxBlockBytes())
                .withMaxInlineBlockBytes(0)
                .withRequestId(request.getQueryId())
                .withEncryptionKey(request.getSplit().getEncryptionKey())
                .withNumSpillThreads(2)
                .build();
    }

    /**
     * Returns the S3 client to use for export spill writes, using the same credential path as the base spiller:
     * the request config options carry FAS session credentials for the (cross-account) spill bucket.
     */
    protected S3Client resolveScopedS3Client(ReadRecordsRequest request)
    {
        S3Client s3Client = getS3Client(getRequestOverrideConfig(request.getIdentity().getConfigOptions()), getAmazonS3());
        if (s3Client == null) {
            // Never fall back to the function's own identity for spill writes.
            throw new AthenaConnectorException("No S3 client available for export spill writes",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
        return s3Client;
    }

    /**
     * Writes the derived schema to the {@code <spillKey>.schema} sidecar, encrypted with the same SDK crypto as the
     * data objects (AesGcmBlockCrypto when a key is present, NoOp otherwise) but under the sidecar's own nonce (see
     * {@link #schemaSidecarEncryptionKey}), and with the same {@code spill_put_request_headers} the spiller applies
     * to data objects (SSE-KMS, bucket-owner checks, ...).
     */
    protected void writeSchemaSidecar(S3Client s3Client, S3SpillLocation spillLocation, EncryptionKey encryptionKey,
            Schema derivedSchema, BlockAllocator allocator)
    {
        BlockCrypto blockCrypto = (encryptionKey != null)
                ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
        byte[] schemaBytes = blockCrypto.encrypt(schemaSidecarEncryptionKey(encryptionKey), derivedSchema.serializeAsMessage());
        putSchema(s3Client, spillLocation.getBucket(), spillLocation.getKey() + SCHEMA_SUFFIX, schemaBytes);
    }

    /**
     * The key used to encrypt the {@code <spillKey>.schema} sidecar: the split's AES key with a nonce derived from the
     * split's nonce and {@link #SCHEMA_SIDECAR_KEY_LABEL}. AES-GCM must never encrypt two messages under the same
     * (key, nonce) pair, and the data blocks already use the split's nonce as-is; the sidecar's plaintext (an Arrow
     * schema message) is also highly predictable, so it gets its own nonce. Readers must apply the same derivation.
     *
     * @param splitKey the split's encryption key, or null when spill encryption is disabled
     * @return the sidecar key, or null when {@code splitKey} is null
     */
    public static EncryptionKey schemaSidecarEncryptionKey(EncryptionKey splitKey)
    {
        return splitKey == null ? null : splitKey.deriveWithLabel(SCHEMA_SIDECAR_KEY_LABEL);
    }

    private void putSchema(S3Client client, String bucket, String key, byte[] payload)
    {
        PutObjectRequest.Builder putRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentLength((long) payload.length);
        S3BlockSpiller.createSpillPutRequestOverrideConfig(configOptions).ifPresent(putRequest::overrideConfiguration);
        client.putObject(putRequest.build(), RequestBody.fromBytes(payload));
        LOGGER.info("putSchema: wrote {} bytes to s3://{}/{}", payload.length, bucket, key);
    }

    /**
     * Create a field extractor for complex List type.
     * @param field Field's metadata information.
     * @return Extractor for the List type.
     */
    protected FieldWriterFactory makeFactory(Field field)
    {
        return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                (FieldWriter) (Object context, int rowNum) ->
                {
                    Array arrayField = ((ResultSet) context).getArray(field.getName());
                    if (!((ResultSet) context).wasNull()) {
                        List<Object> fieldValue = new ArrayList<>(Arrays.asList((Object[]) arrayField.getArray()));
                        BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                    }
                    return true;
                };
    }

    protected boolean enableCaseSensitivelyLookUpSession(Connection connection)
    {
        return false;
    }

    protected boolean disableCaseSensitivelyLookUpSession(Connection connection)
    {
        return false;
    }

    /**
     * Creates an Extractor for the given field.
     */
    @VisibleForTesting
    protected Extractor makeExtractor(Field field, ResultSet resultSet, Map<String, String> partitionValues)
    {
        return makeExtractor(field, resultSet, partitionValues, Map.of());
    }
    
    private Extractor makeExtractor(Field field, ResultSet resultSet, Map<String, String> partitionValues, Map<String, String> colNameRemapping)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        final String fieldName = colNameRemapping.getOrDefault(field.getName(), field.getName());

        if (partitionValues.containsKey(fieldName)) {
            return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = partitionValues.get(fieldName);
            };
        }

        // Check if column exists in ResultSet - if not, return null extractor
        try {
            resultSet.findColumn(fieldName);
        }
        catch (SQLException e) {
            LOGGER.debug("Column {} not found in ResultSet, returning null extractor", fieldName);
            return makeNullExtractor(fieldType);
        }

        switch (fieldType) {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    boolean value = resultSet.getBoolean(fieldName);
                    dst.value = value ? 1 : 0;
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    dst.value = resultSet.getByte(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    dst.value = resultSet.getShort(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) ->
                {
                    dst.value = resultSet.getInt(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    dst.value = resultSet.getLong(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    dst.value = resultSet.getFloat(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    try {
                        dst.value = resultSet.getDouble(fieldName);
                    }
                    catch (java.sql.SQLException ex) {
                        // We need to use Double.parseDouble()
                        // replaceAll() use to strip commas "$25,000.00"
                        dst.value = Double.parseDouble(resultSet.getString(fieldName).replaceAll(",", "").replaceAll("\\$", ""));
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    dst.value = resultSet.getBigDecimal(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    //Issue fix for getting different date (offset by 1) for any dates prior to 1/1/1970.
                    if (resultSet.getDate(fieldName) != null) {
                        dst.value = (int) LocalDate.parse(resultSet.getDate(fieldName).toString()).toEpochDay();
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    if (resultSet.getTimestamp(fieldName) != null) {
                        dst.value = resultSet.getTimestamp(fieldName).getTime();
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
                {
                    if (null != resultSet.getString(fieldName)) {
                        dst.value = resultSet.getString(fieldName);
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    dst.value = resultSet.getBytes(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            default:
                throw new AthenaConnectorException("Unhandled type " + fieldType,
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
        }
    }

    /**
     * Builds split SQL string and returns prepared statement.
     *
     * @param jdbcConnection jdbc connection. See {@link Connection}
     * @param catalogName Athena provided catalog name.
     * @param tableName database table name.
     * @param schema table schema.
     * @param constraints constraints to push down to the database.
     * @param split table split.
     * @return prepared statement with sql. See {@link PreparedStatement}
     * @throws SQLException JDBC database exception.
     */
    public abstract PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException;

    public PreparedStatement buildQueryPassthroughSql(Connection jdbcConnection, Constraints constraints) throws SQLException
    {
        PreparedStatement preparedStatement;
        queryPassthrough.verify(constraints.getQueryPassthroughArguments());
        String clientPassQuery = constraints.getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);
        preparedStatement = jdbcConnection.prepareStatement(clientPassQuery);
        return preparedStatement;
    }
    
    protected Map<String, String> getColumnNameRemapping(ReadRecordsRequest request)
    {
        if (request.getConstraints() != null 
                && request.getConstraints().getQueryPlan() != null 
                && request.getConstraints().getQueryPlan().getSubstraitPlan() != null) {
            // Get renamed → original mapping from SubstraitSqlUtils
            Map<String, String> renamedToOriginal = SubstraitSqlUtils.getColumnRemapping(
                    request.getConstraints().getQueryPlan().getSubstraitPlan(), getSqlDialect());
            
            // Invert to original → renamed mapping (filtering out null values for computed expressions)
            Map<String, String> originalToRenamed = new java.util.HashMap<>();
            for (Map.Entry<String, String> entry : renamedToOriginal.entrySet()) {
                if (entry.getValue() != null) {
                    originalToRenamed.put(entry.getValue(), entry.getKey());
                }
            }
            return originalToRenamed;
        }
        return Map.of();
    }
    
    protected SqlDialect getSqlDialect() 
    {
        return AnsiSqlDialect.DEFAULT;
    }
    
    private Extractor makeNullExtractor(Types.MinorType fieldType)
    {
        switch (fieldType) {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) -> dst.isSet = 0;
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) -> dst.isSet = 0;
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) -> dst.isSet = 0;
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) -> dst.isSet = 0;
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) -> dst.isSet = 0;
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) -> dst.isSet = 0;
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) -> dst.isSet = 0;
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) -> dst.isSet = 0;
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) -> dst.isSet = 0;
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) -> dst.isSet = 0;
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) -> dst.isSet = 0;
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) -> dst.isSet = 0;
            default:
                throw new AthenaConnectorException("Unhandled type " + fieldType,
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
        }
    }

    /**
     * Closes the underlying {@link JdbcConnectionFactory}, releasing any pooled connections
     * and their associated background threads. Callers that create handler instances per-request
     * (e.g., multi-tenant wrappers) should call this method when the handler is no longer needed
     * to prevent thread and memory leaks from unreleased connection pools.
     */
    @Override
    public void close() throws Exception
    {
        if (jdbcConnectionFactory != null) {
            jdbcConnectionFactory.close();
        }
    }
}
