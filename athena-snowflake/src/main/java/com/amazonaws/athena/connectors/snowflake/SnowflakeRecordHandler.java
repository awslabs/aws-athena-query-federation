/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.snowflake.connection.SnowflakeConnectionFactory;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorAppender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.Validate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.JDBC_PROPERTIES;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_QUERY_ID;

public class SnowflakeRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeRecordHandler.class);
    private final JdbcConnectionFactory jdbcConnectionFactory;
    private static final int EXPORT_READ_BATCH_SIZE_BYTE = 32768;
    private static final int FETCH_SIZE = 1000;
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     * <p>
     */
    public SnowflakeRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME, configOptions), configOptions);
    }

    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(
            databaseConnectionConfig,
            new SnowflakeConnectionFactory(
                databaseConnectionConfig,
                SnowflakeEnvironmentProperties.getSnowFlakeParameter(JDBC_PROPERTIES, configOptions),
                new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS, SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT),
                configOptions),
            configOptions);
    }

    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, GenericJdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                jdbcConnectionFactory, new SnowflakeQueryStringBuilder(SnowflakeConstants.DOUBLE_QUOTE_CHAR, new SnowflakeFederationExpressionParser(SnowflakeConstants.DOUBLE_QUOTE_CHAR)), configOptions);
    }

    @VisibleForTesting
    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcConnectionFactory = jdbcConnectionFactory;
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    /**
     * Used to handle data transfer between Snowflake and Athena, supporting both direct query and S3 export paths,
     * converts to arrow format, and manages spillover logic.
     *
     * @param spiller            A BlockSpiller that should be used to write the row data associated with this Split.
     *                           The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including:
     *                           1. The Split
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws Exception       Throws an Exception
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        if (SnowflakeConstants.isS3ExportEnabled(configOptions)) {
            // Use S3 export path for data transfer
            handleS3ExportRead(spiller, recordsRequest, queryStatusChecker);
        }
        else {
            // Use traditional direct query path
            handleDirectRead(spiller, recordsRequest, queryStatusChecker);
        }
    }

    private void handleS3ExportRead(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws IOException
    {
        LOGGER.info("handleS3ExportRead: schema[{}] tableName[{}]", recordsRequest.getSchema(), recordsRequest.getTableName());
        Split split = recordsRequest.getSplit();
        String id = split.getProperty(SNOWFLAKE_SPLIT_QUERY_ID);
        String exportBucket = split.getProperty(SNOWFLAKE_SPLIT_EXPORT_BUCKET);
        String s3ObjectKey = split.getProperty(SNOWFLAKE_SPLIT_OBJECT_KEY);

        if (s3ObjectKey.isEmpty()) {
            LOGGER.debug("S3 object key is empty from request, skip read from S3");
            return;
        }

        String s3path = constructS3Uri(exportBucket, s3ObjectKey);
        try (ArrowReader reader = constructArrowReader(s3path, recordsRequest.getSchema())) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                // use writeRows method as it handle the spilling to s3.
                spiller.writeRows((Block block, int startRowNum) -> {
                    // Copy vectors directly to the block
                    // Write all rows in the batch at once
                    for (Field field : root.getSchema().getFields()) {
                        if (recordsRequest.getSchema().findField(field.getName()) != null) {
                            FieldVector originalVector = block.getFieldVector(field.getName());
                            FieldVector toAppend = root.getVector(field.getName());

                            // to_append block, both vector must be same time
                            // However, Athena treat TSWithTZ as DateTimeMilli(UTC), hence we will need a conversion from TimeStampMilliTZ to DateTimeMilli
                            if (toAppend instanceof TimeStampMilliTZVector) {
                               toAppend = convertTimestampTZMilliToDateMilliFast((TimeStampMilliTZVector) toAppend, toAppend.getAllocator());
                            }

                            VectorAppender appender = new VectorAppender(originalVector);
                            toAppend.accept(appender, null);
                        }
                    }
                    return root.getRowCount();
                });
            }
        }
        catch (Exception e) {
            throw new AthenaConnectorException("Error in object content for object : " + s3path + " " + e.getMessage(), e,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
    }

    private void handleDirectRead(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
      super.readWithConstraint(spiller, recordsRequest, queryStatusChecker);
    }

    /**
     * Query-passthrough export ({@link JdbcRecordHandler#EXPORT_SCHEMA} marker). When S3 export is enabled, the query was already
     * unloaded to Parquet by {@code COPY INTO} during GetSplits, so instead of re-running it over JDBC we read this
     * split's Parquet file, derive the schema from it (Parquet is self-describing), re-spill the rows as Arrow, and
     * write the {@code <spillKey>.schema} sidecar. The schema and data use the Parquet's faithful types with no
     * coercion (unlike the normal read path, which normalizes to request.getSchema()). When S3 export is not enabled,
     * we fall back to the base JDBC path (which derives the schema from the ResultSet).
     */
    @Override
    protected Schema exportResultSetToSpill(ReadRecordsRequest request, BlockAllocator allocator, List<SpillLocation> spillLocations)
            throws Exception
    {
        if (!SnowflakeConstants.isS3ExportEnabled(configOptions)) {
            return super.exportResultSetToSpill(request, allocator, spillLocations);
        }

        Split split = request.getSplit();
        String exportBucket = split.getProperty(SNOWFLAKE_SPLIT_EXPORT_BUCKET);
        String s3ObjectKey = split.getProperty(SNOWFLAKE_SPLIT_OBJECT_KEY);
        // Empty export (no rows): fall back to JDBC so we can still derive the schema and emit an empty result + sidecar.
        if (s3ObjectKey == null || s3ObjectKey.isEmpty()) {
            return super.exportResultSetToSpill(request, allocator, spillLocations);
        }

        S3SpillLocation spillLocation = (S3SpillLocation) split.getSpillLocation();
        S3Client s3Client = resolveScopedS3Client(request);
        SpillConfig spillConfig = buildExportSpillConfig(request);
        String s3path = constructS3Uri(exportBucket, s3ObjectKey);
        LOGGER.info("exportResultSetToSpill(S3 export): reading exported Parquet {} and re-spilling as Arrow", s3path);

        // Derive the schema from the self-describing Parquet, then read all its columns with faithful types (no
        // projection/coercion: we keep the exported schema and carry it in the sidecar).
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(new RootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, s3path);
        Schema derivedSchema = datasetFactory.inspect();

        try (ArrowReader reader = constructArrowReader(s3path, derivedSchema);
                ConstraintEvaluator evaluator = new ConstraintEvaluator(allocator, derivedSchema, request.getConstraints());
                S3BlockSpiller spiller = new S3BlockSpiller(s3Client, spillConfig, allocator, derivedSchema, evaluator, configOptions)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // Write the schema sidecar BEFORE any record is written, so a reader can discover the schema
            // as soon as the first data object appears (and even if the row-writing loop later fails).
            writeSchemaSidecar(s3Client, spillLocation, split.getEncryptionKey(), derivedSchema, allocator);
            while (reader.loadNextBatch()) {
                // Bulk columnar copy: append each Parquet vector into the spill block (types already match).
                spiller.writeRows((Block block, int startRowNum) -> {
                    for (Field field : root.getSchema().getFields()) {
                        FieldVector src = root.getVector(field.getName());
                        VectorAppender appender = new VectorAppender(block.getFieldVector(field.getName()));
                        src.accept(appender, null);
                    }
                    return root.getRowCount();
                });
            }
            if (spiller.spilled()) {
                spillLocations.addAll(spiller.getSpillLocations());
            }
        }
        return derivedSchema;
    }

    @VisibleForTesting
    protected ArrowReader constructArrowReader(String uri, Schema schema)
    {
        LOGGER.debug("URI {}", uri);
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                uri);
        Dataset dataset = datasetFactory.finish();

        // do a scan projection, only getting the column we want
        ScanOptions options = new ScanOptions(/*batchSize*/ EXPORT_READ_BATCH_SIZE_BYTE,
                Optional.of(schema.getFields().stream()
                        .map(Field::getName)
                        .filter(name -> !name.equalsIgnoreCase(BLOCK_PARTITION_COLUMN_NAME))
                        .toArray(String[]::new))); // Project the column we needed only.

        Scanner scanner = dataset.newScan(options);
        return scanner.scanBatches();
    }

    private static String constructS3Uri(String bucket, String key)
    {
        return "s3://" + bucket + "/" + key;
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableNameInput, Schema schema, Constraints constraints, Split split) throws SQLException
    {
        PreparedStatement preparedStatement;
        try {
            if (constraints.isQueryPassThrough()) {
                preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
            }
            else {
                preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableNameInput.getSchemaName(), tableNameInput.getTableName(), schema, constraints, split);
            }

            // Disable fetching all rows.
            preparedStatement.setFetchSize(FETCH_SIZE);
        }
        catch (SQLException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
        return preparedStatement;
    }

    @Override
    public CredentialsProvider createCredentialsProvider(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        if (StringUtils.isNotBlank(secretName)) {
            return new SnowflakeCredentialsProvider(secretName, requestOverrideConfiguration);
        }

        return null;
    }

    // TSmilli and DateTimeMilli vector both have same width of 8bytes and same data type(long)
    // direct copy the value.
     static DateMilliVector convertTimestampTZMilliToDateMilliFast(
            TimeStampMilliTZVector tsVector,
            BufferAllocator allocator)
     {
        // record's timezone must be in UTC
        if (!tsVector.getTimeZone().equalsIgnoreCase("UTC")) {
            throw new IllegalArgumentException("Athena S3 Export only support Timezone with UTC");
        }

        int rowCount = tsVector.getValueCount();
        DateMilliVector resultVector = new DateMilliVector(tsVector.getName(), allocator);
        resultVector.allocateNew(rowCount);

        // Copy data buffer directly to save time
        ArrowBuf srcData = tsVector.getDataBuffer();
        ArrowBuf dstData = resultVector.getDataBuffer();
        long bytes = (long) rowCount * Long.BYTES;

        // copy the data value into destination
        dstData.setBytes(0, srcData, 0, bytes);

        // copy the bitmap as well, otherwise it will show as empty(no value present)
        ArrowBuf srcValidity = tsVector.getValidityBuffer();
        ArrowBuf dstValidity = resultVector.getValidityBuffer();

        dstValidity.setBytes(
                0,
                srcValidity,
                0,
                BitVectorHelper.getValidityBufferSize(rowCount)
        );

        // finalized the actual value, without this vector will see no data.
        resultVector.setValueCount(rowCount);

        return resultVector;
    }

    /**
     * Preserves the declared precision of integer-like Snowflake {@code NUMBER(p,0)} columns on the JDBC export
     * path. Snowflake reports a scale-0 {@code NUMBER} with JDBC type {@code BIGINT}/{@code INTEGER}/..., so the
     * default mapping yields an Arrow integer vector that cannot carry the precision {@code p}. We re-map those
     * columns to {@code Decimal(p, 0)} so the exported schema (and sidecar) keeps the source precision.
     *
     * <p>Columns the driver already maps to a decimal (scale &gt; 0) are left untouched, and precisions outside the
     * Arrow 128-bit decimal range (1..38) fall back to the default integer mapping. Timestamp types are left as-is
     * ({@code timestamp[ms, tz=UTC]}); they are written unpacked because the base export path disables timezone
     * packing for the current thread (see
     * {@link com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil#overrideTimezonePacking(boolean)}).
     */
    @Override
    protected Schema deriveExportSchema(ResultSet resultSet) throws SQLException
    {
        Schema baseSchema = super.deriveExportSchema(resultSet);
        ResultSetMetaData metadata = resultSet.getMetaData();
        List<Field> fields = new ArrayList<>(baseSchema.getFields().size());

        for (int i = 0; i < baseSchema.getFields().size(); i++) {
            Field baseField = baseSchema.getFields().get(i);
            int column = i + 1; // ResultSetMetaData columns are 1-indexed
            Types.MinorType minorType = Types.getMinorTypeForArrowType(baseField.getType());
            boolean isArrowInteger = minorType == Types.MinorType.TINYINT || minorType == Types.MinorType.SMALLINT
                    || minorType == Types.MinorType.INT || minorType == Types.MinorType.BIGINT;

            if (isArrowInteger && metadata.getScale(column) == 0) {
                int precision = metadata.getPrecision(column);
                if (precision >= 1 && precision <= 38) {
                    ArrowType.Decimal decimalType = new ArrowType.Decimal(precision, 0, 128);
                    fields.add(new Field(baseField.getName(),
                            new FieldType(baseField.isNullable(), decimalType, null), baseField.getChildren()));
                    continue;
                }
                LOGGER.debug("deriveExportSchema: column {} precision {} outside Arrow decimal range; keeping {}",
                        baseField.getName(), precision, baseField.getType());
            }
            fields.add(baseField);
        }
        return new Schema(fields, baseSchema.getCustomMetadata());
    }
}
