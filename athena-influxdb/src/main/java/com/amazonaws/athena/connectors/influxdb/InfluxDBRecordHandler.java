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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.influxdb.InfluxDBConnectionFactory.ColumnInfo;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.PART_TIME_LOWER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.PART_TIME_UPPER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.SOURCE_TYPE;

public class InfluxDBRecordHandler
        extends
            RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBRecordHandler.class);
    private static final ZoneId UTC = ZoneId.of("UTC");

    private final InfluxDBConnectionFactory connectionFactory;
    private final InfluxDBQueryPassthrough queryPassthrough = new InfluxDBQueryPassthrough();

    /**
     * Creates a handler with its own connection factory. Used when the record handler is deployed as its own
     * Lambda handler.
     */
    public InfluxDBRecordHandler(final Map<String, String> configOptions)
    {
        this(configOptions, new InfluxDBConnectionFactory(configOptions, null));
    }

    /**
     * Creates a handler that uses the given connection factory, which may be shared with an
     * {@link InfluxDBMetadataHandler} in the same Lambda container. This handler becomes the factory's secret
     * resolver.
     */
    public InfluxDBRecordHandler(final Map<String, String> configOptions,
            final InfluxDBConnectionFactory connectionFactory)
    {
        this(S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(), connectionFactory,
                configOptions);
    }

    @VisibleForTesting
    protected InfluxDBRecordHandler(
            final S3Client s3Client,
            final SecretsManagerClient secretsManager,
            final AthenaClient athena,
            final InfluxDBConnectionFactory connectionFactory,
            final Map<String, String> configOptions)
    {
        super(s3Client, secretsManager, athena, SOURCE_TYPE, configOptions);
        this.connectionFactory = connectionFactory;
        if (connectionFactory != null) {
            connectionFactory.setHandler(this);
        }
    }

    @Override
    protected void readWithConstraint(final BlockSpiller spiller, final ReadRecordsRequest recordsRequest,
            final QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        final Schema schema = recordsRequest.getSchema();
        final String resolvedDB;
        final String sql;
        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            queryPassthrough.assertEnabled(configOptions);
            final Map<String, String> qptArgs = recordsRequest.getConstraints().getQueryPassthroughArguments();
            queryPassthrough.verify(qptArgs);
            resolvedDB = qptArgs.get(InfluxDBQueryPassthrough.DATABASE);
            sql = qptArgs.get(InfluxDBQueryPassthrough.QUERY);
            logger.info("readWithConstraint: query passthrough against database={}", resolvedDB);
        }
        else {
            final TableName authorizedTable = recordsRequest.getTableName();
            final String schemaName = authorizedTable.getSchemaName();
            resolvedDB = connectionFactory.resolveDatabase(schemaName);
            final String resolvedTable = connectionFactory.resolveTableName(resolvedDB, authorizedTable);
            // Athena lowercases column names; map each one back to its case-sensitive InfluxDB name.
            final Map<String, String> columnNames = new HashMap<>();
            for (final ColumnInfo column : connectionFactory.listColumns(resolvedDB, resolvedTable)) {
                columnNames.put(column.name.toLowerCase(Locale.ROOT), column.name);
            }

            final String timeLower = recordsRequest.getSplit().getProperty(PART_TIME_LOWER);
            final String timeUpper = recordsRequest.getSplit().getProperty(PART_TIME_UPPER);
            sql = InfluxDBQueryBuilder.buildSql(schema, resolvedTable, recordsRequest.getConstraints(),
                    timeLower, timeUpper, columnNames);
            logger.info("readWithConstraint: schema={}, table={}", schemaName, resolvedTable);
        }
        // The SQL embeds constraint literal values (possible PII) so it is logged only at debug.
        logger.debug("readWithConstraint SQL: {}", sql);

        final List<Field> fields = schema.getFields();

        connectionFactory.executeWithTokenRetry(resolvedDB, client -> {
            // queryBatches returns Arrow VectorSchemaRoots, so each timestamp column's
            // precision is known from its Arrow type rather than guessed from magnitude.
            try (Stream<VectorSchemaRoot> batches = client.queryBatches(sql)) {
                final Iterator<VectorSchemaRoot> it = batches.iterator();
                while (queryStatusChecker.isQueryRunning() && it.hasNext()) {
                    final VectorSchemaRoot root = it.next();
                    // Resolve each schema field to its result vector by name. A schema field absent from the
                    // result has no vector and is written as null.
                    final FieldVector[] columns = new FieldVector[fields.size()];
                    for (int j = 0; j < fields.size(); j++) {
                        columns[j] = root.getVector(fields.get(j).getName());
                    }
                    final int rowCount = root.getRowCount();
                    for (int i = 0; i < rowCount; i++) {
                        final int row = i;
                        spiller.writeRows((final Block block, final int rowNum) -> {
                            boolean matched = true;
                            for (int j = 0; j < fields.size(); j++) {
                                final Object val = (columns[j] != null) ? readValue(columns[j], row) : null;
                                matched &= block.offerValue(fields.get(j).getName(), rowNum, val);
                            }
                            return matched ? 1 : 0;
                        });
                    }
                }
            }
            return null;
        });
    }

    /**
     * Reads one value from a Flight result vector as the Java type that {@code BlockUtils.setValue} expects for
     * the column's Athena type (see {@link InfluxDBTypeMapper}). Values are read from the Arrow vectors directly,
     * not through the InfluxDB client's internal converter:
     * <ul>
     *   <li>Timestamps become a UTC {@link ZonedDateTime}, using the vector's time unit.</li>
     *   <li>Integers (signed and unsigned, any width) become a Long, which BIGINT expects.</li>
     *   <li>Floating-point numbers (any width) become a Double, which FLOAT8 expects.</li>
     *   <li>Everything else uses the vector's own Java value, for example Text for strings, Boolean for booleans,
     *       Integer days for DATEDAY, and LocalDateTime for DATEMILLI.</li>
     * </ul>
     *
     * @throws AthenaConnectorException with {@code OPERATION_NOT_SUPPORTED_EXCEPTION} for a dictionary-encoded
     *             column. InfluxDB 3 sends tags as plain strings, and the client does not expose the dictionaries
     *             needed to decode the indices.
     */
    static Object readValue(final FieldVector vector, final int row)
    {
        if (vector.isNull(row)) {
            return null;
        }
        final Field field = vector.getField();
        if (field.getDictionary() != null) {
            throw InfluxDBErrors.error(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION,
                    "Dictionary-encoded column '" + field.getName() + "' is not supported");
        }
        if (vector instanceof TimeStampVector) {
            final TimeUnit unit = ((ArrowType.Timestamp) field.getType()).getUnit();
            return toZonedDateTime(((TimeStampVector) vector).get(row), unit);
        }
        if (vector instanceof BaseIntVector) {
            return ((BaseIntVector) vector).getValueAsLong(row);
        }
        if (vector instanceof FloatingPointVector) {
            return ((FloatingPointVector) vector).getValueAsDouble(row);
        }
        return vector.getObject(row);
    }

    /**
     * Converts an epoch timestamp to a UTC {@link ZonedDateTime} using the column's
     * Arrow {@link TimeUnit}, so the granularity is known rather than inferred from
     * the value's magnitude. BlockUtils.setValue for TIMESTAMPMILLITZ expects a ZonedDateTime.
     */
    static ZonedDateTime toZonedDateTime(final long epoch, final TimeUnit unit)
    {
        switch (unit) {
            case SECOND :
                return Instant.ofEpochSecond(epoch).atZone(UTC);
            case MILLISECOND :
                return Instant.ofEpochMilli(epoch).atZone(UTC);
            case MICROSECOND :
                return Instant.EPOCH.plus(epoch, ChronoUnit.MICROS).atZone(UTC);
            case NANOSECOND :
                return Instant.ofEpochSecond(0L, epoch).atZone(UTC);
            default :
                throw InfluxDBErrors.error(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION,
                        "Unsupported timestamp unit: " + unit);
        }
    }
}
