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
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.influxdb.v3.client.internal.VectorSchemaRootConverter;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
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

            final String timeLower = recordsRequest.getSplit().getProperty(PART_TIME_LOWER);
            final String timeUpper = recordsRequest.getSplit().getProperty(PART_TIME_UPPER);
            sql = InfluxDBQueryBuilder.buildSql(schema, resolvedTable, recordsRequest.getConstraints(),
                    timeLower, timeUpper);
            logger.info("readWithConstraint: schema={}, table={}", schemaName, resolvedTable);
        }
        // The SQL embeds constraint literal values (possible PII) so it is logged only at debug.
        logger.debug("readWithConstraint SQL: {}", sql);

        final List<Field> fields = schema.getFields();
        // Pre-compute each column's Athena type.
        final Types.MinorType[] athenaTypes = new Types.MinorType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            athenaTypes[i] = Types.getMinorTypeForArrowType(fields.get(i).getType());
        }

        connectionFactory.executeWithTokenRetry(resolvedDB, client -> {
            // queryBatches returns Arrow VectorSchemaRoots, so each timestamp column's
            // precision is known from its Arrow type rather than guessed from magnitude.
            try (Stream<VectorSchemaRoot> batches = client.queryBatches(sql)) {
                final Iterator<VectorSchemaRoot> it = batches.iterator();
                while (queryStatusChecker.isQueryRunning() && it.hasNext()) {
                    final VectorSchemaRoot root = it.next();
                    final List<FieldVector> vectors = root.getFieldVectors();
                    // Resolve each result column to its position by name.
                    final Map<String, Integer> arrowIndexByName = new HashMap<>();
                    for (int i = 0; i < vectors.size(); i++) {
                        arrowIndexByName.put(vectors.get(i).getField().getName(), i);
                    }
                    final int rowCount = root.getRowCount();
                    for (int i = 0; i < rowCount; i++) {
                        final int rowIdx = i;
                        // Reuse the client's converter for value extraction (handles
                        // dictionary-encoded tags, Utf8, numerics, booleans). Values are returned in
                        // Arrow column order, so they are indexed by the resolved Arrow position.
                        final Object[] values =
                                VectorSchemaRootConverter.INSTANCE.getArrayObjectFromVectorSchemaRoot(root, rowIdx);
                        spiller.writeRows((final Block block, final int rowNum) -> {
                            boolean matched = true;
                            for (int j = 0; j < fields.size(); j++) {
                                final String fieldName = fields.get(j).getName();
                                final Integer col = arrowIndexByName.get(fieldName);
                                // A schema field absent from the result maps to null.
                                Object val = (col != null) ? values[col] : null;
                                final ArrowType sourceType = (col != null) ? vectors.get(col).getField().getType() : null;
                                if (val != null && athenaTypes[j] == Types.MinorType.TIMESTAMPMILLITZ
                                        && sourceType instanceof ArrowType.Timestamp) {
                                    // BlockUtils.setValue for TIMESTAMPMILLITZ expects a ZonedDateTime.
                                    // Convert using the column's actual Arrow time unit.
                                    final TimeUnit unit = ((ArrowType.Timestamp) sourceType).getUnit();
                                    val = toZonedDateTime(((TimeStampVector) vectors.get(col)).get(rowIdx), unit);
                                }
                                else if (sourceType instanceof ArrowType.Date) {
                                    // Read dates from the vector. A date computed from the time column (for
                                    // example CAST(time AS DATE)) keeps time's iox::column::type metadata, and
                                    // the client's converter returns null for it.
                                    val = vectors.get(col).getObject(rowIdx);
                                }
                                else {
                                    val = toAthenaValue(val, athenaTypes[j]);
                                }
                                matched &= block.offerValue(fieldName, rowNum, val);
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
     * Converts a value read from a Flight result to the Java type that {@code BlockUtils.setValue} expects for the
     * column's Athena type. {@link InfluxDBTypeMapper} widens every integer type to BIGINT and every floating-point
     * type to FLOAT8, but the vectors return narrower Java types (for example, Integer for Int32, Character for
     * UInt16, and Float for Float32), which {@code setValue} would reject with a ClassCastException.
     */
    static Object toAthenaValue(final Object value, final Types.MinorType athenaType)
    {
        if (athenaType == Types.MinorType.BIGINT) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value instanceof Character) {
                return (long) (Character) value;
            }
        }
        if (athenaType == Types.MinorType.FLOAT8 && value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return value;
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
