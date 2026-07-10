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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.PART_TIME_LOWER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.PART_TIME_UPPER;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.SOURCE_TYPE;

public class InfluxDbRecordHandler
        extends
            RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbRecordHandler.class);
    private static final ZoneId UTC = ZoneId.of("UTC");

    private final InfluxDbConnectionFactory connectionFactory;

    public InfluxDbRecordHandler(final Map<String, String> configOptions)
    {
        this(S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                new InfluxDbConnectionFactory(configOptions, null),
                configOptions);
    }

    @VisibleForTesting
    protected InfluxDbRecordHandler(
            final S3Client s3Client,
            final SecretsManagerClient secretsManager,
            final AthenaClient athena,
            final InfluxDbConnectionFactory connectionFactory,
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
        String tableName = recordsRequest.getTableName().getTableName();
        final String schemaName = recordsRequest.getTableName().getSchemaName();

        // Use the original case-sensitive table name stored by the MetadataHandler.
        final String originalTableName = schema.getCustomMetadata().get("originalTableName");
        if (originalTableName != null) {
            tableName = originalTableName;
        }

        final String resolvedDb = schema.getCustomMetadata().get("resolvedDatabaseName");

        final String timeLower = recordsRequest.getSplit().getProperty(PART_TIME_LOWER);
        final String timeUpper = recordsRequest.getSplit().getProperty(PART_TIME_UPPER);
        final String sql = InfluxDbQueryBuilder.buildSql(schema, tableName, recordsRequest.getConstraints(),
                timeLower, timeUpper);
        logger.info("readWithConstraint: schema={}, sql={}", schemaName, sql);

        final List<Field> fields = schema.getFields();
        // Pre-compute which columns are timestamps
        final boolean[] isTimestamp = new boolean[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            final Types.MinorType mt = Types.getMinorTypeForArrowType(fields.get(i).getType());
            isTimestamp[i] = (mt == Types.MinorType.TIMESTAMPMILLITZ || mt == Types.MinorType.DATEMILLI);
        }

        connectionFactory.executeWithTokenRetry(resolvedDb, client -> {
            // queryBatches returns Arrow VectorSchemaRoots, so each timestamp column's
            // precision is known from its Arrow type rather than guessed from magnitude.
            try (Stream<VectorSchemaRoot> batches = client.queryBatches(sql)) {
            batches.forEach(root -> {
                if (!queryStatusChecker.isQueryRunning()) {
                    return;
                }
                final List<FieldVector> vectors = root.getFieldVectors();
                final int rowCount = root.getRowCount();
                for (int r = 0; r < rowCount; r++) {
                    final int rowIdx = r;
                    // Reuse the client's converter for value extraction (handles
                    // dictionary-encoded tags, Utf8, numerics, booleans).
                    final Object[] values =
                            VectorSchemaRootConverter.INSTANCE.getArrayObjectFromVectorSchemaRoot(root, rowIdx);
                    spiller.writeRows((final Block block, final int rowNum) -> {
                        boolean matched = true;
                        for (int i = 0; i < fields.size(); i++) {
                            Object val = values[i];
                            if (val != null && isTimestamp[i]) {
                                // BlockUtils.setValue for TIMESTAMPMILLITZ expects a ZonedDateTime.
                                // Convert using the column's actual Arrow time unit.
                                final FieldVector vector = vectors.get(i);
                                if (vector.getField().getType() instanceof ArrowType.Timestamp) {
                                    final TimeUnit unit =
                                            ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
                                    val = toZonedDateTime(((TimeStampVector) vector).get(rowIdx), unit);
                                }
                            }
                            matched &= block.offerValue(fields.get(i).getName(), rowNum, val);
                        }
                        return matched ? 1 : 0;
                    });
                }
            });
            }
            return null;
        });
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
                throw new IllegalArgumentException("Unsupported timestamp unit: " + unit);
        }
    }
}
