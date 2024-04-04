/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.timestream.qpt.TimestreamQueryPassthrough;
import com.amazonaws.athena.connectors.timestream.query.QueryFactory;
import com.amazonaws.athena.connectors.timestream.query.SelectQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimestreamRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamRecordHandler.class);
    //Time stream `yyyy-MM-dd HH:mm:ss` doesn't contain zone information, treat everything as UTC
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss.")
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 9, false)
            .toFormatter()
            .withZone(ZoneId.of("UTC"));

    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "timestream";

    private final QueryFactory queryFactory = new QueryFactory();
    private final AmazonTimestreamQuery tsQuery;
    private final TimestreamQueryPassthrough queryPassthrough = new TimestreamQueryPassthrough();

    public TimestreamRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(
            AmazonS3ClientBuilder.defaultClient(),
            AWSSecretsManagerClientBuilder.defaultClient(),
            AmazonAthenaClientBuilder.defaultClient(),
            TimestreamClientBuilder.buildQueryClient(SOURCE_TYPE),
            configOptions);
    }

    @VisibleForTesting
    protected TimestreamRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, AmazonTimestreamQuery tsQuery, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE, configOptions);
        this.tsQuery = tsQuery;
    }

    /**
     * Scans TimeStream.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        TableName tableName = recordsRequest.getTableName();
        String query;
        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            queryPassthrough.verify(recordsRequest.getConstraints().getQueryPassthroughArguments());
            query = recordsRequest.getConstraints().getQueryPassthroughArguments().get(TimestreamQueryPassthrough.QUERY);
        }
        else {
            SelectQueryBuilder queryBuilder = queryFactory.createSelectQueryBuilder(GlueMetadataHandler.VIEW_METADATA_FIELD);
            query = queryBuilder.withDatabaseName(tableName.getSchemaName())
                    .withTableName(tableName.getTableName())
                    .withProjection(recordsRequest.getSchema())
                    .withConjucts(recordsRequest.getConstraints())
                    .build();
        }

        logger.info("readWithConstraint: query[{}]", query);

        GeneratedRowWriter rowWriter = buildRowWriter(recordsRequest);
        String nextToken = null;
        long numRows = 0;

        do {
            QueryResult queryResult = tsQuery.query(new QueryRequest().withQueryString(query).withNextToken(nextToken));
            List<Row> data = queryResult.getRows();
            if (data != null) {
                numRows += data.size();
                for (Row nextRow : data) {
                    spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, nextRow) ? 1 : 0);
                }
            }
            nextToken = queryResult.getNextToken();
            logger.info("readWithConstraint: numRows[{}]", numRows);
        } while (nextToken != null && !nextToken.isEmpty());
    }

    private GeneratedRowWriter buildRowWriter(ReadRecordsRequest request)
    {
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(request.getConstraints());

        int fieldNum = 0;
        for (Field nextField : request.getSchema().getFields()) {
            int curFieldNum = fieldNum++;
            switch (Types.getMinorTypeForArrowType(nextField.getType())) {
                case VARCHAR:
                    builder.withExtractor(nextField.getName(), (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                        String stringValue = ((Row) context).getData().get(curFieldNum).getScalarValue();
                        if (stringValue != null) {
                            value.isSet = 1;
                            value.value = stringValue;
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case FLOAT8:
                    builder.withExtractor(nextField.getName(), (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
                        String doubleValue = ((Row) context).getData().get(curFieldNum).getScalarValue();
                        if (doubleValue != null) {
                            value.isSet = 1;
                            value.value = Double.valueOf(doubleValue);
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case BIT:
                    builder.withExtractor(nextField.getName(), (BitExtractor) (Object context, NullableBitHolder value) -> {
                        value.isSet = 1;
                        value.value = Boolean.valueOf(((Row) context).getData().get(curFieldNum).getScalarValue()) == false ? 0 : 1;
                    });
                    break;
                case BIGINT:
                    builder.withExtractor(nextField.getName(), (BigIntExtractor) (Object context, NullableBigIntHolder value) -> {
                        String longValue = ((Row) context).getData().get(curFieldNum).getScalarValue();
                        if (longValue != null) {
                            value.isSet = 1;
                            value.value = Long.valueOf(longValue);
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case DATEMILLI:
                    builder.withExtractor(nextField.getName(), (DateMilliExtractor) (Object context, NullableDateMilliHolder value) -> {
                        String dateMilliValue = ((Row) context).getData().get(curFieldNum).getScalarValue();
                        if (dateMilliValue != null) {
                            value.isSet = 1;
                            value.value = Instant.from(TIMESTAMP_FORMATTER.parse(dateMilliValue)).toEpochMilli();
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case LIST:
                    //TODO: This presently only supports TimeSeries results but it is possible that customers may
                    //generate LIST type results for other reasons when using VIEWs. For now this seems like an OK
                    //compromise since it enables an important capability of TimeStream even if it doesn't enable arbitrary
                    //complex types.
                    buildTimeSeriesExtractor(builder, nextField, curFieldNum);
                    break;
                default:
                    throw new RuntimeException("Unsupported field type[" + nextField.getType() + "] for field[" + nextField.getName() + "]");
            }
        }
        return builder.build();
    }

    private void buildTimeSeriesExtractor(GeneratedRowWriter.RowWriterBuilder builder, Field field, int curFieldNum)
    {
        builder.withFieldWriterFactory(field.getName(),
                (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (Object context, int rowNum) -> {
                            Row row = (Row) context;
                            Datum datum = row.getData().get(curFieldNum);
                            Field timeField = field.getChildren().get(0).getChildren().get(0);
                            Field valueField = field.getChildren().get(0).getChildren().get(1);

                            if (datum.getTimeSeriesValue() != null) {
                                List<Map<String, Object>> values = new ArrayList<>();
                                for (TimeSeriesDataPoint nextDatum : datum.getTimeSeriesValue()) {
                                    Map<String, Object> eventMap = new HashMap<>();

                                    eventMap.put(timeField.getName(), Instant.from(TIMESTAMP_FORMATTER.parse(nextDatum.getTime())).toEpochMilli());

                                    switch (Types.getMinorTypeForArrowType(valueField.getType())) {
                                        case FLOAT8:
                                            eventMap.put(valueField.getName(), Double.valueOf(nextDatum.getValue().getScalarValue()));
                                            break;
                                        case BIGINT:
                                            eventMap.put(valueField.getName(), Long.valueOf(nextDatum.getValue().getScalarValue()));
                                            break;
                                        case INT:
                                            eventMap.put(valueField.getName(), Integer.valueOf(nextDatum.getValue().getScalarValue()));
                                            break;
                                        case BIT:
                                            eventMap.put(valueField.getName(),
                                                    Boolean.valueOf(((Row) context).getData().get(curFieldNum).getScalarValue()) == false ? 0 : 1);
                                            break;
                                    }
                                    values.add(eventMap);
                                }
                                BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, values);
                            }
                            else {
                                throw new RuntimeException("Only LISTs of type TimeSeries are presently supported.");
                            }

                            return true;    //we don't yet support predicate pushdown on complex types
                        });
    }
}
