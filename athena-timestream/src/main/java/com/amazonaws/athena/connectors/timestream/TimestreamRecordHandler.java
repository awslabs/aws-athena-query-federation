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
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
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
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.Row;
import software.amazon.awssdk.services.timestreamquery.model.TimeSeriesDataPoint;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "timestream";

    private final QueryFactory queryFactory = new QueryFactory();
    private final TimestreamQueryClient tsQuery;
    private final TimestreamQueryPassthrough queryPassthrough = new TimestreamQueryPassthrough();

    public TimestreamRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(
            S3Client.create(),
            SecretsManagerClient.create(),
            AthenaClient.create(),
            TimestreamClientBuilder.buildQueryClient(SOURCE_TYPE),
            configOptions);
    }

    @VisibleForTesting
    protected TimestreamRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, TimestreamQueryClient tsQuery, java.util.Map<String, String> configOptions)
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
            QueryResponse queryResult = tsQuery.query(QueryRequest.builder().queryString(query).nextToken(nextToken).build());
            List<Row> data = queryResult.rows();
            if (data != null) {
                numRows += data.size();
                for (Row nextRow : data) {
                    spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, nextRow) ? 1 : 0);
                }
            }
            nextToken = queryResult.nextToken();
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
                        String stringValue = ((Row) context).data().get(curFieldNum).scalarValue();
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
                        String doubleValue = ((Row) context).data().get(curFieldNum).scalarValue();
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
                        String boolValue = ((Row) context).data().get(curFieldNum).scalarValue();
                        if (boolValue != null) {
                            value.isSet = 1;
                            value.value = Boolean.parseBoolean(boolValue) ? 1 : 0;
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case BIGINT:
                    builder.withExtractor(nextField.getName(), (BigIntExtractor) (Object context, NullableBigIntHolder value) -> {
                        String longValue = ((Row) context).data().get(curFieldNum).scalarValue();
                        if (longValue != null) {
                            value.isSet = 1;
                            value.value = Long.valueOf(longValue);
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case INT:
                    builder.withExtractor(nextField.getName(), (IntExtractor) (Object context, NullableIntHolder value) -> {
                        String intValue = ((Row) context).data().get(curFieldNum).scalarValue();
                        if (intValue != null) {
                            value.isSet = 1;
                            value.value = Integer.valueOf(intValue);
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case DATEMILLI:
                    builder.withExtractor(nextField.getName(), (DateMilliExtractor) (Object context, NullableDateMilliHolder value) -> {
                        String dateMilliValue = ((Row) context).data().get(curFieldNum).scalarValue();
                        if (dateMilliValue != null) {
                            value.isSet = 1;
                            value.value = Instant.from(TIMESTAMP_FORMATTER.parse(dateMilliValue)).toEpochMilli();
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case DATEDAY:
                    builder.withExtractor(nextField.getName(), (DateDayExtractor) (Object context, NullableDateDayHolder value) -> {
                        String dateValue = ((Row) context).data().get(curFieldNum).scalarValue();
                        if (dateValue != null) {
                            value.isSet = 1;
                            value.value = (int) LocalDate.parse(dateValue, DATE_FORMATTER).toEpochDay();
                        }
                        else {
                            value.isSet = 0;
                        }
                    });
                    break;
                case LIST:
                    if (isTimeSeriesListField(nextField)) {
                        buildTimeSeriesExtractor(builder, nextField, curFieldNum);
                    }
                    else {
                        buildListExtractor(builder, nextField, curFieldNum);
                    }
                    break;
                case STRUCT:
                    buildStructExtractor(builder, nextField, curFieldNum);
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
                            Datum datum = row.data().get(curFieldNum);
                            if (Boolean.TRUE.equals(datum.nullValue())) {
                                return true;
                            }
                            if (!datum.hasTimeSeriesValue()) {
                                throw new RuntimeException("Expected TimeSeries payload for column[" + field.getName() + "]");
                            }
                            List<Map<String, Object>> values = buildTimeSeriesRowMaps(datum, field);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, values);
                            return true;
                        });
    }

    private void buildListExtractor(GeneratedRowWriter.RowWriterBuilder builder, Field field, int curFieldNum)
    {
        builder.withFieldWriterFactory(field.getName(),
                (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (Object context, int rowNum) -> {
                            Datum datum = ((Row) context).data().get(curFieldNum);
                            if (Boolean.TRUE.equals(datum.nullValue())) {
                                return true;
                            }
                            Object value = convertDatumToObject(datum, field);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, value);
                            return true;
                        });
    }

    private void buildStructExtractor(GeneratedRowWriter.RowWriterBuilder builder, Field field, int curFieldNum)
    {
        builder.withFieldWriterFactory(field.getName(),
                (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (Object context, int rowNum) -> {
                            Datum datum = ((Row) context).data().get(curFieldNum);
                            if (Boolean.TRUE.equals(datum.nullValue())) {
                                return true;
                            }
                            Object value = convertDatumToObject(datum, field);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, value);
                            return true;
                        });
    }

    /**
     * Timestream time series columns are modeled as {@code LIST<STRUCT<time, measure_value>>} with {@code time} first.
     */
    private static boolean isTimeSeriesListField(Field listField)
    {
        if (listField.getChildren().size() != 1) {
            return false;
        }
        Field inner = listField.getChildren().get(0);
        if (Types.getMinorTypeForArrowType(inner.getType()) != Types.MinorType.STRUCT) {
            return false;
        }
        if (inner.getChildren().size() != 2) {
            return false;
        }
        Field timeChild = inner.getChildren().get(0);
        return "time".equals(timeChild.getName())
                && Types.getMinorTypeForArrowType(timeChild.getType()) == Types.MinorType.DATEMILLI;
    }

    private List<Map<String, Object>> buildTimeSeriesRowMaps(Datum datum, Field listField)
    {
        Field structField = listField.getChildren().get(0);
        Field timeField = structField.getChildren().get(0);
        Field valueField = structField.getChildren().get(1);
        List<Map<String, Object>> values = new ArrayList<>();
        for (TimeSeriesDataPoint nextDatum : datum.timeSeriesValue()) {
            Map<String, Object> eventMap = new HashMap<>();
            eventMap.put(timeField.getName(), Instant.from(TIMESTAMP_FORMATTER.parse(nextDatum.time())).toEpochMilli());
            eventMap.put(valueField.getName(), parseMeasureScalar(nextDatum.value(), valueField));
            values.add(eventMap);
        }
        return values;
    }

    private static Object parseMeasureScalar(Datum measureDatum, Field valueField)
    {
        String sv = measureDatum.scalarValue();
        if (sv == null) {
            return null;
        }
        switch (Types.getMinorTypeForArrowType(valueField.getType())) {
            case FLOAT8:
                return Double.valueOf(sv);
            case BIGINT:
                return Long.valueOf(sv);
            case INT:
                return Integer.valueOf(sv);
            case BIT:
                return Boolean.parseBoolean(sv) ? 1 : 0;
            case VARCHAR:
                return sv;
            default:
                throw new RuntimeException("Unsupported time series measure type[" + valueField.getType() + "]");
        }
    }

    private Object convertDatumToObject(Datum datum, Field field)
    {
        if (datum == null || Boolean.TRUE.equals(datum.nullValue())) {
            return null;
        }
        Types.MinorType minor = Types.getMinorTypeForArrowType(field.getType());
        switch (minor) {
            case LIST:
                Field elementField = field.getChildren().get(0);
                if (isTimeSeriesListField(field) && datum.hasTimeSeriesValue()) {
                    return buildTimeSeriesRowMaps(datum, field);
                }
                if (datum.hasArrayValue()) {
                    List<Object> elements = new ArrayList<>();
                    for (Datum next : datum.arrayValue()) {
                        elements.add(convertDatumToObject(next, elementField));
                    }
                    return elements;
                }
                throw new RuntimeException("Unexpected LIST payload for field[" + field.getName() + "]");
            case STRUCT:
                if (datum.rowValue() == null) {
                    return null;
                }
                software.amazon.awssdk.services.timestreamquery.model.Row structRow = datum.rowValue();
                Map<String, Object> map = new LinkedHashMap<>();
                List<Field> children = field.getChildren();
                for (int i = 0; i < children.size(); i++) {
                    Field childField = children.get(i);
                    Datum childDatum = structRow.data().get(i);
                    map.put(childField.getName(), convertDatumToObject(childDatum, childField));
                }
                return map;
            default:
                return parseScalarString(datum.scalarValue(), field);
        }
    }

    private static Object parseScalarString(String scalarValue, Field field)
    {
        if (scalarValue == null) {
            return null;
        }
        switch (Types.getMinorTypeForArrowType(field.getType())) {
            case VARCHAR:
                return scalarValue;
            case FLOAT8:
                return Double.valueOf(scalarValue);
            case BIT:
                return Boolean.parseBoolean(scalarValue) ? 1 : 0;
            case BIGINT:
                return Long.valueOf(scalarValue);
            case INT:
                return Integer.valueOf(scalarValue);
            case DATEMILLI:
                return Instant.from(TIMESTAMP_FORMATTER.parse(scalarValue)).toEpochMilli();
            case DATEDAY:
                return (int) LocalDate.parse(scalarValue, DATE_FORMATTER).toEpochDay();
            default:
                return scalarValue;
        }
    }
}
