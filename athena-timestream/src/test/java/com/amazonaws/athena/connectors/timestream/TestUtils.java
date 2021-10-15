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

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.mockito.stubbing.Answer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.arrow.vector.types.Types.MinorType.FLOAT8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtils
{
    private TestUtils() {}

    private static final SimpleDateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    private static final Random RAND = new Random();

    private static final String[] AZS = {"us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"};

    public static QueryResult makeMockQueryResult(Schema schemaForRead, int numRows)
    {
        QueryResult mockResult = mock(QueryResult.class);
        final AtomicLong nextToken = new AtomicLong(0);

        when(mockResult.getRows()).thenAnswer((Answer<List<Row>>) invocationOnMock -> {
                    List<Row> rows = new ArrayList<>();
                    for (int i = 0; i < 100; i++) {
                        nextToken.incrementAndGet();
                        List<Datum> columnData = new ArrayList<>();
                        for (Field nextField : schemaForRead.getFields()) {
                            columnData.add(makeValue(nextField));
                        }

                        Row row = new Row();
                        row.setData(columnData);
                        rows.add(row);
                    }
                    return rows;
                }
        );

        when(mockResult.getNextToken()).thenAnswer((Answer<String>) invocationOnMock -> {
                    if (nextToken.get() < numRows) {
                        return String.valueOf(nextToken.get());
                    }
                    return null;
                }
        );

        return mockResult;
    }

    public static Datum makeValue(Field field)
    {
        Datum datum = new Datum();
        switch (Types.getMinorTypeForArrowType(field.getType())) {
            case VARCHAR:
                if (field.getName().equals("az")) {
                    datum.setScalarValue(AZS[RAND.nextInt(4)]);
                }
                else {
                    datum.setScalarValue(field.getName() + "_" + RAND.nextInt(10_000_000));
                }
                break;
            case FLOAT8:
                datum.setScalarValue(String.valueOf(RAND.nextFloat()));
                break;
            case INT:
                datum.setScalarValue(String.valueOf(RAND.nextInt()));
                break;
            case BIT:
                datum.setScalarValue(String.valueOf(RAND.nextBoolean()));
                break;
            case BIGINT:
                datum.setScalarValue(String.valueOf(RAND.nextLong()));
                break;
            case DATEMILLI:
                datum.setScalarValue(TIMESTAMP_FORMATTER.format(new Date(System.currentTimeMillis())));
                break;
            case LIST:
                buildTimeSeries(field, datum);
                break;
            default:
                throw new RuntimeException("Unsupported field type[" + field.getType() + "] for field[" + field.getName() + "]");
        }

        return datum;
    }

    private static void buildTimeSeries(Field field, Datum datum)
    {
        List<TimeSeriesDataPoint> dataPoints = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TimeSeriesDataPoint dataPoint = new TimeSeriesDataPoint();
            Datum dataPointValue = new Datum();

            dataPoint.setTime(TIMESTAMP_FORMATTER.format(new Date(System.currentTimeMillis() - RAND.nextInt(1_000_000))));

            /**
             * Presently we only support TimeSeries as LIST<STRUCT<DATEMILLISECONDS, DOUBLE|INT|FLOAT8|BIT|BIGINT>>
             */
            Field struct = field.getChildren().get(0);
            assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(struct.getType()));
            Field baseSeriesType = struct.getChildren().get(1);

            switch (Types.getMinorTypeForArrowType(baseSeriesType.getType())) {
                case FLOAT8:
                    dataPointValue.setScalarValue(String.valueOf(RAND.nextFloat()));
                    break;
                case BIT:
                    dataPointValue.setScalarValue(String.valueOf(RAND.nextBoolean()));
                    break;
                case INT:
                    dataPointValue.setScalarValue(String.valueOf(RAND.nextInt()));
                    break;
                case BIGINT:
                    dataPointValue.setScalarValue(String.valueOf(RAND.nextLong()));
                    break;
            }

            dataPoint.setValue(dataPointValue);
            dataPoints.add(dataPoint);
        }
        datum.setTimeSeriesValue(dataPoints);
    }
}
