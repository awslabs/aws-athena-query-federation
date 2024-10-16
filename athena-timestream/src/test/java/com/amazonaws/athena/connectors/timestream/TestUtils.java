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

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.Row;
import software.amazon.awssdk.services.timestreamquery.model.TimeSeriesDataPoint;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtils
{
    private TestUtils() {}

    static final LocalDateTime startDate = LocalDateTime.now();

    private static final Random RAND = new Random();

    private static final String[] AZS = {"us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"};

    public static QueryResponse makeMockQueryResult(Schema schemaForRead, int numRows)
    {
        return makeMockQueryResult(schemaForRead, numRows, 100, true);
    }

    public static QueryResponse makeMockQueryResult(Schema schemaForRead, int numRows, int maxDataGenerationRow, boolean isRandomAZ)
    {
        QueryResponse mockResult = mock(QueryResponse.class);
        final AtomicLong nextToken = new AtomicLong(0);

        when(mockResult.rows()).thenAnswer((Answer<List<Row>>) invocationOnMock -> {
                    List<Row> rows = new ArrayList<>();
                    for (int i = 0; i < maxDataGenerationRow; i++) {
                        nextToken.incrementAndGet();
                        List<Datum> columnData = new ArrayList<>();
                        for (Field nextField : schemaForRead.getFields()) {
                            columnData.add(makeValue(nextField, i, isRandomAZ));
                        }

                        Row row = Row.builder().data(columnData).build();
                        rows.add(row);
                    }
                    return rows;
                }
        );

        when(mockResult.nextToken()).thenAnswer((Answer<String>) invocationOnMock -> {
                    if (nextToken.get() < numRows) {
                        return String.valueOf(nextToken.get());
                    }
                    return null;
                }
        );

        return mockResult;
    }

    public static Datum makeValue(Field field, int num, boolean isRandomAZ)
    {
        Datum.Builder datum = Datum.builder();
        switch (Types.getMinorTypeForArrowType(field.getType())) {
            case VARCHAR:
                if (field.getName().equals("az")) {
                    datum.scalarValue(isRandomAZ ? AZS[RAND.nextInt(4)] : "us-east-1a");
                }
                else {
                    datum.scalarValue(field.getName() + "_" + RAND.nextInt(10_000_000));
                }
                break;
            case FLOAT8:
                datum.scalarValue(String.valueOf(RAND.nextFloat()));
                break;
            case INT:
                datum.scalarValue(String.valueOf(RAND.nextInt()));
                break;
            case BIT:
                datum.scalarValue(String.valueOf(RAND.nextBoolean()));
                break;
            case BIGINT:
                datum.scalarValue(String.valueOf(RAND.nextLong()));
                break;
            case DATEMILLI:
                datum.scalarValue(startDate.plusDays(num).toString().replace('T', ' '));
                break;
            case LIST:
                buildTimeSeries(field, datum, num);
                break;
            default:
                throw new RuntimeException("Unsupported field type[" + field.getType() + "] for field[" + field.getName() + "]");
        }

        return datum.build();
    }

    private static void buildTimeSeries(Field field, Datum.Builder datum, int num)
    {
        List<TimeSeriesDataPoint> dataPoints = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TimeSeriesDataPoint.Builder dataPoint = TimeSeriesDataPoint.builder();
            Datum.Builder dataPointValue = Datum.builder();

            dataPoint.time(startDate.plusDays(num).toString().replace('T', ' '));

            /**
             * Presently we only support TimeSeries as LIST<STRUCT<DATEMILLISECONDS, DOUBLE|INT|FLOAT8|BIT|BIGINT>>
             */
            Field struct = field.getChildren().get(0);
            assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(struct.getType()));
            Field baseSeriesType = struct.getChildren().get(1);

            switch (Types.getMinorTypeForArrowType(baseSeriesType.getType())) {
                case FLOAT8:
                    dataPointValue.scalarValue(String.valueOf(RAND.nextFloat()));
                    break;
                case BIT:
                    dataPointValue.scalarValue(String.valueOf(RAND.nextBoolean()));
                    break;
                case INT:
                    dataPointValue.scalarValue(String.valueOf(RAND.nextInt()));
                    break;
                case BIGINT:
                    dataPointValue.scalarValue(String.valueOf(RAND.nextLong()));
                    break;
            }

            dataPoint.value(dataPointValue.build());
            dataPoints.add(dataPoint.build());
        }
        datum.timeSeriesValue(dataPoints);
    }
}
