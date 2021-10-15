/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.integ;

import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.TimeUnit;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used to build a WriteRecordRequest object that can be used to insert records into a Timestream
 * database table. Each WriteRecordRequest object can be constructed with multiple table records that can be inserted
 * into the table with a single request.
 */
public class TimestreamWriteRecordRequestBuilder
{
    private final List<Record> records;

    private String databaseName;
    private String tableName;
    private String measureName;
    private MeasureValueType measureValueType;

    TimestreamWriteRecordRequestBuilder()
    {
        this.records = new ArrayList<>();
    }

    /**
     * Sets the Timestream database where the record will be inserted.
     * @param databaseName The name of the Timestream database.
     * @return A builder object.
     */
    public TimestreamWriteRecordRequestBuilder withDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Sets the Timestream table where the record will be inserted.
     * @param tableName The name of the Timestream table.
     * @return a builder object.
     */
    public TimestreamWriteRecordRequestBuilder withTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    /**
     * Sets the Timestream table's Measure column used for tracking information.
     * @param measureName The name of the Measure column (e.g. speed, rate, etc...)
     * @return A builder object.
     */
    public TimestreamWriteRecordRequestBuilder withMeasureName(String measureName)
    {
        this.measureName = measureName;
        return this;
    }

    /**
     * Sets the Timestream table Measure column's type.
     * @param measureValueType The type of the Measure column (e.g. DOUBLE, VARCHAR, etc...)
     * @return A builder object.
     */
    public TimestreamWriteRecordRequestBuilder withMeasureValueType(MeasureValueType measureValueType)
    {
        this.measureValueType = measureValueType;
        return this;
    }

    /**
     * Constructs a Timestream table record and adds it to a list used for inserting multiple records with a single
     * request.
     * @param columns A Map of column names and their values.
     * @param measureValue The value of the Measure column associated with the record.
     * @param timeMillis Current time in milliseconds.
     * @return A builder object.
     */
    public TimestreamWriteRecordRequestBuilder withRecord(Map<String, String> columns, String measureValue,
                                                          long timeMillis)
    {
        List<Dimension> dimensions = new ArrayList<>();
        columns.forEach((k, v) -> dimensions.add(new Dimension().withName(k).withValue(v)));
        records.add(new Record()
                .withDimensions(dimensions)
                .withMeasureName(measureName)
                .withMeasureValue(measureValue)
                .withMeasureValueType(measureValueType)
                .withTime(String.valueOf(timeMillis))
                .withTimeUnit(TimeUnit.MILLISECONDS));
        return this;
    }

    /**
     * Builds the request used to insert the records into the Timestream table.
     * @return A write records request object.
     */
    public WriteRecordsRequest build()
    {
        return new WriteRecordsRequest()
                .withDatabaseName(databaseName)
                .withTableName(tableName)
                .withRecords(records);
    }
}
