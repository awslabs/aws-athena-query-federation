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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.timestreamquery.model.ColumnInfo;
import software.amazon.awssdk.services.timestreamquery.model.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimestreamSchemaUtils
{
    private TimestreamSchemaUtils() {}

    public static Field makeField(String name, String type)
    {
        TimestreamType timeStreamType = TimestreamType.fromId(type);
        return FieldBuilder.newBuilder(name, timeStreamType.getMinorType().getType()).build();
    }

    /**
     * Builds an Arrow field from Timestream {@link ColumnInfo}, including array, row, and timeseries types.
     */
    public static Field makeFieldFromColumnInfo(ColumnInfo columnInfo)
    {
        String columnName = columnInfo.name();
        if (columnName == null) {
            columnName = "";
        }
        return fieldFromTimestreamType(columnName, columnInfo.type());
    }

    private static Field fieldFromTimestreamType(String columnName, Type type)
    {
        if (type == null) {
            throw new AthenaConnectorException(
                    "Timestream column '" + columnName + "' has no type information.",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        if (type.timeSeriesMeasureValueColumnInfo() != null) {
            return buildTimeSeriesListField(columnName, type.timeSeriesMeasureValueColumnInfo());
        }
        if (type.arrayColumnInfo() != null) {
            return buildArrayField(columnName, type.arrayColumnInfo());
        }
        if (type.hasRowColumnInfo()) {
            return buildRowField(columnName, type.rowColumnInfo());
        }
        String scalar = type.scalarTypeAsString();
        if (scalar != null && !scalar.trim().isEmpty()) {
            return makeField(columnName.isEmpty() ? "item" : columnName, scalar.trim().toLowerCase());
        }
        throw new AthenaConnectorException(
                "The Timestream connector could not map column '" + columnName
                        + "' to an Arrow type (missing scalar, array, row, or timeseries metadata). "
                        + "For more details, see https://docs.aws.amazon.com/athena/latest/ug/connectors-timestream.html",
                ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
    }

    private static Field buildTimeSeriesListField(String columnName, ColumnInfo measureValueColumnInfo)
    {
        String valueName = measureValueColumnInfo.name();
        if (valueName == null || valueName.isEmpty()) {
            valueName = "measure_value";
        }
        Field valueField = fieldFromTimestreamType(valueName, measureValueColumnInfo.type());
        Field timeField = new Field("time", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        Field structField = new Field(
                "item",
                FieldType.nullable(Types.MinorType.STRUCT.getType()),
                List.of(timeField, valueField));
        return new Field(
                columnName,
                FieldType.nullable(Types.MinorType.LIST.getType()),
                Collections.singletonList(structField));
    }

    private static Field buildArrayField(String columnName, ColumnInfo arrayElementColumnInfo)
    {
        String elementName = arrayElementColumnInfo.name();
        if (elementName == null || elementName.isEmpty()) {
            elementName = "item";
        }
        Field elementField = fieldFromTimestreamType(elementName, arrayElementColumnInfo.type());
        return new Field(
                columnName,
                FieldType.nullable(Types.MinorType.LIST.getType()),
                Collections.singletonList(elementField));
    }

    private static Field buildRowField(String columnName, List<ColumnInfo> rowColumnInfo)
    {
        List<Field> children = new ArrayList<>();
        int idx = 0;
        for (ColumnInfo col : rowColumnInfo) {
            String fieldName = col.name();
            if (fieldName == null || fieldName.isEmpty()) {
                fieldName = "field_" + idx;
            }
            idx++;
            children.add(makeFieldFromColumnInfo(ColumnInfo.builder().name(fieldName).type(col.type()).build()));
        }
        return new Field(columnName, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
    }
}
