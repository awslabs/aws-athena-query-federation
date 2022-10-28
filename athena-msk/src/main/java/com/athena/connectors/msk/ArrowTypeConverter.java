/*-
 * #%L
 * athena-msk
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.athena.connectors.msk;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class ArrowTypeConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowTypeConverter.class);

    private ArrowTypeConverter()
    {
    }

    /**
     * Convert values of the column into the column datatype defined in mapping file present in glue schema
     * This is to add column with the datatype, as mentioned in the mapping file uploaded in glue schema
     * @param typeName (column value)
     * @return ArrowType
     */
    public static ArrowType toArrowType(String typeName)
    {
        switch (typeName.toUpperCase()) {
            case "NULL":
                return new ArrowType.Null();
            case "TINYINT":
                return new ArrowType.Int(8, true);
            case "SMALLINT":
                return new ArrowType.Int(16, true);
            case "INTEGER":
                return new ArrowType.Int(32, true);
            case "BIGINT":
                return new ArrowType.Int(64, true);
            case "DOUBLE":
            case "FLOAT":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "BINARY VARYING":
                return new ArrowType.Binary();
            case "BOOLEAN":
                return new ArrowType.Bool();
            case "DECIMAL":
                return new ArrowType.Decimal(5, 5, 128);
            case "DATE":
                return new ArrowType.Date(DateUnit.DAY);
            case "TIME(3)":
            case "TIME":
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            case "TIMESTAMP(3)":
            case "TIMESTAMP":
                return new ArrowType.Date(DateUnit.MILLISECOND);
            case "INTERVAL DAY TO SECOND":
                return new ArrowType.Interval(IntervalUnit.DAY_TIME);
            case "INTERVAL YEAR TO MONTH":
                return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
            case "BINARY":
                return new ArrowType.FixedSizeBinary(50);
            case "MAP":
            case "ARRAY":
            case "UNION":
            case "CHARACTER VARYING":
            case "VARCHAR":
            default:
                return new ArrowType.Utf8();
        }
    }

    /**
     * Convert string value into particular date format
     *
     * @param value containing date
     * @return LocalDate or LocalDateTime
     */
    public static Object convertToDate(String value)
    {
        LOGGER.debug("Parsing Date " + value);
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            return LocalDate.parse(value, formatter);
        }
        catch (Exception ex) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
                value = value.contains(".") ? value : value + ".000";
                return LocalDateTime.parse(value, formatter);
            }
            catch (Exception exp) {
                LOGGER.error("Unable to parse the given Timestamp " + exp);
            }
        }
        return null;
    }
}
