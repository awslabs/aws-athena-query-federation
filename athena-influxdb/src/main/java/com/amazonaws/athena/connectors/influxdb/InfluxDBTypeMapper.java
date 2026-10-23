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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Locale;

/**
 * Maps InfluxDB 3 column types to the Athena (Arrow) types the connector returns.
 *
 * Table schemas ({@code doGetTable}) and passthrough schemas ({@code doGetQueryPassthroughSchema}) both use
 * {@link #toAthenaType(ArrowType)}, so a column has the same Athena type on both paths:
 * <ul>
 *   <li>All timestamps become millisecond UTC timestamps ({@link #ATHENA_TIMESTAMP}).</li>
 *   <li>Dates keep their unit (DATEDAY or DATEMILLI).</li>
 *   <li>All signed and unsigned integers become BIGINT. An unsigned 64-bit value above
 *       {@link Long#MAX_VALUE} wraps to a negative number, because Athena has no unsigned type.</li>
 *   <li>All floating-point numbers become FLOAT8.</li>
 *   <li>Booleans become BIT.</li>
 *   <li>Tags (dictionary-encoded strings), strings, and every other type become VARCHAR.</li>
 * </ul>
 */
final class InfluxDBTypeMapper
{
    /** The Athena type for every InfluxDB timestamp column. */
    static final ArrowType ATHENA_TIMESTAMP = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");

    private InfluxDBTypeMapper()
    {
    }

    /**
     * Maps a column of a Flight query result to its Athena type. A dictionary-encoded column (an InfluxDB tag)
     * maps to VARCHAR regardless of its index type.
     */
    static ArrowType toAthenaType(final Field field)
    {
        if (field.getDictionary() != null) {
            return Types.MinorType.VARCHAR.getType();
        }
        return toAthenaType(field.getType());
    }

    /**
     * Maps a column's {@code data_type} value from {@code information_schema.columns} to its Athena type.
     */
    static ArrowType toAthenaType(final String dataType)
    {
        return toAthenaType(parseDataType(dataType));
    }

    /**
     * Maps an InfluxDB Arrow type to its Athena type.
     */
    static ArrowType toAthenaType(final ArrowType influxType)
    {
        switch (Types.getMinorTypeForArrowType(influxType)) {
            case TIMESTAMPNANOTZ:
            case TIMESTAMPMICROTZ:
            case TIMESTAMPMILLITZ:
            case TIMESTAMPSECTZ:
            case TIMESTAMPNANO:
            case TIMESTAMPMICRO:
            case TIMESTAMPMILLI:
            case TIMESTAMPSEC:
                return ATHENA_TIMESTAMP;
            case DATEDAY:
                return Types.MinorType.DATEDAY.getType();
            case DATEMILLI:
                return Types.MinorType.DATEMILLI.getType();
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
            case UINT8:
            case UINT4:
            case UINT2:
            case UINT1:
                return Types.MinorType.BIGINT.getType();
            case FLOAT8:
            case FLOAT4:
            case FLOAT2:
                return Types.MinorType.FLOAT8.getType();
            case BIT:
                return Types.MinorType.BIT.getType();
            default:
                return Types.MinorType.VARCHAR.getType();
        }
    }

    /**
     * Parses a {@code data_type} value from {@code information_schema.columns} into an Arrow type. InfluxDB 3
     * (DataFusion) reports the Arrow type's display name, for example {@code Int64}, {@code UInt64},
     * {@code Float64}, {@code Boolean}, {@code Utf8}, {@code Date32}, {@code Timestamp(Nanosecond, None)}, or
     * {@code Dictionary(Int32, Utf8)} for a tag. A few SQL type names are also accepted. An unrecognized name
     * parses as Utf8.
     */
    static ArrowType parseDataType(final String dataType)
    {
        final String upper = dataType.trim().toUpperCase(Locale.ROOT);
        if (upper.startsWith("DICTIONARY")) {
            return ArrowType.Utf8.INSTANCE;
        }
        if (upper.startsWith("TIMESTAMP")) {
            return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
        }
        switch (upper) {
            case "INT8":
            case "TINYINT":
                return new ArrowType.Int(8, true);
            case "INT16":
            case "SMALLINT":
                return new ArrowType.Int(16, true);
            case "INT32":
            case "INT":
            case "INTEGER":
                return new ArrowType.Int(32, true);
            case "INT64":
            case "BIGINT":
                return new ArrowType.Int(64, true);
            case "UINT8":
                return new ArrowType.Int(8, false);
            case "UINT16":
                return new ArrowType.Int(16, false);
            case "UINT32":
                return new ArrowType.Int(32, false);
            case "UINT64":
                return new ArrowType.Int(64, false);
            case "FLOAT32":
            case "FLOAT":
            case "REAL":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "FLOAT64":
            case "FLOAT8":
            case "DOUBLE":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "BOOLEAN":
            case "BOOL":
                return ArrowType.Bool.INSTANCE;
            case "DATE32":
            case "DATE":
                return new ArrowType.Date(DateUnit.DAY);
            case "DATE64":
                return new ArrowType.Date(DateUnit.MILLISECOND);
            default:
                return ArrowType.Utf8.INSTANCE;
        }
    }
}
