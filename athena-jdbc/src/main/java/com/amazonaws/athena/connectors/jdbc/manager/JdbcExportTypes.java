/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/**
 * Type mapping and value reading for the query-passthrough export ({@link JdbcRecordHandler#EXPORT_SCHEMA}). Each
 * column maps to a type the spill can write, and each value is read with the {@link ResultSet} getter that matches
 * that type, so driver-specific objects (LOBs, vendor timestamp classes) never reach {@code BlockUtils.setValue}.
 *
 * <p>Connectors map their vendor-specific JDBC types first and use {@link #defaultArrowType} and {@link #readValue}
 * for the rest.
 */
public final class JdbcExportTypes
{
    /** Widest decimal the spill can hold (decimal128). */
    public static final int MAX_DECIMAL_PRECISION = 38;
    /** Connection option with the scale used for a numeric column that declares none. */
    public static final String DEFAULT_SCALE_KEY = "default_scale";
    /** Exported timestamps are plain epoch milliseconds in UTC. */
    public static final ArrowType TIMESTAMP_UTC = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");

    private static final int DECIMAL_BIT_WIDTH = 128;

    private JdbcExportTypes()
    {
    }

    /**
     * The Arrow type for a column without a connector-specific mapping:
     * <ul>
     *   <li>a numeric that declares no precision becomes {@code Decimal(38, default_scale)}, as on the normal read
     *       path;</li>
     *   <li>a numeric wider than 38 digits becomes VARCHAR, holding the exact value as text;</li>
     *   <li>DATE becomes DateDay and TIMESTAMP a UTC millisecond timestamp;</li>
     *   <li>a type the spill cannot write (TIME, INTERVAL, arrays, structs, vendor types) becomes VARCHAR.</li>
     * </ul>
     */
    public static ArrowType defaultArrowType(ResultSetMetaData metadata, int column, Map<String, String> configOptions)
            throws SQLException
    {
        int jdbcType = metadata.getColumnType(column);
        if (jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL) {
            int precision = metadata.getPrecision(column);
            int scale = metadata.getScale(column);
            if (precision <= 0) {
                return decimal(MAX_DECIMAL_PRECISION, defaultScale(configOptions));
            }
            // A negative scale rounds to the left of the point: NUMBER(3,-2) holds 12300, which needs 5 digits.
            int digits = scale < 0 ? precision - scale : precision;
            if (digits > MAX_DECIMAL_PRECISION) {
                return ArrowType.Utf8.INSTANCE;
            }
            return decimal(digits, Math.max(scale, 0));
        }
        switch (jdbcType) {
            case Types.FLOAT:
                // JDBC FLOAT is double precision; the Arrow adapter maps it to single precision.
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case Types.DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return TIMESTAMP_UTC;
            default:
                break;
        }
        ArrowType type;
        try {
            type = JdbcToArrowUtils.getArrowTypeFromJdbcType(new JdbcFieldInfo(metadata, column),
                    Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        }
        catch (UnsupportedOperationException e) {
            return ArrowType.Utf8.INSTANCE;
        }
        if (type == null || !SupportedTypes.isSupported(type) || type instanceof ArrowType.List
                || type instanceof ArrowType.Struct || type instanceof ArrowType.Map) {
            return ArrowType.Utf8.INSTANCE;
        }
        return type;
    }

    /** A {@code Decimal(precision, scale)} that the spill can write. */
    public static ArrowType decimal(int precision, int scale)
    {
        return new ArrowType.Decimal(precision, scale, DECIMAL_BIT_WIDTH);
    }

    /**
     * The scale for a numeric column that declares none (precision 0), from the {@code default_scale} option.
     * Defaults to 18 rather than 0: an unconstrained numeric reports no scale, and a 0 default would silently
     * drop every fractional digit (e.g. {@code 3.14159 -> 3}, {@code 0.001 -> 0}). 18 preserves them and matches
     * Spark's {@code DecimalType} default and the Databricks connector's {@code DECIMAL(38,18)} fallback. A
     * connection can still override it via {@code default_scale}.
     */
    public static int defaultScale(Map<String, String> configOptions)
    {
        return Integer.parseInt(configOptions.getOrDefault(DEFAULT_SCALE_KEY, "18"));
    }

    /**
     * Reads column {@code column} of the current row with the getter that matches {@code type}. Text columns use
     * {@code getString}, so CLOBs, JSON, intervals and other vendor types arrive as their text form rather than as
     * {@code toString()} of a driver object; binary columns use {@code getBytes}, or the LOB's bytes for a BLOB.
     */
    public static Object readValue(ResultSet resultSet, int column, ArrowType type) throws SQLException
    {
        switch (type.getTypeID()) {
            case Utf8:
                return resultSet.getString(column);
            case Binary:
                return readBytes(resultSet, column);
            case Decimal:
                return resultSet.getBigDecimal(column);
            case Bool: {
                boolean value = resultSet.getBoolean(column);
                return resultSet.wasNull() ? null : value;
            }
            case Date:
                return resultSet.getDate(column);
            case Timestamp:
                return resultSet.getTimestamp(column);
            default:
                return resultSet.getObject(column);
        }
    }

    /**
     * Reads a timestamp-with-time-zone column as an instant. {@code BlockUtils.setValue} accepts a
     * {@code ZonedDateTime}, which keeps the instant exact regardless of the JVM or session time zone.
     */
    public static Object readOffsetTimestamp(ResultSet resultSet, int column) throws SQLException
    {
        OffsetDateTime value = resultSet.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toZonedDateTime();
    }

    private static byte[] readBytes(ResultSet resultSet, int column) throws SQLException
    {
        Object value = resultSet.getObject(column);
        if (value == null || value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof Blob) {
            Blob blob = (Blob) value;
            try {
                long length = blob.length();
                if (length > Integer.MAX_VALUE) {
                    throw new SQLException("BLOB column " + column + " is larger than 2 GB");
                }
                return blob.getBytes(1, (int) length);
            }
            finally {
                blob.free();
            }
        }
        return resultSet.getBytes(column);
    }
}
