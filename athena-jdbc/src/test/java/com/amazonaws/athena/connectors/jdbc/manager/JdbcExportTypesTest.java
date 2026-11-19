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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcExportTypesTest
{
    private static ResultSetMetaData column(int jdbcType, int precision, int scale) throws SQLException
    {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(jdbcType);
        when(metadata.getPrecision(1)).thenReturn(precision);
        when(metadata.getScale(1)).thenReturn(scale);
        when(metadata.getColumnLabel(1)).thenReturn("c");
        when(metadata.getColumnName(1)).thenReturn("c");
        return metadata;
    }

    private static ArrowType map(int jdbcType, int precision, int scale, Map<String, String> options) throws SQLException
    {
        return JdbcExportTypes.defaultArrowType(column(jdbcType, precision, scale), 1, options);
    }

    private static ArrowType map(int jdbcType, int precision, int scale) throws SQLException
    {
        return map(jdbcType, precision, scale, Map.of());
    }

    @Test
    public void testDeclaredDecimalKeepsPrecisionAndScale() throws SQLException
    {
        assertEquals(JdbcExportTypes.decimal(10, 2), map(Types.DECIMAL, 10, 2));
        assertEquals(JdbcExportTypes.decimal(38, 10), map(Types.NUMERIC, 38, 10));
    }

    @Test
    public void testUnboundedNumericUsesDefaultScale() throws SQLException
    {
        assertEquals(JdbcExportTypes.decimal(38, 18), map(Types.NUMERIC, 0, 0));
        assertEquals(JdbcExportTypes.decimal(38, 4), map(Types.NUMERIC, 0, -127, Map.of("default_scale", "4")));
    }

    @Test
    public void testNegativeScaleWidensPrecision() throws SQLException
    {
        // NUMBER(3,-2) holds values up to 99900.
        assertEquals(JdbcExportTypes.decimal(5, 0), map(Types.NUMERIC, 3, -2));
    }

    @Test
    public void testDecimalWiderThan38DigitsIsText() throws SQLException
    {
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.NUMERIC, 40, 2));
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.NUMERIC, 37, -5));
    }

    @Test
    public void testTemporalTypes() throws SQLException
    {
        assertEquals(new ArrowType.Date(DateUnit.DAY), map(Types.DATE, 10, 0));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, map(Types.TIMESTAMP, 26, 6));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, map(Types.TIMESTAMP_WITH_TIMEZONE, 35, 6));
    }

    @Test
    public void testFloatIsDoublePrecision() throws SQLException
    {
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), map(Types.FLOAT, 53, 0));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), map(Types.DOUBLE, 17, 0));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), map(Types.REAL, 8, 0));
    }

    @Test
    public void testIntegersAndText() throws SQLException
    {
        assertEquals(new ArrowType.Int(32, true), map(Types.INTEGER, 10, 0));
        assertEquals(new ArrowType.Int(64, true), map(Types.BIGINT, 19, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.VARCHAR, 255, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.CLOB, 0, 0));
        assertEquals(ArrowType.Binary.INSTANCE, map(Types.VARBINARY, 16, 0));
    }

    @Test
    public void testTypesTheSpillCannotWriteAreText() throws SQLException
    {
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.TIME, 8, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.ARRAY, 0, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.STRUCT, 0, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.OTHER, 0, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, map(Types.SQLXML, 0, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, map(-101, 0, 0)); // a vendor type code
    }

    @Test
    public void testReadValueUsesTheGetterForTheType() throws SQLException
    {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString(1)).thenReturn("text");
        when(resultSet.getBigDecimal(2)).thenReturn(new BigDecimal("1.50"));
        when(resultSet.getBoolean(3)).thenReturn(false);
        when(resultSet.wasNull()).thenReturn(true);

        assertEquals("text", JdbcExportTypes.readValue(resultSet, 1, ArrowType.Utf8.INSTANCE));
        assertEquals(new BigDecimal("1.50"), JdbcExportTypes.readValue(resultSet, 2, JdbcExportTypes.decimal(10, 2)));
        assertNull(JdbcExportTypes.readValue(resultSet, 3, ArrowType.Bool.INSTANCE));
    }

    @Test
    public void testReadValueReadsBlobBytesAndFreesTheLob() throws SQLException
    {
        Blob blob = mock(Blob.class);
        when(blob.length()).thenReturn(3L);
        when(blob.getBytes(1, 3)).thenReturn(new byte[] {1, 2, 3});
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(blob);

        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) JdbcExportTypes.readValue(resultSet, 1, ArrowType.Binary.INSTANCE));
        verify(blob).free();
    }

    @Test
    public void testReadOffsetTimestampKeepsTheInstant() throws SQLException
    {
        OffsetDateTime value = OffsetDateTime.of(2026, 10, 23, 5, 34, 56, 789_000_000, ZoneOffset.ofHours(-7));
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1, OffsetDateTime.class)).thenReturn(value);

        ZonedDateTime read = (ZonedDateTime) JdbcExportTypes.readOffsetTimestamp(resultSet, 1);

        assertEquals(value.toInstant(), read.toInstant());
        assertNull(JdbcExportTypes.readOffsetTimestamp(mock(ResultSet.class), 1));
    }
}
