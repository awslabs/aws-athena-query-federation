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
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InfluxDBTypeMapperTest
{
    private static Types.MinorType athenaMinorType(final String dataType)
    {
        return Types.getMinorTypeForArrowType(InfluxDBTypeMapper.toAthenaType(dataType));
    }

    private static Types.MinorType athenaMinorType(final ArrowType arrowType)
    {
        return Types.getMinorTypeForArrowType(InfluxDBTypeMapper.toAthenaType(arrowType));
    }

    @Test
    public void testDataTypeStringsMapToAthenaTypes()
    {
        assertEquals(InfluxDBTypeMapper.ATHENA_TIMESTAMP, InfluxDBTypeMapper.toAthenaType("Timestamp(Nanosecond, None)"));
        assertEquals(InfluxDBTypeMapper.ATHENA_TIMESTAMP, InfluxDBTypeMapper.toAthenaType("Timestamp(ns)"));
        assertEquals(Types.MinorType.VARCHAR, athenaMinorType("Dictionary(Int32, Utf8)"));
        assertEquals(Types.MinorType.BIGINT, athenaMinorType("Int64"));
        assertEquals(Types.MinorType.BIGINT, athenaMinorType("UInt64"));
        assertEquals(Types.MinorType.BIGINT, athenaMinorType("Int32"));
        assertEquals(Types.MinorType.FLOAT8, athenaMinorType("Float64"));
        assertEquals(Types.MinorType.FLOAT8, athenaMinorType("Float32"));
        assertEquals(Types.MinorType.BIT, athenaMinorType("Boolean"));
        assertEquals(Types.MinorType.VARCHAR, athenaMinorType("Utf8"));
        assertEquals(Types.MinorType.DATEDAY, athenaMinorType("Date32"));
        assertEquals(Types.MinorType.DATEMILLI, athenaMinorType("Date64"));
        // SQL type names are accepted too.
        assertEquals(Types.MinorType.BIGINT, athenaMinorType("BIGINT"));
        assertEquals(Types.MinorType.FLOAT8, athenaMinorType("DOUBLE"));
        assertEquals(Types.MinorType.BIT, athenaMinorType("BOOLEAN"));
        assertEquals(InfluxDBTypeMapper.ATHENA_TIMESTAMP, InfluxDBTypeMapper.toAthenaType("TIMESTAMP"));
        assertEquals(Types.MinorType.VARCHAR, athenaMinorType("Decimal128(10, 2)"));
        assertEquals(Types.MinorType.VARCHAR, athenaMinorType("UNKNOWN_TYPE"));
    }

    @Test
    public void testArrowTypesMapToAthenaTypes()
    {
        for (final TimeUnit unit : TimeUnit.values()) {
            assertEquals(InfluxDBTypeMapper.ATHENA_TIMESTAMP,
                    InfluxDBTypeMapper.toAthenaType(new ArrowType.Timestamp(unit, null)));
            assertEquals(InfluxDBTypeMapper.ATHENA_TIMESTAMP,
                    InfluxDBTypeMapper.toAthenaType(new ArrowType.Timestamp(unit, "UTC")));
        }
        for (final int bitWidth : new int[] {8, 16, 32, 64}) {
            assertEquals(Types.MinorType.BIGINT, athenaMinorType(new ArrowType.Int(bitWidth, true)));
            assertEquals(Types.MinorType.BIGINT, athenaMinorType(new ArrowType.Int(bitWidth, false)));
        }
        assertEquals(Types.MinorType.FLOAT8, athenaMinorType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        assertEquals(Types.MinorType.FLOAT8, athenaMinorType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        assertEquals(Types.MinorType.BIT, athenaMinorType(ArrowType.Bool.INSTANCE));
        assertEquals(Types.MinorType.DATEDAY, athenaMinorType(new ArrowType.Date(DateUnit.DAY)));
        assertEquals(Types.MinorType.DATEMILLI, athenaMinorType(new ArrowType.Date(DateUnit.MILLISECOND)));
        assertEquals(Types.MinorType.VARCHAR, athenaMinorType(ArrowType.Utf8.INSTANCE));
        assertEquals(Types.MinorType.VARCHAR, athenaMinorType(new ArrowType.Decimal(10, 2, 128)));
    }

    @Test
    public void testDictionaryEncodedFieldMapsToVarchar()
    {
        // A tag column: dictionary-encoded strings with Int32 indices.
        final Field tag = new Field("host",
                new FieldType(true, ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(0L, false, new ArrowType.Int(32, true))),
                null);
        assertEquals(Types.MinorType.VARCHAR,
                Types.getMinorTypeForArrowType(InfluxDBTypeMapper.toAthenaType(tag)));
    }

    /**
     * The table schema (from information_schema data_type strings) and the passthrough schema (from the
     * Flight result's Arrow types) must agree for every InfluxDB type.
     */
    @Test
    public void testTableAndPassthroughSchemasAgree()
    {
        final Map<String, ArrowType> dataTypes = new LinkedHashMap<>();
        dataTypes.put("Timestamp(Nanosecond, None)", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
        dataTypes.put("Int64", new ArrowType.Int(64, true));
        dataTypes.put("UInt64", new ArrowType.Int(64, false));
        dataTypes.put("Int32", new ArrowType.Int(32, true));
        dataTypes.put("Float64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        dataTypes.put("Float32", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        dataTypes.put("Boolean", ArrowType.Bool.INSTANCE);
        dataTypes.put("Utf8", ArrowType.Utf8.INSTANCE);
        dataTypes.put("Date32", new ArrowType.Date(DateUnit.DAY));
        dataTypes.put("Date64", new ArrowType.Date(DateUnit.MILLISECOND));
        for (final Map.Entry<String, ArrowType> entry : dataTypes.entrySet()) {
            assertEquals(entry.getKey(),
                    InfluxDBTypeMapper.toAthenaType(entry.getKey()),
                    InfluxDBTypeMapper.toAthenaType(Field.nullable("c", entry.getValue())));
        }
    }
}
