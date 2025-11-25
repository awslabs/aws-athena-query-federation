/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeArrowTypeConverter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class SnowflakeArrowTypeConverterTest
{
    @Test
    public void testToArrowTypeInteger()
    {
        Map<String, String> configOptions = new HashMap<>();
        Optional<ArrowType> result = SnowflakeArrowTypeConverter.toArrowType("intCol", Types.INTEGER, 10, 0, configOptions);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ArrowType.Int);
    }

    @Test
    public void testToArrowTypeVarchar()
    {
        Map<String, String> configOptions = new HashMap<>();
        Optional<ArrowType> result = SnowflakeArrowTypeConverter.toArrowType("varcharCol", Types.VARCHAR, 255, 0, configOptions);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ArrowType.Utf8);
    }

    @Test
    public void testToArrowTypeBigInt()
    {
        Map<String, String> configOptions = new HashMap<>();
        int expectedPrecision = 19;
        Optional<ArrowType> result = SnowflakeArrowTypeConverter.toArrowType("bigintCol", Types.BIGINT, 19, 0, configOptions);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ArrowType.Decimal);
        ArrowType.Decimal decimal = (ArrowType.Decimal) result.get();
        assertEquals(expectedPrecision, decimal.getPrecision());
    }

    @Test
    public void testToArrowTypeNumericWithDefaultScale()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("default_scale", "2");
        Optional<ArrowType> result = SnowflakeArrowTypeConverter.toArrowType("numericCol", Types.NUMERIC, 0, 0, configOptions);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ArrowType.Decimal);
        ArrowType.Decimal decimal = (ArrowType.Decimal) result.get();
        assertEquals(38, decimal.getPrecision());
        assertEquals(2, decimal.getScale());
    }

    @Test
    public void testToArrowTypeDecimalExceedingPrecision()
    {
        Map<String, String> configOptions = new HashMap<>();
        Optional<ArrowType> result = SnowflakeArrowTypeConverter.toArrowType("decimalCol", Types.DECIMAL, 50, 10, configOptions);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ArrowType.Decimal);
        ArrowType.Decimal decimal = (ArrowType.Decimal) result.get();
        assertEquals(38, decimal.getPrecision());
    }

    @Test
    public void testToArrowTypeTimestampWithTimezone()
    {
        Map<String, String> configOptions = new HashMap<>();
        Optional<ArrowType> result = SnowflakeArrowTypeConverter.toArrowType("timestampCol", Types.TIMESTAMP_WITH_TIMEZONE, 0, 0, configOptions);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ArrowType.Date);
    }

    @Test
    public void testToArrowTypeArrayType()
    {
        Map<String, String> configOptions = new HashMap<>();
        Optional<ArrowType> result = SnowflakeArrowTypeConverter.toArrowType("arrayCol", Types.ARRAY, 0, 0, configOptions);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof ArrowType.List);
    }

    @Test
    public void testToArrowTypeWithNullConfigOptions()
    {
        try {
            SnowflakeArrowTypeConverter.toArrowType("intCol", Types.INTEGER, 10, 0, null);
            fail("null config map should failed");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("configOptions is null"));
        }
    }
}