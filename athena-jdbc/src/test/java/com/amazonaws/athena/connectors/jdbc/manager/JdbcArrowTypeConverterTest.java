/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class JdbcArrowTypeConverterTest
{

    @Test
    public void toArrowType()
    {
        Assert.assertEquals(Types.MinorType.BIT.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.BIT, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.BIT.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.BOOLEAN, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.TINYINT.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.TINYINT, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.SMALLINT.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.SMALLINT, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.INT.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.INTEGER, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.BIGINT.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.BIGINT, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.FLOAT4.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.REAL, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.FLOAT4.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.FLOAT, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.FLOAT8.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.DOUBLE, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(new ArrowType.Decimal(5, 3), JdbcArrowTypeConverter.toArrowType(java.sql.Types.DECIMAL, 5, 3, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(new ArrowType.Decimal(38, 0), JdbcArrowTypeConverter.toArrowType(java.sql.Types.NUMERIC, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARCHAR.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.CHAR, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARCHAR.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.NCHAR, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARCHAR.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.VARCHAR, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARCHAR.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.NVARCHAR, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARCHAR.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.LONGVARCHAR, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARCHAR.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.LONGNVARCHAR, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARBINARY.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.BINARY, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARBINARY.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.VARBINARY, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.VARBINARY.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.LONGVARBINARY, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.DATEDAY.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.DATE, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.TIMEMILLI.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.TIME, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.DATEMILLI.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.TIMESTAMP, 0, 0, com.google.common.collect.ImmutableMap.of()).get());
        Assert.assertEquals(Types.MinorType.LIST.getType(), JdbcArrowTypeConverter.toArrowType(java.sql.Types.ARRAY, 0, 0, com.google.common.collect.ImmutableMap.of()).get());

        //As of 18.1.0 Arrow does not support TIMESTAMP_WITH_TIMEZONE! Hence the arrow type returns an empty optional
        Assert.assertTrue(JdbcArrowTypeConverter.toArrowType(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, 0, 0, com.google.common.collect.ImmutableMap.of()).isEmpty());
        //Test if precision is more than the default
        Assert.assertEquals(new ArrowType.Decimal(JdbcArrowTypeConverter.DEFAULT_PRECISION , 3), JdbcArrowTypeConverter.toArrowType(java.sql.Types.DECIMAL, JdbcArrowTypeConverter.DEFAULT_PRECISION + 1, 3, com.google.common.collect.ImmutableMap.of()).get());
        //test for negative scale
        Assert.assertEquals(new ArrowType.Decimal(JdbcArrowTypeConverter.DEFAULT_PRECISION, 0), JdbcArrowTypeConverter.toArrowType(java.sql.Types.NUMERIC, 0, -1, com.google.common.collect.ImmutableMap.of()).get());
    }
}
