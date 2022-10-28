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
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ArrowTypeConverterTest {

    @Test
    public void testtoArrowType(){
        Assert.assertEquals(new ArrowType.Null(),ArrowTypeConverter.toArrowType("NULL"));
        Assert.assertEquals(new ArrowType.Int(8,true),ArrowTypeConverter.toArrowType("TINYINT"));
        Assert.assertEquals(new ArrowType.Int(16,true),ArrowTypeConverter.toArrowType("SMALLINT"));
        Assert.assertEquals(new ArrowType.Int(32,true),ArrowTypeConverter.toArrowType("INTEGER"));
        Assert.assertEquals(new ArrowType.Int(64,true),ArrowTypeConverter.toArrowType("BIGINT"));
        Assert.assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),ArrowTypeConverter.toArrowType("FLOAT"));
        Assert.assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),ArrowTypeConverter.toArrowType("DOUBLE"));
        Assert.assertEquals(new ArrowType.Utf8(),ArrowTypeConverter.toArrowType("VARCHAR"));
        Assert.assertEquals(new ArrowType.Binary(),ArrowTypeConverter.toArrowType("BINARY VARYING"));
        Assert.assertEquals(new ArrowType.Bool(),ArrowTypeConverter.toArrowType("BOOLEAN"));
        Assert.assertEquals(new ArrowType.Decimal(5, 5, 128),ArrowTypeConverter.toArrowType("DECIMAL"));
        Assert.assertEquals(new ArrowType.Date(DateUnit.DAY),ArrowTypeConverter.toArrowType("DATE"));
        Assert.assertEquals(new ArrowType.Time(TimeUnit.MICROSECOND, 64),ArrowTypeConverter.toArrowType("TIME"));
        Assert.assertEquals(new ArrowType.Date(DateUnit.MILLISECOND),ArrowTypeConverter.toArrowType("TIMESTAMP"));
        Assert.assertEquals(new ArrowType.Interval(IntervalUnit.DAY_TIME),ArrowTypeConverter.toArrowType("INTERVAL DAY TO SECOND"));
        Assert.assertEquals(new ArrowType.Interval(IntervalUnit.YEAR_MONTH),ArrowTypeConverter.toArrowType("INTERVAL YEAR TO MONTH"));
        Assert.assertEquals(new ArrowType.FixedSizeBinary(50),ArrowTypeConverter.toArrowType("BINARY"));
        Assert.assertEquals(new ArrowType.Utf8(),ArrowTypeConverter.toArrowType("Map"));
        Assert.assertEquals(new ArrowType.Utf8(),ArrowTypeConverter.toArrowType("ARRAY"));
    }
    @Test
    public void testconvertToDate() throws ParseException {
        String date1="2022-09-28T12:11:20.123";
        String date2="2022-09-26";
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        Assert.assertEquals(LocalDate.parse(date2, dateFormatter),ArrowTypeConverter.convertToDate(date2));
        Assert.assertEquals(LocalDateTime.parse(date1, timeFormatter),ArrowTypeConverter.convertToDate(date1));
    }
}
