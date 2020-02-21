/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.data;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class DateTimeFormatterUtilTest {
    private static final Logger logger = LoggerFactory.getLogger(DateTimeFormatterUtilTest.class);

    private static final ZoneId DEFAULT_TIME_ZONE = ZoneId.of("UTC");

    @Rule
    public TestName testName = new TestName();


    @Before
    public void setUp() {
        logger.info("====================== Starting Test {} ======================", testName.getMethodName());
        Locale.setDefault(new Locale("en", "UK"));
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @After
    public void tearDown()
    {
        logger.info("====================== Finishing Test {} ======================", testName.getMethodName());
    }

    @Test
    public void stringToLocalDateTest() {
        LocalDate expected = LocalDate.of(2020, 02, 27);

        LocalDate actual = DateTimeFormatterUtil.stringToLocalDate("27022020", "ddMMyyyy", DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);

        actual = DateTimeFormatterUtil.stringToLocalDate("2020-02-27", null, DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);
    }

    @Test(expected=IllegalArgumentException.class)
    public void stringToLocalDateTestCustomerConfiguredFormatFail() {
        DateTimeFormatterUtil.stringToLocalDate("27--02-2020", "ddMMyyyy", DEFAULT_TIME_ZONE);
    }

    @Test(expected=IllegalArgumentException.class)
    public void stringToLocalDateTestFail() {
        DateTimeFormatterUtil.stringToLocalDate("27--02-2020", null, DEFAULT_TIME_ZONE);
    }

    @Test
    public void stringToLocalDateTimeTest() {
        LocalDateTime expected = LocalDateTime.of(2020, 2, 27, 0, 2, 27);

        LocalDateTime actual = DateTimeFormatterUtil.stringToLocalDateTime("00:02:27S2020-02-27", "HH:mm:ss'S'yyyy-MM-dd", DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);

        actual = DateTimeFormatterUtil.stringToLocalDateTime("2020-02-27T00:02:27", null, DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);

        actual = DateTimeFormatterUtil.stringToLocalDateTime("2020-02-27T00:02:27Z", null, DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);

        actual = DateTimeFormatterUtil.stringToLocalDateTime("2020-02-27T00:02:27-05:00", null, DEFAULT_TIME_ZONE);
        assertEquals(expected.plusHours(5), actual);

        actual = DateTimeFormatterUtil.stringToLocalDateTime("20200227T000227", null, DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);

        actual = DateTimeFormatterUtil.stringToLocalDateTime("20200227T000227Z", null, DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);
    }

    @Test(expected=IllegalArgumentException.class)
    public void stringToLocalDateTimeTestCustomerConfiguredFormatFail() {
        DateTimeFormatterUtil.stringToLocalDate("00:02:27S2020---02-27", "HH:mm:ss'S'yyyy-MM-dd", DEFAULT_TIME_ZONE);
    }

    @Test(expected=IllegalArgumentException.class)
    public void stringToLocalDateTimeTestFail() {
        DateTimeFormatterUtil.stringToLocalDate("00:02:27S2020---02-27", null, DEFAULT_TIME_ZONE);
    }

    @Test
    public void bigDecimalToLocalDateTest() {
        LocalDate expected = LocalDate.of(2020, 02, 27);
        Instant instant = expected.atTime(LocalTime.MIDNIGHT).atZone(DEFAULT_TIME_ZONE).toInstant();

        LocalDate actual = DateTimeFormatterUtil.bigDecimalToLocalDate(new BigDecimal(instant.toEpochMilli()), DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);

    }

    @Test
    public void bigDecimalToLocalDateTimeTest() {
        LocalDateTime expected = LocalDateTime.of(2020, 2, 27, 0, 2, 27);
        Instant instant = expected.atZone(DEFAULT_TIME_ZONE).toInstant();

        LocalDateTime actual = DateTimeFormatterUtil.bigDecimalToLocalDateTime(new BigDecimal(instant.toEpochMilli()), DEFAULT_TIME_ZONE);
        assertEquals(expected, actual);
    }
}
