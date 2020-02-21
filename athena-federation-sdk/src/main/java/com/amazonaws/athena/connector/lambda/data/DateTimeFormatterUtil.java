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

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class DateTimeFormatterUtil
{
    private static final String[] FALL_BACK_FORMATS = {
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            "yyyyMMdd'T'HHmmss"};

    private DateTimeFormatterUtil()
    {}

    public static LocalDate stringToLocalDate(String value, String customerConfiguredFormat,
                                              ZoneId defaultTimeZone)
    {
        Date dateTransformedValue = stringToDate(value, customerConfiguredFormat);
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDate();
    }

    public static LocalDateTime stringToLocalDateTime(String value, String customerConfiguredFormat,
                                                      ZoneId defaultTimeZone)
    {
        Date dateTransformedValue = stringToDate(value, customerConfiguredFormat);
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDateTime();
    }

    public static LocalDate bigDecimalToLocalDate(BigDecimal value, ZoneId defaultTimeZone)
    {
        Date dateTransformedValue = bigDecimalToDate(value);
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDate();
    }

    public static LocalDateTime bigDecimalToLocalDateTime(BigDecimal value, ZoneId defaultTimeZone)
    {
        Date dateTransformedValue = bigDecimalToDate(value);
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDateTime();
    }

    private static Date stringToDate(String value, String customerConfiguredFormat)
    {
        Date dateTransformedValue;
        try {
            if (customerConfiguredFormat != null) {
                dateTransformedValue = DateUtils.parseDate(value, customerConfiguredFormat);
            }
            else {
                throw new ParseException("Customer Configured Format Parsing Failed, falling back", 0);
            }
        }
        catch (Exception ex) {
            try {
                dateTransformedValue = com.amazonaws.util.DateUtils.parseISO8601Date(value);
            }
            catch (Exception ex1) {
                try {
                    dateTransformedValue = com.amazonaws.util.DateUtils.parseCompressedISO8601Date(value);
                }
                catch (IllegalArgumentException ex2) {
                    try {
                        dateTransformedValue = DateUtils.parseDate(value, FALL_BACK_FORMATS);
                    }
                    catch (ParseException ex3) {
                        throw new IllegalArgumentException(ex3);
                    }
                }
            }
        }
        return dateTransformedValue;
    }

    private static Date bigDecimalToDate(BigDecimal value)
    {
        return new Date(value.longValue());
    }
}
