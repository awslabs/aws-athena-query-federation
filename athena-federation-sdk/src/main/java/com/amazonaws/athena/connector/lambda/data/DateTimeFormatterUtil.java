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

import com.amazonaws.util.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 *
 */
public class DateTimeFormatterUtil
{
    private static final Logger logger = LoggerFactory.getLogger(DateTimeFormatterUtil.class);

    private static final String[] SUPPORTED_DATETIME_FORMAT = {
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern(), //"yyyy-MM-dd"
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern(), //"yyyy-MM-dd'T'HH:mm:ss"
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.getPattern(), //"yyyy-MM-dd'T'HH:mm:ssZZ"
            "yyyyMMdd'T'HHmmss",
            "yyyyMMdd'T'HHmmssZZ"
    };

    private DateTimeFormatterUtil()
    {}

    /**
     * Transforms the raw string to LocalDate using the provided default format
     *
     * @param value raw value to be transformed to LocalDate
     * @param dateFormat customer specified or inferred dateformat
     * @param defaultTimeZone default timezone to be applied
     * @return LocalDate object parsed from value
     */
    public static LocalDate stringToLocalDate(String value, String dateFormat,
                                              ZoneId defaultTimeZone)
    {
        if (StringUtils.isNullOrEmpty(dateFormat)) {
            logger.info("Unable to parse {} as Date type due to invalid dateformat", value);
            return null;
        }
        Date dateTransformedValue = stringToDate(value, dateFormat);
        if (dateTransformedValue == null) {
            return null;
        }
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDate();
    }

    /**
     * Transforms the raw string to LocalDateTime using the provided default format
     *
     * @param value raw value to be transformed to LocalDateTime
     * @param dateFormat customer specified or inferred dateformat
     * @param defaultTimeZone default timezone to be applied
     * @return LocalDateTime object parsed from value
     */
    public static LocalDateTime stringToLocalDateTime(String value, String dateFormat,
                                                      ZoneId defaultTimeZone)
    {
        if (StringUtils.isNullOrEmpty(dateFormat)) {
            logger.info("Unable to parse {} as DateTime type due to invalid dateformat", value);
            return null;
        }
        Date dateTransformedValue = stringToDate(value, dateFormat);
        if (dateTransformedValue == null) {
            return null;
        }
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDateTime();
    }

    /**
     * Transforms big decimal value to LocalDate using the provided default format
     *
     * @param value raw value to be transformed to LocalDate
     * @param defaultTimeZone default timezone to be applied
     * @return LocalDate object parsed from value
     */
    public static LocalDate bigDecimalToLocalDate(BigDecimal value, ZoneId defaultTimeZone)
    {
        Date dateTransformedValue = bigDecimalToDate(value);
        if (dateTransformedValue == null) {
            return null;
        }
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDate();
    }

    /**
     * Transforms big decimal value to LocalDateTime using the provided default format
     *
     * @param value raw value to be transformed to LocalDateTime
     * @param defaultTimeZone default timezone to be applied
     * @return LocalDateTime object parsed from value
     */
    public static LocalDateTime bigDecimalToLocalDateTime(BigDecimal value, ZoneId defaultTimeZone)
    {
        Date dateTransformedValue = bigDecimalToDate(value);
        if (dateTransformedValue == null) {
            return null;
        }
        return dateTransformedValue.toInstant()
                .atZone(defaultTimeZone)
                .toLocalDateTime();
    }

    /**
     * Infers the date format to be used for the values in that column by passing through an array of supported formats
     * this is then cached in DDDBRecordMetadata and reused so that the values in the same column
     * does not have to be re-evaluated/parsed
     *
     * @param value string value to infer a date time format from
     * @return string representing the datetime format
     *         null if it does not match any of the supported formats
     */
    public static String inferDateTimeFormat(String value)
    {
        for (String datetimeFormat : SUPPORTED_DATETIME_FORMAT) {
            try {
                DateUtils.parseDate(value, datetimeFormat);
                logger.info("Inferred format {} for value {}", datetimeFormat, value);
                return datetimeFormat;
            }
            catch (ParseException ex) {
                continue;
            }
        }
        logger.error("Failed to infer format for {}", value);
        return null;
    }

    /**
     * Parses the given raw string to the provided date format
     * @param value raw string value to be parsed to Date object
     * @param dateFormat date format to be used to parse
     * @return date object that is parsed from raw value
     */
    private static Date stringToDate(String value, String dateFormat)
    {
        try {
            return DateUtils.parseDate(value, dateFormat);
        }
        catch (ParseException | IllegalArgumentException ex) {
            logger.error("Encountered value {} that is not consistent with inferred date/datetime format {}" +
                            " - will skip in result",
                    value, dateFormat);
        }
        return null;
    }

    /**
     * trnasforms big decimal value (epoch milli) to date
     * @param value big decimal value containing epoch value
     * @return date object derived from value
     */
    private static Date bigDecimalToDate(BigDecimal value)
    {
        return new Date(value.longValue());
    }
}
