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
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Date;

import static java.util.Objects.requireNonNull;

/**
 * Provides utility methods relating to formatting strings to date/datetime type
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

    private static final ZoneId UTC_ZONE_ID =  ZoneId.of("UTC");

    // These values are used to transform a ZonedDateTime object to long
    // by bit-shifting (MILLIS_SHIFT) UTC equivalent time in millis
    // and &'ing with 12 bit representation of the timezone value in the ZonedDateTime.
    // This encoding is from Presto's DateTimeEncoding.
    private static final int TIME_ZONE_MASK = 0xFFF;
    private static final int MILLIS_SHIFT = 12;

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
     * Tries to parse the string value to ZonedDateTime, If fails, then falls back to the LocalDateTime with default zone
     *
     * @param value raw value to be transformed to LocalDateTime
     * @param dateFormat customer specified or inferred dateformat
     * @param defaultTimeZone default timezone to be applied
     * @return LocalDateTime or ZonedDateTime object parsed from value
     */
    public static Object stringToZonedDateTime(String value, String dateFormat, ZoneId defaultTimeZone)
    {
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(value);
            logger.info("Parsed {} to ZonedDateTime {}", value, zdt.toString());
            return zdt;
        }
        catch (DateTimeParseException e) {
            logger.warn("Unable to parse {} as ZonedDateTime type due to invalid date format, " +
                    "Falling back to LocalDateTime using timezone {}", value, defaultTimeZone);
        }
        return stringToDateTime(value, dateFormat, defaultTimeZone);
    }

    /**
     * Transforms the raw string to LocalDateTime using the provided default format
     *
     * @param value raw value to be transformed to LocalDateTime
     * @param dateFormat customer specified or inferred dateformat
     * @param defaultTimeZone default timezone to be applied
     * @return LocalDateTime or ZonedDateTime object parsed from value
     */
    public static Object stringToDateTime(String value, String dateFormat, ZoneId defaultTimeZone)
    {
        if (StringUtils.isNullOrEmpty(dateFormat)) {
            logger.warn("Unable to parse {} as DateTime type due to invalid date format", value);
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
        logger.warn("Failed to infer format for {}", value);
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
            logger.warn("Encountered value {} that is not consistent with inferred date/datetime format {}" +
                            " - will skip in result", value, dateFormat);
        }
        return null;
    }

    /**
     * transforms big decimal value (epoch milli) to date
     * @param value big decimal value containing epoch value
     * @return date object derived from value
     */
    private static Date bigDecimalToDate(BigDecimal value)
    {
        try {
            return new Date(value.longValue());
        }
        catch (RuntimeException ex) {
            logger.warn("Invalid long value to transform to Date object");
            return null;
        }
    }

    /**
     * Extracts timezone value and time value (in millis in UTCC) from ZonedDateTime
     * and uses Bit Operation* to create a single long value representing ZonedDateTime.
     * This is the the same encoding as Presto's sql datetime type.
     *
     * Bit Operation* : bit-shifting (MILLIS_SHIFT) UTC equivalent time in millis
     * and &'ing with 12 bit representation of the timezone value in the ZonedDateTime.
     *
     * @param zdt ZonedDateTime to extract timezone and time millis in UTC from
     * @return long value representing ZonedDateTime
     */
    public static long packDateTimeWithZone(ZonedDateTime zdt)
    {
        String zoneId = zdt.getZone().getId();
        LocalDateTime zdtInUtc = zdt.withZoneSameInstant(UTC_ZONE_ID).toLocalDateTime();
        return DateTimeFormatterUtil.packDateTimeWithZone(
                zdtInUtc.atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), zoneId);
    }

    /**
     * Uses Bit Operation* to create a single long value representing timestamp with millisUtc and zoneId
     *
     * Bit Operation* : bit-shifting (MILLIS_SHIFT) UTC equivalent time in millis
     * and &'ing with 12 bit representation of the timezone value in the ZonedDateTime.
     *
     * @param millisUtc UTC equivalent time in millis
     * @param zoneId string value of the timezone (ex: UTC)
     * @return long value representing ZonedDateTime
     */
    public static long packDateTimeWithZone(long millisUtc, String zoneId)
    {
        TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(zoneId);
        requireNonNull(timeZoneKey, "timeZoneKey is null");
        return pack(millisUtc, timeZoneKey.getKey());
    }

    /**
     * Bit operation used in packDateTimeWithZone
     * @param millisUtc UTC equivalent time in millis
     * @param timeZoneKey short value of the timezone (ex: UTC)
     * @return long containing both millisUtc and timeZoneKey
     */
    private static long pack(long millisUtc, short timeZoneKey)
    {
        return (millisUtc << MILLIS_SHIFT) | (timeZoneKey & TIME_ZONE_MASK);
    }

    /**
     * Shift the datetime and timezone packed long by MILLIS_SHIFT to extract datetime portion
     * @param dateTimeWithTimeZone datetime and timezone packed long
     * @return extracted datetime from dateTimeWithTimeZone
     */
    private static long unpackMillisUtc(long dateTimeWithTimeZone)
    {
        return dateTimeWithTimeZone >> MILLIS_SHIFT;
    }

    /**
     * Bit operate on the datetime and timezone packed long with & TIME_ZONE_MASK to extract timezone portion
     * @param dateTimeWithTimeZone datetime and timezone packed long
     * @return extracted timezone from dateTimeWithTimeZone
     */
    private static TimeZoneKey unpackZoneKey(long dateTimeWithTimeZone)
    {
        return TimeZoneKey.getTimeZoneKey((short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
    }

    /**
     * Reconstruct ZonedDateTime type from datetime and timezone packed long
     * @param dateTimeWithTimeZone datetime and timezone packed long
     * @return ZonedDateTime representing values from dateTimeWithTimeZone
     */
    public static ZonedDateTime constructZonedDateTime(long dateTimeWithTimeZone)
    {
        TimeZoneKey timeZoneKey = unpackZoneKey(dateTimeWithTimeZone);
        long millisUtc = unpackMillisUtc(dateTimeWithTimeZone);
        LocalDateTime zdtInUtc = new Timestamp(millisUtc).toLocalDateTime();
        ZoneId timeZone = ZoneId.of(timeZoneKey.getId());
        LocalDateTime localDateTime = zdtInUtc.atZone(UTC_ZONE_ID)
                .withZoneSameInstant(timeZone)
                .toLocalDateTime();
        return ZonedDateTime.of(localDateTime, timeZone);
    }
}
