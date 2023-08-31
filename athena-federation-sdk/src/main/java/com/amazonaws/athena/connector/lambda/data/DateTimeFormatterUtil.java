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
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
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

    // By default this is enabled because this is the historical behavior
    private static boolean packTimezone = true;

    public static void disableTimezonePacking()
    {
        logger.info("Timezone packing disabled");
        packTimezone = false;
    }

    // Enabling is only available in tests.
    // In production, once an application calls disable we don't allow it to be
    // re-enable it during the lifetime of the application since there is no
    // use case for toggling this and this makes reasoning about what happpens
    // when we investigate issues easier.
    @VisibleForTesting
    static void enableTimezonePacking()
    {
        logger.info("Timezone packing enabled");
        packTimezone = true;
    }

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
    private static long packDateTimeWithZone(ZonedDateTime zdt)
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
    private static long packDateTimeWithZone(long millisUtc, String zoneId)
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
     * Reconstruct a ZonedDateTime given an epoch timestamp and its arrow type
     * @param epochTimestamp this is the timestamp in epoch time. Possibly in packed form, where it contains the timezone.
     * @param arrowType this is the arrowType that the value being passed in came from. The arrow type contains information about the units and timezone.
     * @return ZonedDateTime the converted ZonedDateTime from the epochUtc timestamp and timezone either from the packed value or arrowType.
     */
    public static ZonedDateTime constructZonedDateTime(long epochTimestamp, ArrowType.Timestamp arrowType)
    {
        org.apache.arrow.vector.types.TimeUnit timeunit = arrowType.getUnit();
        ZoneId timeZone = ZoneId.of(arrowType.getTimezone());
        if (packTimezone) {
            if (!org.apache.arrow.vector.types.TimeUnit.MILLISECOND.equals(timeunit)) {
                throw new UnsupportedOperationException("Unpacking is only supported for milliseconds");
            }
            // arrowType's timezone is ignored in this case since the timezone is packed into the long
            TimeZoneKey timeZoneKey = unpackZoneKey(epochTimestamp);
            epochTimestamp = unpackMillisUtc(epochTimestamp);
            timeZone = ZoneId.of(timeZoneKey.getId());
        }
        return Instant.EPOCH
            .plus(epochTimestamp, DateTimeFormatterUtil.arrowTimeUnitToChronoUnit(timeunit))
            .atZone(timeZone);
    }

    public static java.time.temporal.ChronoUnit arrowTimeUnitToChronoUnit(org.apache.arrow.vector.types.TimeUnit timeunit)
    {
        switch (timeunit) {
            case MICROSECOND:
                return java.time.temporal.ChronoUnit.MICROS;
            case MILLISECOND:
                return java.time.temporal.ChronoUnit.MILLIS;
            case NANOSECOND:
                return java.time.temporal.ChronoUnit.NANOS;
            case SECOND:
                return java.time.temporal.ChronoUnit.SECONDS;
        }

        throw new UnsupportedOperationException(String.format("Unsupported timeunit: %s", timeunit));
    }

    public static ZonedDateTime zonedDateTimeFromObject(Object value)
    {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        }
        else if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(ZoneId.of("UTC"));
        }
        else if (value instanceof Date) {
            return ((Date) value).toInstant().atZone(ZoneId.of("UTC"));
        }

        throw new UnsupportedOperationException(String.format("Type: %s not supported", value.getClass()));
    }

    public static org.apache.arrow.vector.holders.TimeStampMilliTZHolder timestampMilliTzHolderFromObject(Object value, String targetTimeZoneId)
    {
        ZonedDateTime zdt = zonedDateTimeFromObject(value);
        org.apache.arrow.vector.holders.TimeStampMilliTZHolder holder = new org.apache.arrow.vector.holders.TimeStampMilliTZHolder();
        holder.timezone = zdt.getZone().getId();
        holder.value = zdt.toInstant().toEpochMilli();

        // Pack the timezone if packing is enabled.
        // Note that it doesn't matter if we already also have the timezone
        // set in the holder, since engines that expect packing will not be
        // looking at that field.
        // Also note that we are packing the original timezone for backwards compatibility
        // because this is what Athena expects rather than using the target timezone.
        if (packTimezone) {
            holder.value = packDateTimeWithZone(holder.value, holder.timezone);
        }

        // epoch time is timezone agnostic, so we can set any target timezone without doing any
        // extra conversion.
        if (targetTimeZoneId != null) {
          holder.timezone = targetTimeZoneId;
        }
        return holder;
    }

    public static org.apache.arrow.vector.holders.TimeStampMicroTZHolder timestampMicroTzHolderFromObject(Object value, String targetTimeZoneId)
    {
        if (packTimezone) {
            throw new UnsupportedOperationException("Packing for TimeStampMicroTZ is not currently supported");
        }

        ZonedDateTime zdt = zonedDateTimeFromObject(value);
        // epoch time is timezone agnostic, so we can set any target timezone without doing any
        // extra conversion.
        String zone = targetTimeZoneId != null ? targetTimeZoneId : zdt.getZone().getId();
        Instant instant = zdt.toInstant();

        // From: https://stackoverflow.com/a/47869726
        long epochMicros = java.util.concurrent.TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) +
            instant.getLong(java.time.temporal.ChronoField.MICRO_OF_SECOND);

        org.apache.arrow.vector.holders.TimeStampMicroTZHolder holder = new org.apache.arrow.vector.holders.TimeStampMicroTZHolder();
        holder.timezone = zone;
        holder.value = epochMicros;
        return holder;
    }
}
