/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake.utils;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;

public final class SnowflakeArrowTypeConverter
{
    public static final int DEFAULT_PRECISION = 38;
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcArrowTypeConverter.class);

    private SnowflakeArrowTypeConverter()
    {
    }

    /**
     * Coverts Jdbc data type to Arrow data type.
     *
     * @param jdbcType Jdbc integer type. See {@link java.sql.Types}.
     * @param precision Decimal precision.
     * @param scale Decimal scale.
     * @return Arrow type. See {@link ArrowType}.
     */
    public static Optional<ArrowType> toArrowType(String name, final int jdbcType, final int precision, final int scale, java.util.Map<String, String> configOptions)
    {
        requireNonNull(configOptions, "configOptions is null");
        int defaultScale = Integer.parseInt(configOptions.getOrDefault("default_scale", "0"));
        int resolvedPrecision = precision;
        int resolvedScale = scale;
        boolean needsResolving = jdbcType == Types.NUMERIC && (precision == 0 && scale <= 0);
        boolean decimalExceedingPrecision = jdbcType == Types.DECIMAL && precision > DEFAULT_PRECISION;
        // Resolve Precision and Scale if they're not available
        if (needsResolving) {
            resolvedPrecision = DEFAULT_PRECISION;
            resolvedScale = defaultScale;
        }
        else if (decimalExceedingPrecision) {
            resolvedPrecision = DEFAULT_PRECISION;
        }

        Optional<ArrowType> arrowTypeOptional = Optional.empty();

        try {
            if (jdbcType == Types.BIGINT) {
                //snowflake spec
                arrowTypeOptional = Optional.of(new ArrowType.Decimal(resolvedPrecision, resolvedScale, 128));
            }
            else {
                // Support Snowflake TimeStamp with NTZ type, treat all timestamp as UTC.
                arrowTypeOptional = Optional.of(JdbcToArrowUtils.getArrowTypeFromJdbcType(
                        new JdbcFieldInfo(jdbcType, resolvedPrecision, resolvedScale), Calendar.getInstance(TimeZone.getTimeZone("UTC"))));
            }
        }
        catch (UnsupportedOperationException e) {
            LOGGER.warn("Error converting JDBC Type [{}] to arrow: {}", jdbcType, e.getMessage());
            if (jdbcType == Types.TIMESTAMP_WITH_TIMEZONE) {
                // Convert from TIMESTAMP_WITH_TIMEZONE to DateMilli
                LOGGER.debug("Converting JDBC Type [{}] to arrow: {}", jdbcType, e.getMessage());
                return Optional.of(new ArrowType.Date(DateUnit.MILLISECOND));
            }
            return arrowTypeOptional;
        }

        if (arrowTypeOptional.isPresent() && arrowTypeOptional.get() instanceof ArrowType.Date) {
            // Convert from DateMilli to DateDay
            return Optional.of(new ArrowType.Date(DateUnit.DAY));
        }
        else if (arrowTypeOptional.isPresent() && arrowTypeOptional.get() instanceof ArrowType.Timestamp) {
            // Convert from Timestamp to DateMilli
            return Optional.of(new ArrowType.Date(DateUnit.MILLISECOND));
        }

        return arrowTypeOptional;
    }
}
