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

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.sql.Types;

/**
 * Utility abstracts Jdbc to Arrow type conversions.
 */
public final class JdbcArrowTypeConverter
{
    private static final int DEFAULT_PRECISION = 38;

    private JdbcArrowTypeConverter() {}

    /**
     * Coverts Jdbc data type to Arrow data type.
     *
     * @param jdbcType Jdbc integer type. See {@link java.sql.Types}.
     * @param precision Decimal precision.
     * @param scale Decimal scale.
     * @return Arrow type. See {@link ArrowType}.
     */
    public static ArrowType toArrowType(final int jdbcType, final int precision, final int scale, java.util.Map<String, String> configOptions)
    {
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

        ArrowType arrowType = JdbcToArrowUtils.getArrowTypeFromJdbcType(
                new JdbcFieldInfo(jdbcType, resolvedPrecision, resolvedScale),
                null);

        if (arrowType instanceof ArrowType.Date) {
            // Convert from DateMilli to DateDay
            return new ArrowType.Date(DateUnit.DAY);
        }
        else if (arrowType instanceof ArrowType.Timestamp) {
            // Convert from Timestamp to DateMilli
            return new ArrowType.Date(DateUnit.MILLISECOND);
        }

        return arrowType;
    }
}
