/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.Types;
import org.stringtemplate.v4.ST;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class JdbcSqlUtils
{
    private JdbcSqlUtils() {}

    /**
     * Quotes an identifier (table name, column name, schema name, etc.) using the specified quote character.
     * Escapes any existing quote characters within the identifier by doubling them.
     *
     * @param identifier The identifier to quote (e.g., column name, table name).
     * @param quoteChar The quote character to use (e.g., "`" for MySQL, "\"" for PostgreSQL).
     * @return The quoted identifier, or null if the input identifier is null.
     */
    public static String quoteIdentifier(String identifier, String quoteChar)
    {
        if (identifier == null) {
            return null;
        }
        String escapedName = identifier.replace(quoteChar, quoteChar + quoteChar);
        return quoteChar + escapedName + quoteChar;
    }

    /**
     * Generic method to render any string template with parameters.
     *
     * @param queryFactory The factory to get the template from.
     * @param templateName The name of the template to render.
     * @param params Map of parameter key-value pairs.
     * @return The rendered template string.
     */
    public static String renderTemplate(JdbcQueryFactory queryFactory, String templateName, Map<String, Object> params)
    {
        ST template = queryFactory.getQueryTemplate(templateName);
        if (template == null) {
            throw new RuntimeException("Template not found: " + templateName);
        }

        // Add all parameters from the map
        params.forEach(template::add);

        return template.render().trim();
    }

    /**
     * Sets parameters on a PreparedStatement from a list of TypeAndValue objects.
     * This is a utility method extracted from JdbcSplitQueryBuilder to allow
     * connectors using StringTemplate to set parameters without needing a QueryStringBuilder instance.
     *
     * @param statement The PreparedStatement to set parameters on.
     * @param parameterValues List of TypeAndValue objects containing parameter values.
     * @throws SQLException if a database access error occurs.
     */
    public static void setParameters(PreparedStatement statement, List<TypeAndValue> parameterValues) throws SQLException
    {
        for (int i = 0; i < parameterValues.size(); i++) {
            TypeAndValue typeAndValue = parameterValues.get(i);

            Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(typeAndValue.getType());

            switch (minorTypeForArrowType) {
                case BIGINT:
                    statement.setLong(i + 1, (long) typeAndValue.getValue());
                    break;
                case INT:
                    statement.setInt(i + 1, ((Number) typeAndValue.getValue()).intValue());
                    break;
                case SMALLINT:
                    statement.setShort(i + 1, ((Number) typeAndValue.getValue()).shortValue());
                    break;
                case TINYINT:
                    statement.setByte(i + 1, ((Number) typeAndValue.getValue()).byteValue());
                    break;
                case FLOAT8:
                    statement.setDouble(i + 1, (double) typeAndValue.getValue());
                    break;
                case FLOAT4:
                    statement.setFloat(i + 1, (float) typeAndValue.getValue());
                    break;
                case BIT:
                    statement.setBoolean(i + 1, (boolean) typeAndValue.getValue());
                    break;
                case DATEDAY:
                    //we received value in "UTC" time with DAYS only, appended it to timeMilli in UTC
                    long utcMillis = TimeUnit.DAYS.toMillis(((Number) typeAndValue.getValue()).longValue());
                    //Get the default timezone offset and offset it.
                    //This is because sql.Date will parse millis into localtime zone
                    //ex system timezone in GMT-5, sql.Date will think the utcMillis is in GMT-5, we need to add offset(eg. -18000000) .
                    //ex system timezone in GMT+9, sql.Date will think the utcMillis is in GMT+9, we need to remove offset(eg. 32400000).
                    TimeZone aDefault = TimeZone.getDefault();
                    int offset = aDefault.getOffset(utcMillis);
                    utcMillis -= offset;

                    statement.setDate(i + 1, new Date(utcMillis));
                    break;
                case DATEMILLI:
                    LocalDateTime timestamp = ((LocalDateTime) typeAndValue.getValue());
                    statement.setTimestamp(i + 1, new Timestamp(timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()));
                    break;
                case VARCHAR:
                    statement.setString(i + 1, String.valueOf(typeAndValue.getValue()));
                    break;
                case VARBINARY:
                    statement.setBytes(i + 1, (byte[]) typeAndValue.getValue());
                    break;
                case DECIMAL:
                    statement.setBigDecimal(i + 1, (BigDecimal) typeAndValue.getValue());
                    break;
                default:
                    throw new AthenaConnectorException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), minorTypeForArrowType),
                            ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
            }
        }
    }
}
