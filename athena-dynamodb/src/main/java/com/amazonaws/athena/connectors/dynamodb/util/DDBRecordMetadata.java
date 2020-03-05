/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.arrow.vector.types.pojo.Schema;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DATETIME_FORMAT_MAPPING_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_TIME_ZONE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.UTC;

/**
 * This is primarily used for passing any table metadata that applies to the records when normalizing the data
 */
public class DDBRecordMetadata
{
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().withKeyValueSeparator("=");

    private Map<String, String> dateTimeFormatMapping;
    private ZoneId defaultTimeZone;

    public DDBRecordMetadata(Schema schema)
    {
        dateTimeFormatMapping = getDateTimeFormatMapping(schema);
        defaultTimeZone = getDefaultTimeZone(schema);
    }

    /**
     * Getter function retrieve the date/datetime formatting for a specific column name
     * @param columnName name of the column
     * @return the string that represents the date/datetime format
     */
    public String getDateTimeFormat(String columnName)
    {
        return dateTimeFormatMapping.getOrDefault(columnName, null);
    }

    /**
     * Setter function to add a inferred datetimeFormat to the datetimeFormatMapping for a specified column
     * @param columnName name of the column that contains data of date/datetime with datetimeFormat
     * @param datetimeFormat inferred format of the date/datetime
     */
    public void setDateTimeFormat(String columnName, String datetimeFormat)
    {
        dateTimeFormatMapping.put(columnName, datetimeFormat);
    }

    /**
     * Getter for default timezone
     * @return default ZoneId to apply to date/datetime that does not have timezone already specified
     */
    public ZoneId getDefaultTimeZone()
    {
        return defaultTimeZone;
    }

    /**
     * Retrieves the map of normalized column names to customized format for any string representation of date/datetime
     * @param schema Schema to extract out the info from
     * @return Map of column names to datetime format
     */
    private Map<String, String> getDateTimeFormatMapping(Schema schema)
    {
        if (schema.getCustomMetadata() != null) {
            String datetimeFormatMappingParam = schema.getCustomMetadata().getOrDefault(DATETIME_FORMAT_MAPPING_PROPERTY, null);
            if (!Strings.isNullOrEmpty(datetimeFormatMappingParam)) {
                return new HashMap<>(MAP_SPLITTER.split(datetimeFormatMappingParam));
            }
        }
        return Collections.emptyMap();
    }

    /**
     * Retrieves default timezone that would apply to date/datetime if not already specified in the record
     *
     * @param schema Schema to extract out the info from
     * @return ZoneId of the default timezone to use, falling back to UTC if not specified
     */
    private ZoneId getDefaultTimeZone(Schema schema)
    {
        return schema.getCustomMetadata() != null
                ? ZoneId.of(schema.getCustomMetadata().getOrDefault(DEFAULT_TIME_ZONE, UTC))
                : ZoneId.of(UTC);
    }
}
