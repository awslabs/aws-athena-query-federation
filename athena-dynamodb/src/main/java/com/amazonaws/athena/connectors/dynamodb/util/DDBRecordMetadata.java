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
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;

import java.time.ZoneId;
import java.util.Map;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DATETIME_FORMAT_MAPPING_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_TIME_ZONE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.UTC;

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

    public String getDateTimeFormat(String fieldName)
    {
        return dateTimeFormatMapping.getOrDefault(fieldName, null);
    }

    public ZoneId getDefaultTimeZone()
    {
        return defaultTimeZone;
    }

    /*
Retrieves the map of normalized column names to customized format for any string representation of date/datetime
 */
    private Map<String, String> getDateTimeFormatMapping(Schema schema)
    {
        if (schema.getCustomMetadata() != null) {
            String datetimeFormatMappingParam = schema.getCustomMetadata().getOrDefault(DATETIME_FORMAT_MAPPING_PROPERTY, null);
            if (!Strings.isNullOrEmpty(datetimeFormatMappingParam)) {
                return MAP_SPLITTER.split(datetimeFormatMappingParam);
            }
        }
        return ImmutableMap.of();
    }

    private ZoneId getDefaultTimeZone(Schema schema)
    {
        return ZoneId.of(schema.getCustomMetadata().getOrDefault(DEFAULT_TIME_ZONE, UTC));
    }
}
