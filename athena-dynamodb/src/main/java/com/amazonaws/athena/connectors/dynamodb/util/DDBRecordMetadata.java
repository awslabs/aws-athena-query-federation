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

import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.glue.DefaultGlueType;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.COLUMN_NAME_MAPPING_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED;
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
    private Map<String, String> columnNameMapping;
    private boolean containsCoercibleType;
    private Set<String> nonComparableColumns;
    private boolean glueTableContainedPreviouslyUnsupportedTypes;

    public DDBRecordMetadata(Schema schema)
    {
        dateTimeFormatMapping = getDateTimeFormatMapping(schema);
        defaultTimeZone = getDefaultTimeZone(schema);
        columnNameMapping = getColumnNameMapping(schema);
        containsCoercibleType = isContainsCoercibleType(schema);
        nonComparableColumns = getNonComparableColumns(schema);
        glueTableContainedPreviouslyUnsupportedTypes = checkGlueTableContainedPreviouslyUnsupportedTypes(schema);
    }

    /**
     * retrieves the names of columns that have types that are not natively supported by glue/dynamodb
     * and therefore needs to be excluded from the constraining clause in scan/query.
     *
     * @return set of strings containing names of columns with non-native types (such as timestamptz)
     */
    public Set<String> getNonComparableColumns()
    {
        return nonComparableColumns;
    }

    private Set<String> getNonComparableColumns(Schema schema)
    {
        Set<String> nonComparableColumns = new HashSet<>();
        if (schema != null && schema.getFields() != null) {
            for (Field field : schema.getFields()) {
                Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
                if (DefaultGlueType.getNonComparableSet().contains(fieldType.name())) {
                    nonComparableColumns.add(field.getName());
                }
            }
        }
        return nonComparableColumns;
    }

    /**
     * determines whether the schema contains any type that can be coercible
     * @param schema Schema to extract out the info from
     * @return boolean indicating existence of coercible type in schema
     */
    private boolean isContainsCoercibleType(Schema schema)
    {
        if (schema != null && schema.getFields() != null) {
            for (Field field : schema.getFields()) {
                Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
                if (isDateTimeFieldType(fieldType) || !fieldType.equals(Types.MinorType.DECIMAL)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * returns whether the schema contains any type that can be coercible
     * @return boolean indicating existence of coercible type in schema
     */
    public boolean isContainsCoercibleType()
    {
        return containsCoercibleType;
    }

    /**
     * checks if the type is a datetime field type
     * @param fieldType minorType to be checked
     * @return boolean true if its one of the three supported datetime types, otherwise false
     */
    public static boolean isDateTimeFieldType(Types.MinorType fieldType)
    {
        return fieldType.equals(Types.MinorType.DATEMILLI)
                || fieldType.equals(Types.MinorType.DATEDAY)
                || fieldType.equals(Types.MinorType.TIMESTAMPMILLITZ);
    }

    /**
     * Retrieves the map of glue column names to glue/normalized column names from the table schema
     * @param schema Schema to extract out the info from
     * @return mapping of glue column names to ddb column names
     */
    private static Map<String, String> getColumnNameMapping(Schema schema)
    {
        if (schema != null && schema.getCustomMetadata() != null) {
            String columnNameMappingParam = schema.getCustomMetadata().getOrDefault(
                    COLUMN_NAME_MAPPING_PROPERTY, null);
            if (!Strings.isNullOrEmpty(columnNameMappingParam)) {
                return MAP_SPLITTER.split(columnNameMappingParam);
            }
        }
        return ImmutableMap.of();
    }

    public Map<String, String> getColumnNameMapping()
    {
        return columnNameMapping;
    }

    /**
     * Getter function that retrieves the date/datetime formatting for a specific column name
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
        if (schema != null && schema.getCustomMetadata() != null) {
            String datetimeFormatMappingParam = schema.getCustomMetadata().getOrDefault(
                    DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED, null);
            if (!Strings.isNullOrEmpty(datetimeFormatMappingParam)) {
                return new HashMap<>(MAP_SPLITTER.split(datetimeFormatMappingParam));
            }
        }
        return new HashMap<>();
    }

    /**
     * Retrieves default timezone that would apply to date/datetime if not already specified in the record
     *
     * @param schema Schema to extract out the info from
     * @return ZoneId of the default timezone to use, falling back to UTC if not specified
     */
    private ZoneId getDefaultTimeZone(Schema schema)
    {
        return schema != null && schema.getCustomMetadata() != null
                ? ZoneId.of(schema.getCustomMetadata().getOrDefault(DEFAULT_TIME_ZONE, UTC))
                : ZoneId.of(UTC);
    }

    private boolean checkGlueTableContainedPreviouslyUnsupportedTypes(Schema schema)
    {
        if (schema != null && schema.getCustomMetadata() != null) {
            return Boolean.valueOf(schema.getCustomMetadata().getOrDefault(
                    GlueMetadataHandler.GLUE_TABLE_CONTAINS_PREVIOUSLY_UNSUPPORTED_TYPE, "false"));
        }
        return false;
    }

    public boolean getGlueTableContainedPreviouslyUnsupportedTypes()
    {
        return glueTableContainedPreviouslyUnsupportedTypes;
    }
}
