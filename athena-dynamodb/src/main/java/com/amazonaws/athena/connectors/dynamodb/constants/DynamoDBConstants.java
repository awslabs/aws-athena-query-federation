/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.constants;

public final class DynamoDBConstants
{
    private DynamoDBConstants() {}

    public static final String DEFAULT_SCHEMA = "default";
    public static final String PARTITION_TYPE_METADATA = "partitionType";
    public static final String QUERY_PARTITION_TYPE = "query";
    public static final String SCAN_PARTITION_TYPE = "scan";
    public static final String SEGMENT_COUNT_METADATA = "segmentCount";
    public static final String SEGMENT_ID_PROPERTY = "segmentId";
    public static final String TABLE_METADATA = "sourceTable";
    public static final String INDEX_METADATA = "index";
    public static final String HASH_KEY_NAME_METADATA = "hashKeyName";
    public static final String RANGE_KEY_NAME_METADATA = "rangeKeyName";
    public static final String RANGE_KEY_FILTER_METADATA = "rangeKeyFilter";
    public static final String NON_KEY_FILTER_METADATA = "nonKeyFilter";
    public static final String EXPRESSION_NAMES_METADATA = "expressionAttributeNames";
    public static final String EXPRESSION_VALUES_METADATA = "expressionAttributeValues";

    // Metadata key whose value is a string that represents the maping from normalized column names to
    // any non-8601 format that customer wants to specify
    public static final String DATETIME_FORMAT_MAPPING_PROPERTY = "datetimeFormatMapping";
    // Metadata key whose value is a string representation of default time zone the customer wants to apply to
    // any date/datetime objects that does not include timezone information
    public static final String DEFAULT_TIME_ZONE = "defaultTimeZone";
    // Fallback timezone if a default timezone cant be inferred from the value or customer specified override
    public static final String UTC = "UTC";
}
