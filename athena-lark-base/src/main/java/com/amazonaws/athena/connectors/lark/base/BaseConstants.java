/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base;

public final class BaseConstants
{
    private BaseConstants()
    {
        // Prevent instantiation
    }

    /**
     * The source type that is used to aid in logging diagnostic info when raising a support case.
     */
    public static final String SOURCE_TYPE = "lark-base";

    /**
     * The secret manager that is used to get the Lark credentials.
     */
    public static final String LARK_APP_KEY_ENV_VAR = "default_secret_manager_lark_app_key";

    /**
     * The environment variable which is used to set the default max page size for the connector.
     */
    public static final String DOES_ACTIVATE_EXPERIMENTAL_FEATURE_ENV_VAR = "default_does_activate_experimental_feature";

    /**
     * The environment variable which is used to tell the connector to get table schema from Lark.
     */
    public static final String DOES_ACTIVATE_LARK_BASE_SOURCE_ENV_VAR = "default_does_activate_lark_base_source";

    /**
     * The environment variable which is used to tell the connector to get table schema lark base from Lark Drive.
     */
    public static final String DOES_ACTIVATE_LARK_DRIVE_SOURCE_ENV_VAR = "default_does_activate_lark_drive_source";

    /**
     * The environment variable which is used to tell the connector to generate multiple split if certains conditions are met.
     */
    public static final String DOES_ACTIVATE_PARALLEL_SPLIT_ENV_VAR = "default_does_activate_parallel_split";

    /**
     * The environment variable which is used to enable debug logging (verbose INFO logs).
     * When set to "true", all INFO logs will be shown. When "false" (default), only WARN and ERROR logs are shown.
     */
    public static final String ENABLE_DEBUG_LOGGING_ENV_VAR = "default_enable_debug_logging";

    /**
     * The environment variable which is used to set the default lark base sources for the connector.
     * If we use this, we can ignore crawler and use the lark base sources directly.
     * format: [larkBaseId:larkTableId1,larkBaseId:larkTableId2,...]
     */
    public static final String LARK_BASE_SOURCES_ENV_VAR = "default_lark_base_sources";

    /**
     * The environment variable which is used to set the default lark drive sources for the connector.
     * If we use this, we can ignore crawler and use the lark drive sources directly.
     * format: [larkDriveFolderToken,larkDriveFolderToken,...]
     */
    public static final String LARK_DRIVE_SOURCES_ENV_VAR = "default_lark_drive_sources";

    /**
     * The lark base flag which is used to identify the custom flag on glue catalog.
     */
    public static final String LARK_BASE_FLAG = "lark-base-flag";

    /**
     * The base id property that helps metadata handler and record handler communicate the base id.
     */
    public static final String BASE_ID_PROPERTY = "base_id";

    /**
     * The table id property that helps metadata handler and record handler communicate the table id.
     */
    public static final String TABLE_ID_PROPERTY = "table_id";

    /**
     * The page size property that helps metadata handler and record handler communicate the page size.
     * by default, it is 500 based on lark api, but it should be overriden by the limit if the limit is < 500.
     */
    public static final String PAGE_SIZE_PROPERTY = "page_size";

    /**
     * The expected row count property that helps metadata handler and record handler communicate the expected row count.
     * to help record handler determine when to stop reading the data and call the next page.
     */
    public static final String EXPECTED_ROW_COUNT_PROPERTY = "expected_row_count";

    /**
     * The is parallel split property that helps metadata handler and record handler communicate the is parallel split.
     * this is used to identify the record id that is used to identify the record in the lark base.
     */
    public static final String IS_PARALLEL_SPLIT_PROPERTY = "is_parallel_split";

    /**
     * The split start index property that helps metadata handler and record handler communicate the split start index.
     */
    public static final String SPLIT_START_INDEX_PROPERTY = "split_start_index";

    /**
     * The split end index property that helps metadata handler and record handler communicate the split end index.
     */
    public static final String SPLIT_END_INDEX_PROPERTY = "split_end_index";

    /**
     * The filter expression property that helps metadata handler and record handler communicate the filter expression.
     */
    public static final String FILTER_EXPRESSION_PROPERTY = "filter_expression";

    /**
     * The sort expression property that helps metadata handler and record handler communicate the sort expression.
     */
    public static final String SORT_EXPRESSION_PROPERTY = "sort_expression";

    /**
     * The property that helps metadata handler and record handler communicate the Lark field type mapping.
     * Stores a JSON string representing a Map<String, String> (AthenaFieldName -> LarkUiType).
     */
    public static final String LARK_FIELD_TYPE_MAPPING_PROPERTY = "lark_field_type_mapping";

    /**
     * The reserved record id property that helps metadata handler and record handler communicate the reserved record id.
     * this is used to identify the record id that is used to identify the record in the lark base.
     */
    public static final String RESERVED_RECORD_ID = "$reserved_record_id";

    /**
     * The reserved table id property that helps metadata handler and record handler communicate the reserved table id.
     * this is used to identify the table id that is used to identify the table in the lark base.
     */
    public static final String RESERVED_TABLE_ID = "$reserved_table_id";

    /**
     * The reserved base id property that helps metadata handler and record handler communicate the reserved base id.
     * this is used to identify the base id that is used to identify the base in the lark base.
     */
    public static final String RESERVED_BASE_ID = "$reserved_base_id";

    /**
     * The reserved split key property that helps metadata handler and record handler communicate the reserved split key.
     * this is used to identify the split key that is used to identify the split in the lark base.
     */
    public static final String RESERVED_SPLIT_KEY = "$reserved_split_key";

    /**
     * This is reserved parameter on glue catalog like database, table, column name.
     */
    public static final String LARK_BASE_ID_PARAMETER = "larkBaseId";

    /**
     * This is reserved parameter on glue catalog like database, table, column name.
     */
    public static final String LARK_TABLE_ID_PARAMETER = "larkTableId";

    /**
     * This is constant for the default page size.
     */
    public static final int PAGE_SIZE = 500;
}
