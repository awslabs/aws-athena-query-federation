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
     * When set to "true", every column that would otherwise be built as a List/Struct-shaped Arrow type
     * (MULTI_SELECT, USER, GROUP_CHAT, ATTACHMENT, CREATED_USER, MODIFIED_USER, LOOKUP, URL, LOCATION,
     * SINGLE_LINK, DUPLEX_LINK) is instead built as a plain VARCHAR column holding a JSON-serialized
     * representation of the same value. Opt-in and off by default - existing tables/queries that rely on
     * List/Struct-typed columns are unaffected unless this is explicitly set.
     * <p>
     * Exists because any WHERE constraint (including IS NOT NULL) referencing a List/Struct-typed column
     * crashes the whole query inside Amazon Athena's own managed query engine
     * ({@code IllegalArgumentException: Lists have one child Field. Found: none}, from Apache Arrow's
     * {@code ListVector.initializeChildrenFromFields}) - a genuine platform-level limitation, not something
     * fixable by changing what this connector returns while the column stays List/Struct-typed. Representing
     * the value as a JSON string instead sidesteps the crash entirely, at the cost of losing native
     * array/struct access in Athena (callers must parse the JSON string themselves).
     * <p>
     * Only affects schema built directly by this connector (the live Lark source / experimental providers).
     * A crawler-populated (Glue-backed) table's schema instead comes from Glue's stored type string - set
     * the identically-named env var on {@code glue-lark-base-crawler} too so a re-crawl agrees with this.
     */
    public static final String DOES_ACTIVATE_COMPLEX_TYPE_AS_JSON_STRING_ENV_VAR = "default_does_activate_complex_type_as_json_string";

    /**
     * The environment variable which is used to cap how many LOOKUP hops the connector will follow when resolving
     * a chained LOOKUP field's effective type (e.g. a LOOKUP pointing at another LOOKUP in a different table).
     * This is a defense-in-depth safety valve on top of cycle detection, in case a legitimate (non-circular) chain
     * is unexpectedly deep. Defaults to {@code DEFAULT_LARK_LOOKUP_MAX_DEPTH} if unset or not a positive integer.
     */
    public static final String LARK_LOOKUP_MAX_DEPTH_ENV_VAR = "default_lark_lookup_max_depth";

    /**
     * Default value for {@link #LARK_LOOKUP_MAX_DEPTH_ENV_VAR} when the environment variable is not set.
     */
    public static final int DEFAULT_LARK_LOOKUP_MAX_DEPTH = 20;

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
     * The environment variable which restricts, per schema, which tables the connector will expose.
     * When a schema has at least one entry here, only those tables are visible/queryable for that schema;
     * schemas with no entry are unrestricted by this setting. Evaluated independently of
     * {@link #BLACKLIST_TABLES_ENV_VAR} (blacklist always wins if a table appears in both).
     * format: [schemaName:tableName,schemaName:tableName2,...]
     */
    public static final String WHITELIST_TABLES_ENV_VAR = "default_whitelist_tables";

    /**
     * The environment variable which excludes, per schema, specific tables from the connector regardless of
     * {@link #WHITELIST_TABLES_ENV_VAR}. A table listed here is never visible or queryable.
     * format: [schemaName:tableName,schemaName:tableName2,...]
     */
    public static final String BLACKLIST_TABLES_ENV_VAR = "default_blacklist_tables";

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
     * Carries the raw, un-clamped {@code getTotalRowCount} result from {@code getPartitions} through to
     * {@code doGetSplits}. Unlike {@link #EXPECTED_ROW_COUNT_PROPERTY} (which {@code writeSinglePartition}
     * may cap at the query's LIMIT), this is always the true total matching row count. Athena's engine
     * doesn't populate {@code GetTableLayoutRequest}'s ORDER BY constraint - it's only visible once
     * {@code GetSplitsRequest} arrives - so {@code getPartitions} cannot know in advance whether a query
     * is an ORDER BY one and skip its own row-count lookup accordingly; every query pays for one such
     * lookup at that stage regardless. Reusing that already-fetched raw count here lets doGetSplits's
     * ORDER BY branch size its single collapsed split correctly without a second, redundant Lark API call
     * for the identical baseId/tableId/filterExpression.
     */
    public static final String RAW_TOTAL_ROW_COUNT_PROPERTY = "raw_total_row_count";

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
     * Carries the original Lark field name of the primary ORDER BY column when its direction explicitly
     * requests NULLS FIRST (e.g. {@code ORDER BY x ASC NULLS FIRST}). Lark's Search API sort parameter has
     * no null-positioning control and always places nulls last regardless of the "desc" flag, so this
     * property tells the record handler to run a two-phase fetch (nulls, then the Lark-sorted non-null
     * rows) instead of trusting a single sorted request. Empty/absent when no such conflict exists.
     */
    public static final String NULLS_FIRST_FIELD_PROPERTY = "nulls_first_field";

    /**
     * The property that helps metadata handler and record handler communicate the Lark field type mapping.
     * Stores a JSON string representing a Map<String, String> (AthenaFieldName -> LarkUiType).
     */
    public static final String LARK_FIELD_TYPE_MAPPING_PROPERTY = "lark_field_type_mapping";

    /**
     * The property that helps metadata handler and record handler communicate the mapping from each
     * original Lark field name to its resolved (possibly collision-disambiguated) Athena column name.
     * Stores a JSON string representing a Map<String, String> (LarkFieldName -> AthenaFieldName).
     */
    public static final String LARK_FIELD_NAME_MAPPING_PROPERTY = "lark_field_name_mapping";

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

    /**
     * Safety budget (in bytes) for how much duplicated field-mapping metadata parallel splitting is allowed
     * to add across all of a query's partition rows/splits. Every parallel partition row (and, from it, every
     * Split) carries its own full copy of the table's field type/name mapping JSON, and neither
     * GetTableLayoutResponse nor GetSplitsResponse/Split supports spilling (only ReadRecordsResponse does), so
     * this duplication counts fully against AWS Lambda's ~6MB synchronous response payload limit. 4MB leaves
     * headroom under that limit for the rest of each response (other split properties, JSON/Arrow envelope
     * overhead). When the projected total would exceed this budget, parallel splitting is skipped in favor of
     * the single, sequentially-paginated partition, which already fetches the full result set correctly.
     */
    public static final long MAX_PARALLEL_SPLIT_MAPPING_BYTES = 4_000_000L;
}
