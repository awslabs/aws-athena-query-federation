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
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.service;

import com.amazonaws.athena.connectors.lark.base.model.LarkDatabaseRecord;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.model.response.ListAllTableResponse;
import com.amazonaws.athena.connectors.lark.base.model.response.ListFieldResponse;
import com.amazonaws.athena.connectors.lark.base.model.response.SearchRecordsResponse;
import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import com.amazonaws.athena.connectors.lark.base.util.SearchApiResponseNormalizer;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Pair;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.DEFAULT_LARK_LOOKUP_MAX_DEPTH;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.PAGE_SIZE;
import static java.util.Objects.requireNonNull;

public class LarkBaseService extends CommonLarkService
{
    private static final Logger logger = LoggerFactory.getLogger(LarkBaseService.class);
    private static final String LARK_BASE_URL = LARK_API_BASE_URL + "/bitable/v1/apps";
    // Without this, Jackson deserializes a fractional NUMBER/CURRENCY value from Lark's JSON as a Double
    // before RegistererExtractor.registerDecimalExtractor ever sees it - IEEE 754 double only carries
    // ~15-17 significant decimal digits, so a value combining enough magnitude and decimal precision
    // (e.g. a large currency total with cents) silently loses precision at parse time, before any of this
    // codebase's own (correct) BigDecimal handling runs. Deserializing floats as BigDecimal instead
    // preserves the original JSON digits exactly; integer-valued fields are unaffected (Jackson only
    // applies this to JSON tokens containing a decimal point or exponent).
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private static final int FIELD_CACHE_MAX_SIZE = 1000;
    private static final int FIELD_CACHE_TTL_MINUTES = 5;

    // Cache table fields to avoid N+1 query problem when resolving lookup types
    private final LoadingCache<String, List<ListFieldResponse.FieldItem>> tableFieldsCache;

    // Safety valve on top of cycle detection for chained LOOKUP resolution; see BaseConstants.LARK_LOOKUP_MAX_DEPTH_ENV_VAR.
    private final int lookupMaxDepth;

    public LarkBaseService(String larkAppId, String larkAppSecret)
    {
        this(larkAppId, larkAppSecret, DEFAULT_LARK_LOOKUP_MAX_DEPTH);
    }

    public LarkBaseService(String larkAppId, String larkAppSecret, int lookupMaxDepth)
    {
        super(larkAppId, larkAppSecret);
        this.lookupMaxDepth = lookupMaxDepth;
        this.tableFieldsCache = buildTableFieldsCache();
    }

    public LarkBaseService(String larkAppId, String larkAppSecret, HttpClientWrapper httpClient)
    {
        this(larkAppId, larkAppSecret, httpClient, DEFAULT_LARK_LOOKUP_MAX_DEPTH);
    }

    public LarkBaseService(String larkAppId, String larkAppSecret, HttpClientWrapper httpClient, int lookupMaxDepth)
    {
        super(larkAppId, larkAppSecret, httpClient);
        this.lookupMaxDepth = lookupMaxDepth;
        this.tableFieldsCache = buildTableFieldsCache();
    }

    private LoadingCache<String, List<ListFieldResponse.FieldItem>> buildTableFieldsCache()
    {
        return CacheBuilder.newBuilder()
                .maximumSize(FIELD_CACHE_MAX_SIZE)
                .expireAfterWrite(FIELD_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
                .build(new CacheLoader<String, List<ListFieldResponse.FieldItem>>()
                {
                    @Override
                    @Nonnull
                    public List<ListFieldResponse.FieldItem> load(@Nonnull String tableKey) throws Exception
                    {
                        String[] parts = tableKey.split("\\|");
                        if (parts.length != 2) {
                            throw new IllegalArgumentException("Invalid table key format: " + tableKey);
                        }
                        return fetchTableFieldsUncached(parts[0], parts[1]);
                    }
                });
    }

    /**
     * Get all records from a table
     *
     * @param baseId  base ID
     * @param tableId table ID
     * @return Response with list of records and pagination token
     */
    public List<LarkDatabaseRecord> getDatabaseRecords(String baseId, String tableId) throws IOException
    {
        List<LarkDatabaseRecord> parsedRecords = new ArrayList<>();
        String pageToken = null;
        boolean hasMore;

        do {
            com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest tableRecordsRequest =
                    com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest.builder()
                            .baseId(baseId)
                            .tableId(tableId)
                            .pageSize(PAGE_SIZE)
                            .pageToken(pageToken)
                            .build();

            SearchRecordsResponse recordsResponse = getTableRecords(tableRecordsRequest);

            if (recordsResponse.getCode() == 0) {
                if (recordsResponse.getItems() != null) {
                    for (SearchRecordsResponse.RecordItem record : recordsResponse.getItems()) {
                        Map<String, Object> fields = record.getFields();

                        String id = null;
                        String name = null;

                        if (fields != null) {
                            if (fields.containsKey("id")) {
                                Object idObj = fields.get("id");
                                id = idObj != null ? idObj.toString() : null;
                            }

                            if (fields.containsKey("name")) {
                                Object nameObj = fields.get("name");
                                name = nameObj != null ? nameObj.toString() : null;
                            }
                        }

                        parsedRecords.add(new LarkDatabaseRecord(id, name));
                    }
                }

                pageToken = recordsResponse.getPageToken();
                hasMore = recordsResponse.hasMore();
            }
            else {
                throw new IOException("Failed to retrieve records for table: " + tableId + ", Error: " + recordsResponse.getMsg());
            }
        }
        while (hasMore);

        return parsedRecords;
    }

    /**
     * Getting records from a table with or without filter using the Search API.
     * <a href="https://open.larksuite.com/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-record/search">DOCS</a>
     *
     * @param request Request parameters encapsulating all query options
     * @return Response with list of records and pagination token
     * @throws IOException if API communication fails
     */
    public SearchRecordsResponse getTableRecords(com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest request) throws IOException
    {
        requireNonNull(request, "request cannot be null");
        refreshTenantAccessToken();

        try {
            // page_size/page_token MUST be query parameters, not body fields: sending page_token in
            // the JSON body causes the Lark API to never advance past the first page - has_more stays
            // true and the same page_token/records get returned forever, regardless of how many times
            // you re-request "the next page". Confirmed by direct comparison: identical page_token and
            // request body, only difference being query-param vs body placement, and only the
            // query-param form actually reaches has_more=false. filter/sort remain in the body since
            // they're structured objects, not scalars.
            URIBuilder uriBuilder = new URIBuilder(LARK_BASE_URL + "/" + request.getBaseId() + "/tables/" + request.getTableId() + "/records/search")
                    .addParameter("page_size", String.valueOf(request.getPageSize()));

            if (request.getPageToken() != null && !request.getPageToken().isEmpty()) {
                uriBuilder.addParameter("page_token", request.getPageToken());
            }

            URI uri = uriBuilder.build();

            logger.info("Fetching records from Lark Base Search API, url: {}", uri);

            // Build request body
            com.amazonaws.athena.connectors.lark.base.model.request.SearchRecordsRequest.Builder requestBuilder =
                    com.amazonaws.athena.connectors.lark.base.model.request.SearchRecordsRequest.builder();

            if (request.getFilterJson() != null && !request.getFilterJson().isEmpty()) {
                requestBuilder.filter(request.getFilterJson());
            }

            if (request.getSortJson() != null && !request.getSortJson().isEmpty()) {
                requestBuilder.sort(request.getSortJson());
            }

            String requestBody = OBJECT_MAPPER.writeValueAsString(requestBuilder.build());

            logger.info("Search API request body: {}", requestBody);

            HttpPost httpRequest = new HttpPost(uri);
            httpRequest.setHeader(HEADER_AUTHORIZATION, AUTH_BEARER_PREFIX + tenantAccessToken);
            httpRequest.setHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
            httpRequest.setEntity(new org.apache.http.entity.StringEntity(requestBody, java.nio.charset.StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = httpClient.execute(httpRequest)) {
                String responseBody = EntityUtils.toString(response.getEntity());

                SearchRecordsResponse recordsResponse =
                        OBJECT_MAPPER.readValue(responseBody, SearchRecordsResponse.class);

                if (recordsResponse.getCode() == 0) {
                    sanitizeRecordFieldNames(recordsResponse, request.getFieldNameToAthenaNameMap());
                    return recordsResponse;
                }
                else {
                    throw new IOException("Failed to retrieve records for table: " + request.getTableId() + ", Error: " + recordsResponse.getMsg());
                }
            }
        }
        catch (URISyntaxException e) {
            throw new IOException("Invalid URI for Lark Base API", e);
        }
    }

    /**
     * Sanitize field names and normalize Search API response format for all records
     *
     * @param response Response object
     * @param fieldNameToAthenaNameMap Maps each original Lark field name to the (possibly
     * collision-disambiguated) Athena column name decided at schema-discovery time. When two Lark
     * fields sanitize to the same name (e.g. "Segment 5" and "segment 5" both -> "segment_5"), the
     * schema already tells them apart by suffixing one with its field ID; re-sanitizing each field
     * name independently here (ignoring this map) would collapse both back into a single key,
     * silently dropping one field's value or misattributing it to the other's column. Falls back to
     * plain sanitization for any field name not present in the map (e.g. no schema was resolved).
     */
    private void sanitizeRecordFieldNames(SearchRecordsResponse response, Map<String, String> fieldNameToAthenaNameMap)
    {
        if (response.getItems() == null) {
            return;
        }

        for (SearchRecordsResponse.RecordItem item : response.getItems()) {
            Map<String, Object> sanitizedFields = new HashMap<>();
            Map<String, Object> originalFields = item.getFields();

            if (originalFields != null) {
                // First normalize Search API format to List API format
                Map<String, Object> normalizedFields = SearchApiResponseNormalizer.normalizeRecordFields(originalFields);

                // Then map field names to their resolved Athena column names
                for (Map.Entry<String, Object> entry : normalizedFields.entrySet()) {
                    String athenaName = fieldNameToAthenaNameMap.get(entry.getKey());
                    String sanitizedKey = athenaName != null ? athenaName : CommonUtil.sanitizeGlueRelatedName(entry.getKey());
                    sanitizedFields.put(sanitizedKey, entry.getValue());
                }

                item.setFields(sanitizedFields);
            }
        }
    }

    /**
     * Get table fields from cache if available, otherwise fetch from API.
     * This prevents N+1 query problem when resolving lookup field types.
     * @see "https://open.larksuite.com/document/server-docs/docs/bitable-v1/app-table-field/list"
     * @param baseId  The base ID
     * @param tableId The table ID
     * @return List of field items
     */
    public List<ListFieldResponse.FieldItem> getTableFields(String baseId, String tableId)
    {
        String cacheKey = baseId + "|" + tableId;
        try {
            return tableFieldsCache.get(cacheKey);
        }
        catch (Exception e) {
            logger.warn("Failed to get fields from cache for {}.{}, falling back to direct fetch: {}",
                    baseId, tableId, e.getMessage(), e);
            return fetchTableFieldsUncached(baseId, tableId);
        }
    }

    /**
     * Fetch table fields directly from Lark API without caching.
     * Internal method used by the cache loader.
     *
     * @param baseId  The base ID
     * @param tableId The table ID
     * @return List of field items
     */
    private List<ListFieldResponse.FieldItem> fetchTableFieldsUncached(String baseId, String tableId)
    {
        try {
            refreshTenantAccessToken();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to refresh Lark access token", e);
        }

        List<ListFieldResponse.FieldItem> allFields = new ArrayList<>();
        String pageToken = "";
        boolean hasMore;

        do {
            try {
                URIBuilder uriBuilder = new URIBuilder(LARK_BASE_URL + "/" + baseId + "/tables/" + tableId + "/fields")
                        .addParameter("page_size", String.valueOf(PAGE_SIZE));

                if (!pageToken.isEmpty()) {
                    uriBuilder.addParameter("page_token", pageToken);
                }

                URI uri = uriBuilder.build();

                logger.info("Fetching fields from Lark Base API, url: {}", uri);

                HttpGet request = new HttpGet(uri);
                request.setHeader("Authorization", "Bearer " + tenantAccessToken);
                request.setHeader("Content-Type", "application/json");

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    String responseBody = EntityUtils.toString(response.getEntity());

                    ListFieldResponse fieldResponse =
                            OBJECT_MAPPER.readValue(responseBody, ListFieldResponse.class);

                    if (fieldResponse.getCode() == 0) {
                        List<ListFieldResponse.FieldItem> fields = fieldResponse.getItems();
                        if (fields != null) {
                            allFields.addAll(fields);
                        }

                        pageToken = fieldResponse.getPageToken();
                        hasMore = fieldResponse.hasMore();
                    }
                    else {
                        throw new IOException("Failed to retrieve fields for table: " + tableId + ", Error: " + fieldResponse.getMsg());
                    }
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to get fields for table: " + tableId, e);
            }
        }
        while (hasMore && pageToken != null && !pageToken.isEmpty());

        return allFields;
    }

    /**
     * List all tables.
     *
     * @param baseId The base ID
     * @return The list of tables
     * @see "https://open.larksuite.com/document/server-docs/docs/bitable-v1/app-table/list"
     */
    public List<ListAllTableResponse.BaseItem> listTables(String baseId)
    {
        try {
            refreshTenantAccessToken();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to refresh Lark access token", e);
        }

        List<ListAllTableResponse.BaseItem> allTables = new ArrayList<>();
        String pageToken = "";
        boolean hasMore;

        do {
            try {
                URIBuilder uriBuilder = new URIBuilder(LARK_BASE_URL + "/" + baseId + "/tables")
                        .addParameter("page_size", String.valueOf(PAGE_SIZE));

                if (!pageToken.isEmpty()) {
                    uriBuilder.addParameter("page_token", pageToken);
                }

                URI uri = uriBuilder.build();

                logger.info("Fetching tables from Lark Base API, url: {}", uri);

                HttpGet request = new HttpGet(uri);
                request.setHeader("Authorization", "Bearer " + tenantAccessToken);
                request.setHeader("Content-Type", "application/json");

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    String responseBody = EntityUtils.toString(response.getEntity());
                    ListAllTableResponse tableResponse = OBJECT_MAPPER.readValue(responseBody, ListAllTableResponse.class);

                    // 1254002: No more data
                    if (tableResponse.getCode() == 0 || tableResponse.getCode() == 1254002) {
                        if (tableResponse.getItems() != null) {
                            allTables.addAll(tableResponse.getItems());
                        }

                        pageToken = tableResponse.getPageToken();
                        hasMore = tableResponse.hasMore();
                    }
                    else {
                        throw new IOException("Failed to retrieve tables for base: " + baseId + ", Error: " + tableResponse.getMsg());
                    }
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to get records for base: " + baseId, e);
            }
        }
        while (hasMore && pageToken != null && !pageToken.isEmpty());
        return allTables;
    }

    public UITypeEnum getLookupType(String baseId, String tableId, String fieldId)
    {
        return getLookupType(baseId, tableId, fieldId, new HashSet<>());
    }

    /**
     * Resolves the effective UI type of a (possibly chained) LOOKUP field, following each LOOKUP to its target
     * field/table until a non-LOOKUP type is found.
     *
     * @param visited table/field pairs already visited in this resolution chain. A misconfigured Lark Base can
     *                have LOOKUP fields that reference each other in a cycle (e.g. table A's field looks up to
     *                table B's field, which looks up back to table A's field); without cycle detection this would
     *                recurse indefinitely and crash schema discovery with a StackOverflowError. On top of that,
     *                {@code lookupMaxDepth} (configurable via {@code LARK_LOOKUP_MAX_DEPTH_ENV_VAR}) caps how many
     *                hops are followed even for a legitimate, non-circular chain, as a defense-in-depth safety valve.
     */
    private UITypeEnum getLookupType(String baseId, String tableId, String fieldId, Set<String> visited)
    {
        if (visited.size() >= lookupMaxDepth) {
            logger.warn("LOOKUP resolution for field '{}' in table '{}' (base '{}') exceeded the configured max "
                    + "depth ({}). Returning UNKNOWN.", fieldId, tableId, baseId, lookupMaxDepth);
            return UITypeEnum.UNKNOWN;
        }

        String visitKey = tableId + "|" + fieldId;
        if (!visited.add(visitKey)) {
            logger.warn("Detected circular LOOKUP reference while resolving field '{}' in table '{}' (base '{}'). "
                    + "Breaking the cycle and returning UNKNOWN.", fieldId, tableId, baseId);
            return UITypeEnum.UNKNOWN;
        }

        List<ListFieldResponse.FieldItem> fields = getTableFields(baseId, tableId);
        for (ListFieldResponse.FieldItem field : fields) {
            if (field.getFieldId().equalsIgnoreCase(fieldId)) {
                if (field.getUIType().equals(UITypeEnum.LOOKUP)) {
                    Pair<String, String> lookupId = field.getTargetFieldAndTableForLookup();
                    String newTableId = lookupId.right();
                    String newFieldId = lookupId.left();
                    return getLookupType(baseId, newTableId, newFieldId, visited);
                }

                return field.getUIType();
            }
        }

        return UITypeEnum.UNKNOWN;
    }
}
