/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.response.SearchRecordsResponse;
import com.amazonaws.athena.connectors.lark.base.service.EnvVarService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import com.amazonaws.athena.connectors.lark.base.translator.RegistererExtractor;
import com.amazonaws.athena.connectors.lark.base.translator.SearchApiFilterTranslator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.BASE_ID_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.EXPECTED_ROW_COUNT_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.FILTER_EXPRESSION_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.IS_PARALLEL_SPLIT_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_FIELD_NAME_MAPPING_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_FIELD_TYPE_MAPPING_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.NULLS_FIRST_FIELD_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.PAGE_SIZE_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_BASE_ID;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_RECORD_ID;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_TABLE_ID;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SORT_EXPRESSION_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SOURCE_TYPE;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SPLIT_END_INDEX_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SPLIT_START_INDEX_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.TABLE_ID_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.throttling.BaseExceptionFilter.EXCEPTION_FILTER;
import static java.util.Objects.requireNonNull;

/**
 * Class for Lark Base that is used to read data from Lark Base and write it to BlockSpiller for Athena processing.
 */
public class BaseRecordHandler extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BaseRecordHandler.class);

    private final EnvVarService envVarService;
    private final LarkBaseService larkBaseService;
    private final LoadingCache<String, ThrottlingInvoker> invokerCache;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor for RecordHandler.
     *
     * @param configOptions Connector configuration options
     */
    public BaseRecordHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.envVarService = new EnvVarService(configOptions, invoker);
        this.larkBaseService = new LarkBaseService(envVarService.getLarkAppId(), envVarService.getLarkAppSecret(), envVarService.getLookupMaxDepth());
        this.invokerCache = CacheBuilder.newBuilder().build(
                new CacheLoader<>()
                {
                    @Override
                    @Nonnull
                    public ThrottlingInvoker load(@Nonnull String tableId)
                    {
                        return invoker;
                    }
                }
        );
    }

    /**
     * Constructor for testing purposes.
     */
    @VisibleForTesting
    protected BaseRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager,
                                AthenaClient amazonAthena, java.util.Map<String, String> configOptions, EnvVarService envVarService, LarkBaseService larkBaseService, LoadingCache<String, ThrottlingInvoker> invokerCache)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE, configOptions);
        this.envVarService = envVarService;
        this.larkBaseService = larkBaseService;
        this.invokerCache = invokerCache;
    }

    /**
     * Reading data from Lark Base and writing it to BlockSpiller.
     *
     * @param spiller            BlockSpiller for writing records
     * @param recordsRequest     Details of the read request
     * @param queryStatusChecker Checker for query status
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                                      QueryStatusChecker queryStatusChecker)
    {
        requireNonNull(spiller, "spiller cannot be null");
        requireNonNull(recordsRequest, "recordsRequest cannot be null");
        requireNonNull(queryStatusChecker, "queryStatusChecker cannot be null");

        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            logger.error("readWithConstraint for QueryPassthrough currently not supported");
            throw new AthenaConnectorException("QueryPassthrough not supported",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }

        Split split = recordsRequest.getSplit();
        String larkFieldTypeMappingJson = split.getProperty(LARK_FIELD_TYPE_MAPPING_PROPERTY);
        Map<String, NestedUIType> larkFieldTypeMap = Collections.emptyMap();

        if (larkFieldTypeMappingJson != null && !larkFieldTypeMappingJson.isEmpty()) {
            try {
                larkFieldTypeMap = objectMapper.readValue(larkFieldTypeMappingJson, new TypeReference<>()
                {
                });
            }
            catch (Exception e) {
                logger.warn("readWithConstraint: Failed to deserialize Lark field type mapping: {}. Proceeding without it.", e.getMessage(), e);
            }
        }

        // Maps each original Lark field name to its resolved (possibly collision-disambiguated)
        // Athena column name. Without this, re-sanitizing field names independently while fetching
        // records would collapse two colliding fields back into a single key, even though the schema
        // (built from the same fieldNameMappings) already tells them apart.
        String larkFieldNameMappingJson = split.getProperty(LARK_FIELD_NAME_MAPPING_PROPERTY);
        Map<String, String> larkFieldNameMap = Collections.emptyMap();

        if (larkFieldNameMappingJson != null && !larkFieldNameMappingJson.isEmpty()) {
            try {
                larkFieldNameMap = objectMapper.readValue(larkFieldNameMappingJson, new TypeReference<>()
                {
                });
            }
            catch (Exception e) {
                logger.warn("readWithConstraint: Failed to deserialize Lark field name mapping: {}. Proceeding without it.", e.getMessage(), e);
            }
        }
        RegistererExtractor localRegistererExtractor = new RegistererExtractor(larkFieldTypeMap);
        if (envVarService.isEnableDebugLogging()) {
            logger.info("readWithConstraint: enter - {}", recordsRequest.getSplit());
        }

        try {
            String baseId = split.getProperty(BASE_ID_PROPERTY);
            String tableId = split.getProperty(TABLE_ID_PROPERTY);
            String originalFilterExpression = split.getProperty(FILTER_EXPRESSION_PROPERTY);
            String originalSortExpression = split.getProperties().getOrDefault(SORT_EXPRESSION_PROPERTY, "");
            String nullsFirstFieldName = split.getProperties().getOrDefault(NULLS_FIRST_FIELD_PROPERTY, "");
            int pageSizeForApi = Integer.parseInt(split.getProperty(PAGE_SIZE_PROPERTY));
            int expectedRowCountForSplit = Integer.parseInt(split.getProperty(EXPECTED_ROW_COUNT_PROPERTY));
            boolean isParallelSplit = Boolean.parseBoolean(split.getProperties().getOrDefault(IS_PARALLEL_SPLIT_PROPERTY, "false"));
            long splitStartIndex = Long.parseLong(split.getProperties().getOrDefault(SPLIT_START_INDEX_PROPERTY, "0"));
            long splitEndIndex = Long.parseLong(split.getProperties().getOrDefault(SPLIT_END_INDEX_PROPERTY, "0"));

            invokerCache.get(tableId).setBlockSpiller(spiller);

            Iterator<Map<String, Object>> recordIterator;
            if (!nullsFirstFieldName.isEmpty()) {
                // Lark's sort has no null-positioning control and always puts nulls last, so an
                // ORDER BY ... NULLS FIRST can't be satisfied by a single sorted request. Fetch the
                // null rows first (via an added "isEmpty" filter condition, sort is a no-op among them
                // since they all tie on this column) and then the Lark-sorted non-null rows (via
                // "isNotEmpty", excluding nulls so they aren't emitted twice at the end).
                Iterator<Map<String, Object>> nullsIterator = getIterator(
                        baseId, tableId, pageSizeForApi, expectedRowCountForSplit, isParallelSplit,
                        splitStartIndex, splitEndIndex,
                        SearchApiFilterTranslator.addEmptinessCondition(originalFilterExpression, nullsFirstFieldName, true),
                        originalSortExpression, larkFieldNameMap);
                Iterator<Map<String, Object>> nonNullsIterator = getIterator(
                        baseId, tableId, pageSizeForApi, expectedRowCountForSplit, isParallelSplit,
                        splitStartIndex, splitEndIndex,
                        SearchApiFilterTranslator.addEmptinessCondition(originalFilterExpression, nullsFirstFieldName, false),
                        originalSortExpression, larkFieldNameMap);
                recordIterator = new NullsFirstIterator(nullsIterator, nonNullsIterator, expectedRowCountForSplit);
            }
            else {
                recordIterator = getIterator(
                        baseId,
                        tableId,
                        pageSizeForApi,
                        expectedRowCountForSplit,
                        isParallelSplit,
                        splitStartIndex,
                        splitEndIndex,
                        originalFilterExpression,
                        originalSortExpression,
                        larkFieldNameMap);
            }

            writeItemsToBlock(spiller, recordsRequest, queryStatusChecker, recordIterator, localRegistererExtractor);
        }
        catch (Exception e) {
            String errorMsg = String.format("Error reading records from table %s.%s: %s",
                    split.getProperty(BASE_ID_PROPERTY),
                    split.getProperty(TABLE_ID_PROPERTY),
                    e.getMessage());
            logger.error(errorMsg, e);
            throw new AthenaConnectorException(errorMsg,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
    }

    /**
     * Write items to block using spiller.
     *
     * @param spiller            BlockSpiller
     * @param recordsRequest     ReadRecordsRequest
     * @param queryStatusChecker QueryStatusChecker
     * @param itemIterator       Iterator of items
     */
    protected void writeItemsToBlock(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            Iterator<Map<String, Object>> itemIterator,
            RegistererExtractor registererExtractor)
    {
        Constraints constraintsForWriter = stripComplexTypeConstraints(recordsRequest.getConstraints(), recordsRequest.getSchema());
        GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(constraintsForWriter);
        registererExtractor.registerExtractorsForSchema(rowWriterBuilder, recordsRequest.getSchema());

        try {
            GeneratedRowWriter rowWriter = rowWriterBuilder.build();
            processRecords(spiller, recordsRequest, queryStatusChecker, itemIterator, rowWriter);
            if (envVarService.isEnableDebugLogging()) {
                logger.info("Completed writing items to block");
            }
        }
        catch (Exception e) {
            logger.error("Error building/using row writer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write items to block: " + e.getMessage(), e);
        }
    }

    /**
     * GeneratedRowWriter.newBuilder() eagerly materializes a comparison value for every constrained
     * column via the SDK's own MarkerFactory/BlockUtils.newEmptyBlock, which builds a bare childless
     * Field for container types (List/Struct). Arrow's ListVector then rejects that Field ("Lists have
     * one child Field. Found: none") - this happens purely from having e.g. an IS NOT NULL constraint
     * on a List/Struct column, regardless of whether our own field-writer factories ever consult the
     * constraint. It's a gap in the SDK, not something fixable in this connector's schema-building code.
     * Drop constraints on List/Struct columns before handing them to the SDK so it never attempts this;
     * Athena's own query engine re-applies the WHERE clause against the full (unfiltered on these
     * columns) rows we return, so results stay correct - we're just not pushing the filter down.
     */
    private Constraints stripComplexTypeConstraints(Constraints original, org.apache.arrow.vector.types.pojo.Schema schema)
    {
        if (original == null) {
            return null;
        }

        Map<String, ValueSet> filteredSummary = new HashMap<>();
        for (Map.Entry<String, ValueSet> entry : original.getSummary().entrySet()) {
            Field field = schema.findField(entry.getKey());
            ArrowType fieldType = field != null ? field.getType() : null;
            if (fieldType instanceof ArrowType.List || fieldType instanceof ArrowType.Struct) {
                logger.debug("Dropping constraint on complex-type column '{}' to avoid a known SDK crash "
                        + "(MarkerFactory can't build a comparison value for List/Struct types); "
                        + "Athena will re-apply this filter itself.", entry.getKey());
                continue;
            }
            filteredSummary.put(entry.getKey(), entry.getValue());
        }

        if (filteredSummary.size() == original.getSummary().size()) {
            return original;
        }

        return new Constraints(filteredSummary, original.getExpression(), original.getOrderByClause(),
                original.getLimit(), original.getQueryPassthroughArguments(), original.getQueryPlan());
    }

    /**
     * Processes records from the iterator and writes them to the BlockSpiller.
     * Handles potential errors during row writing by attempting to write a 'safe' version (stringified).
     *
     * @param spiller            The BlockSpiller to write rows to.
     * @param queryStatusChecker Checks if the query is still active.
     * @param itemIterator       Iterator over the raw data records (Map<String, Object>).
     * @param rowWriter          The GeneratedRowWriter configured with appropriate extractors.
     */
    private void processRecords(
            BlockSpiller spiller,
            ReadRecordsRequest recordsRequest,
            QueryStatusChecker queryStatusChecker,
            Iterator<Map<String, Object>> itemIterator,
            GeneratedRowWriter rowWriter)
    {
        int rowCount = 0;
        long successCount = 0;
        long errorCount = 0;

        final org.apache.arrow.vector.types.pojo.Schema schema = recordsRequest.getSchema();

        while (itemIterator.hasNext() && queryStatusChecker.isQueryRunning()) {
            Map<String, Object> item = itemIterator.next();
            final int currentRowNum = ++rowCount;

            try {
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("Attempting to write row #{}. Flattened data: {}", currentRowNum, item);
                }

                // A key missing from `item` means the field was null in Lark's response - RecordItem's
                // constructor strips null-valued entries entirely (see SearchRecordsResponse.RecordItem), so
                // "absent" and "was null" are the same thing here. Every schema field is nullable (see
                // LarkBaseTypeUtils), so always put null and let each extractor's already-correct null
                // handling (isSet=0) take it from there. A previous version of this loop inserted a fake
                // non-null default (e.g. "" for VARCHAR, BigDecimal.ZERO for DECIMAL) whenever the active
                // constraint had nullAllowed=false, on the theory that the SDK's row-level ConstraintProjector
                // needed a concrete value to check - but the SDK correctly evaluates ConstraintProjector.apply
                // (null) too (confirmed against the SDK's VarCharFieldWriter bytecode), and the fake default
                // could itself spuriously satisfy the constraint (confirmed live: an empty-string default
                // sorts below every real value, so `field_single_select NOT BETWEEN 'Option A' AND 'Option B'`
                // included all 10 genuinely-null rows as if they were "less than 'Option A'", returning 190
                // instead of 180). This only surfaces for columns where pushdown to Lark was skipped (so the
                // null rows are actually fetched instead of filtered server-side) combined with a
                // nullAllowed=false constraint - e.g. any inequality/range query on a non-orderable type.
                for (Field field : schema.getFields()) {
                    String fieldName = field.getName();
                    if (!item.containsKey(fieldName)) {
                        item.put(fieldName, null);
                    }
                }

                final Map<String, Object> dataToWrite = item;
                final long[] writeResult = new long[1];

                spiller.writeRows((Block block, int rowNum) -> {
                    try {
                        boolean success = rowWriter.writeRow(block, rowNum, dataToWrite);
                        if (success) {
                            writeResult[0] = 1;
                            return 1;
                        }
                        else {
                            if (envVarService.isEnableDebugLogging()) {
                                logger.info("rowWriter.writeRow returned false for row #{}. Data: {}", currentRowNum, dataToWrite);
                            }
                            writeResult[0] = 0;
                            return 0;
                        }
                    }
                    catch (Exception e) {
                        logger.error("Exception writing row #{}: {}. Data: {}", currentRowNum, e.getMessage(), dataToWrite, e);
                        writeResult[0] = 0;
                        return 0;
                    }
                });

                if (writeResult[0] == 1) {
                    successCount++;
                }
                else {
                    errorCount++;
                }
            }
            catch (Exception e) {
                errorCount++;
                logger.error("Unexpected error processing row #{}: {}", currentRowNum, e.getMessage(), e);
            }
        }

        if (envVarService.isEnableDebugLogging()) {
            logger.info("Completed processing records: {} total rows processed, {} success, {} filtered/error",
                    rowCount, successCount, errorCount);
        }
    }

    /**
     * Creates an iterator that fetches records page by page from the LarkBaseService.
     * Handles pagination using page tokens and manages the current page's iterator.
     * Includes rate limiting via ThrottlingInvoker. Adds reserved fields to each record.
     *
     * @param baseId                   The Lark Base ID.
     * @param tableId                  The Lark Table ID.
     * @param pageSizeForApi           The page size for the API.
     * @param expectedRowCountForSplit Expected row count for the split.
     * @param isParallelSplit          Indicates if the split is parallel.
     * @param splitStartIndex          The start index for the split.
     * @param splitEndIndex            The end index for the split.
     * @param originalFilterExpression The filter expression string to pass to the API.
     * @param originalSortExpression   The sort expression string to pass to the API.
     * @return An Iterator over records (Map<String, Object>).
     */
    protected Iterator<Map<String, Object>> getIterator(
            String baseId,
            String tableId,
            int pageSizeForApi,
            int expectedRowCountForSplit,
            boolean isParallelSplit,
            long splitStartIndex,
            long splitEndIndex,
            String originalFilterExpression,
            String originalSortExpression,
            Map<String, String> fieldNameToAthenaNameMap)
    {
        return new Iterator<>()
        {
            private Iterator<SearchRecordsResponse.RecordItem> currentPageIterator = null;
            private String currentPageToken = null;
            private boolean hasMorePages = true;
            private int currentFetchDataCount = 0;
            private int emittedCount = 0;
            private final String finalFilterExpression = buildFinalFilter();
            private final String finalSortExpression = isParallelSplit && envVarService.isActivateParallelSplit() ? "" : originalSortExpression;

            private String buildFinalFilter()
            {
                if (isParallelSplit && envVarService.isActivateParallelSplit()) {
                    return SearchApiFilterTranslator.toSplitFilterJson(
                            originalFilterExpression,
                            splitStartIndex,
                            splitEndIndex
                    );
                }
                else {
                    return originalFilterExpression != null ? originalFilterExpression : "";
                }
            }

            /**
             * Fetches the next page of records from the Lark service if the current page is exhausted
             * and more pages are expected. Updates the iterator state.
             *
             * @return true if a new page was successfully fetched and has records, false otherwise.
             */
            private boolean fetchNextPage()
            {
                if (!hasMorePages || (expectedRowCountForSplit > 0 && currentFetchDataCount >= expectedRowCountForSplit)) {
                    if (envVarService.isEnableDebugLogging()) {
                        logger.info("fetchNextPage: Stopping fetch. HasMorePages={}, FetchedCount={}, ExpectedForSplit={}",
                                hasMorePages, currentFetchDataCount, expectedRowCountForSplit);
                    }
                    return false;
                }
                try {
                    if (envVarService.isEnableDebugLogging()) {
                        logger.info("Fetching next page: base={}, table={}, pageSize={}, pageToken={}, filter='{}', sort='{}'",
                                baseId, tableId, pageSizeForApi, currentPageToken, finalFilterExpression, finalSortExpression);
                    }

                    com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest tableRecordsRequest =
                            com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest.builder()
                                    .baseId(baseId)
                                    .tableId(tableId)
                                    .pageSize(pageSizeForApi)
                                    .pageToken(currentPageToken)
                                    .filterJson(finalFilterExpression)
                                    .sortJson(finalSortExpression)
                                    .fieldNameToAthenaNameMap(fieldNameToAthenaNameMap)
                                    .build();

                    SearchRecordsResponse response = invokerCache.get(tableId).invoke(() ->
                            larkBaseService.getTableRecords(tableRecordsRequest)
                    );

                    String nextPageToken = (response != null) ? response.getPageToken() : null;
                    boolean responseHasMore = (response != null) && response.hasMore();
                    List<SearchRecordsResponse.RecordItem> records = (response != null) ? response.getItems() : Collections.emptyList();
                    if (records == null) {
                        records = Collections.emptyList();
                    }

                    if (envVarService.isEnableDebugLogging()) {
                        logger.info("API Response: Records={}, HasMore={}, NextToken={}", records.size(), responseHasMore, nextPageToken);
                    }

                    currentPageIterator = records.iterator();

                    hasMorePages = responseHasMore && !StringUtils.isEmpty(nextPageToken);
                    currentPageToken = nextPageToken;
                    currentFetchDataCount += records.size();

                    if (expectedRowCountForSplit > 0 && currentFetchDataCount >= expectedRowCountForSplit) {
                        if (envVarService.isEnableDebugLogging()) {
                            logger.info("Reached expected row count ({}) for this split {}. Stopping further fetches.", expectedRowCountForSplit, baseId + "." + tableId);
                        }
                        hasMorePages = false;
                    }

                    return currentPageIterator.hasNext();
                }
                catch (Exception e) {
                    logger.warn("Error fetching next page from Lark API for table {}.{}: {}. Assuming no matching records for filter.", baseId, tableId, e.getMessage());
                    if (envVarService.isEnableDebugLogging()) {
                        logger.debug("Full error details:", e);
                    }
                    hasMorePages = false;
                    currentPageIterator = Collections.emptyIterator();
                    return false;
                }
            }

            @Override
            public boolean hasNext()
            {
                // expectedRowCountForSplit only gates whether fetchNextPage() asks Lark for another
                // page (checked before/after each fetch) - it never trims the page that pushes the
                // running total over the limit. A single over-fetched page can carry up to
                // pageSizeForApi extra records past the target, so the emitted-count cap here is what
                // actually enforces the boundary regardless of how much this split over-fetched.
                if (expectedRowCountForSplit > 0 && emittedCount >= expectedRowCountForSplit) {
                    return false;
                }
                if (currentPageIterator != null && currentPageIterator.hasNext()) {
                    return true;
                }
                if (!hasMorePages) {
                    return false;
                }
                return fetchNextPage();
            }

            @Override
            public Map<String, Object> next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more records available for this split");
                }
                SearchRecordsResponse.RecordItem item = currentPageIterator.next();
                emittedCount++;
                Map<String, Object> result = item.getFields() instanceof HashMap ?
                        item.getFields() : new HashMap<>(item.getFields());
                result.put(RESERVED_RECORD_ID, item.getRecordId());
                result.put(RESERVED_TABLE_ID, tableId);
                result.put(RESERVED_BASE_ID, baseId);
                return result;
            }
        };
    }

    /**
     * Concatenates a nulls-only iterator and a Lark-sorted non-null iterator into a single ordered
     * stream, enforcing the combined {@code expectedRowCountForSplit} cap itself. Each inner iterator
     * also carries its own copy of that same cap (see {@link #getIterator}), which is harmless: it only
     * means the nulls-only iterator alone stops early if null rows already reach the cap, so the
     * non-null iterator is never consulted, and otherwise it exhausts naturally once real pagination
     * runs out - either way this wrapper's own count is what actually bounds total emissions.
     */
    @VisibleForTesting
    protected static final class NullsFirstIterator implements Iterator<Map<String, Object>>
    {
        private final Iterator<Map<String, Object>> nullsIterator;
        private final Iterator<Map<String, Object>> nonNullsIterator;
        private final int expectedRowCountForSplit;
        private int emitted = 0;

        NullsFirstIterator(Iterator<Map<String, Object>> nullsIterator, Iterator<Map<String, Object>> nonNullsIterator,
                            int expectedRowCountForSplit)
        {
            this.nullsIterator = nullsIterator;
            this.nonNullsIterator = nonNullsIterator;
            this.expectedRowCountForSplit = expectedRowCountForSplit;
        }

        @Override
        public boolean hasNext()
        {
            if (expectedRowCountForSplit > 0 && emitted >= expectedRowCountForSplit) {
                return false;
            }
            return nullsIterator.hasNext() || nonNullsIterator.hasNext();
        }

        @Override
        public Map<String, Object> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records available for this split");
            }
            Map<String, Object> result = nullsIterator.hasNext() ? nullsIterator.next() : nonNullsIterator.next();
            emitted++;
            return result;
        }
    }
}
