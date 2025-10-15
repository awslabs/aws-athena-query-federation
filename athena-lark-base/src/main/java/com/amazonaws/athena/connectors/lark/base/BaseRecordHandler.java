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
import com.amazonaws.athena.connectors.lark.base.model.response.ListRecordsResponse;
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
import org.apache.arrow.vector.types.Types;
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

import java.math.BigDecimal;
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
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_FIELD_TYPE_MAPPING_PROPERTY;
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
    public BaseRecordHandler(Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.envVarService = new EnvVarService(configOptions, invoker);
        this.larkBaseService = new LarkBaseService(envVarService.getLarkAppId(), envVarService.getLarkAppSecret());
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
                                AthenaClient amazonAthena, Map<String, String> configOptions, EnvVarService envVarService, LarkBaseService larkBaseService, LoadingCache<String, ThrottlingInvoker> invokerCache)
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
        RegistererExtractor localRegistererExtractor = new RegistererExtractor(larkFieldTypeMap);
        if (envVarService.isEnableDebugLogging()) {
            logger.info("readWithConstraint: enter - {}", recordsRequest.getSplit());
        }

        try {
            String baseId = split.getProperty(BASE_ID_PROPERTY);
            String tableId = split.getProperty(TABLE_ID_PROPERTY);
            String originalFilterExpression = split.getProperty(FILTER_EXPRESSION_PROPERTY);
            String originalSortExpression = split.getProperties().getOrDefault(SORT_EXPRESSION_PROPERTY, "");
            int pageSizeForApi = Integer.parseInt(split.getProperty(PAGE_SIZE_PROPERTY));
            int expectedRowCountForSplit = Integer.parseInt(split.getProperty(EXPECTED_ROW_COUNT_PROPERTY));
            boolean isParallelSplit = Boolean.parseBoolean(split.getProperties().getOrDefault(IS_PARALLEL_SPLIT_PROPERTY, "false"));
            long splitStartIndex = Long.parseLong(split.getProperties().getOrDefault(SPLIT_START_INDEX_PROPERTY, "0"));
            long splitEndIndex = Long.parseLong(split.getProperties().getOrDefault(SPLIT_END_INDEX_PROPERTY, "0"));

            invokerCache.get(tableId).setBlockSpiller(spiller);

            Iterator<Map<String, Object>> recordIterator = getIterator(
                    baseId,
                    tableId,
                    pageSizeForApi,
                    expectedRowCountForSplit,
                    isParallelSplit,
                    splitStartIndex,
                    splitEndIndex,
                    originalFilterExpression,
                    originalSortExpression);

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
        GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
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

        final Constraints constraints = recordsRequest.getConstraints();
        final Map<String, ValueSet> constraintSummary = (constraints != null) ? constraints.getSummary() : Collections.emptyMap();
        final org.apache.arrow.vector.types.pojo.Schema schema = recordsRequest.getSchema();

        while (itemIterator.hasNext() && queryStatusChecker.isQueryRunning()) {
            Map<String, Object> item = itemIterator.next();
            final int currentRowNum = ++rowCount;

            try {
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("Attempting to write row #{}. Flattened data: {}", currentRowNum, item);
                }

                // Make sure all schema fields are present, provide defaults based on schema AND constraints
                for (Field field : schema.getFields()) {
                    String fieldName = field.getName();
                    if (!item.containsKey(fieldName)) {
                        boolean constraintAllowsNull = true;
                        ValueSet valueSet = constraintSummary.get(fieldName);
                        if (valueSet != null) {
                            constraintAllowsNull = valueSet.isNullAllowed();
                            if (envVarService.isEnableDebugLogging()) {
                                logger.info("Row #{}: Constraint found for field '{}'. nullAllowed={}", currentRowNum, fieldName, constraintAllowsNull);
                            }
                        }

                        // Input null if schema allows null and constraint allows null.
                        // Otherwise, insert default non-null value.
                        if (field.isNullable() && constraintAllowsNull) {
                            if (field.getType() instanceof ArrowType.Bool) {
                                item.put(fieldName, false);
                                if (envVarService.isEnableDebugLogging()) {
                                    logger.info("Row #{}: Missing boolean field '{}'. Defaulting to false.", currentRowNum, fieldName);
                                }
                            }
                            else {
                                item.put(fieldName, null);
                                if (envVarService.isEnableDebugLogging()) {
                                    logger.info("Row #{}: Field '{}' is nullable and constraint allows null (or no constraint), putting null.", currentRowNum, fieldName);
                                }
                            }
                        }
                        else {
                            ArrowType fieldType = field.getType();
                            Object defaultValue = getDefaultValueForType(fieldType);
                            item.put(fieldName, defaultValue);
                        }
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
     * Determines a default value for a given ArrowType, intended for non-nullable fields
     * that are missing from the source data. Uses Types.MinorType for switching.
     * Usually happens when the field is being filtered from the query.
     * For example: SELECT * FROM table WHERE field <> 'foo'
     *
     * @param type The ArrowType of the field.
     * @return A default value (e.g., 0, "", false) or null if no suitable default is known.
     */
    private Object getDefaultValueForType(ArrowType type)
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        if (envVarService.isEnableDebugLogging()) {
            logger.info("getDefaultValueForType: type={}, minorType={}", type, minorType);
        }

        return switch (minorType) {
            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> "";

            case BIT -> false;

            case TINYINT, SMALLINT, INT, UINT1, UINT2, UINT4, DATEDAY -> 0;
            case BIGINT, UINT8, DATEMILLI, TIMESEC, TIMEMILLI, TIMEMICRO, TIMENANO, TIMESTAMPSEC, TIMESTAMPMILLI,
                 TIMESTAMPMICRO, TIMESTAMPNANO, TIMESTAMPSECTZ, TIMESTAMPMILLITZ, TIMESTAMPMICROTZ, TIMESTAMPNANOTZ,
                 DURATION -> 0L;

            case FLOAT4, FLOAT2 -> 0.0f;
            case FLOAT8 -> 0.0d;
            case DECIMAL, DECIMAL256 -> BigDecimal.ZERO;

            case VARBINARY, LARGEVARBINARY, FIXEDSIZEBINARY, VIEWVARBINARY -> new byte[0];

            case INTERVALDAY, INTERVALYEAR, INTERVALMONTHDAYNANO -> {
                logger.warn("Cannot determine a safe default value for non-nullable Interval type {}. Returning null.", minorType);
                yield null;
            }

            case LIST, LARGELIST, LISTVIEW, LARGELISTVIEW, FIXED_SIZE_LIST -> {
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("Returning empty List as default for non-nullable MinorType {}", minorType);
                }
                yield Collections.emptyList();
            }
            case STRUCT, MAP -> {
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("Returning empty Map as default for non-nullable MinorType {}", minorType);
                }
                yield Collections.emptyMap();
            }

            case UNION, DENSEUNION, RUNENDENCODED, EXTENSIONTYPE, NULL -> {
                logger.warn("Cannot determine a safe default value for non-nullable MinorType {}. Returning null.", minorType);
                yield null;
            }
        };
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
            String originalSortExpression)
    {
        return new Iterator<>()
        {
            private Iterator<ListRecordsResponse.RecordItem> currentPageIterator = null;
            private String currentPageToken = null;
            private boolean hasMorePages = true;
            private int currentFetchDataCount = 0;
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
                                    .build();

                    ListRecordsResponse response = invokerCache.get(tableId).invoke(() ->
                            larkBaseService.getTableRecords(tableRecordsRequest)
                    );

                    String nextPageToken = (response != null) ? response.getPageToken() : null;
                    boolean responseHasMore = (response != null) && response.hasMore();
                    List<ListRecordsResponse.RecordItem> records = (response != null) ? response.getItems() : Collections.emptyList();
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
                    logger.error("Error fetching next page from Lark API for table {}.{}: {}", baseId, tableId, e.getMessage(), e);
                    hasMorePages = false;
                    currentPageIterator = null;
                    throw new RuntimeException("Error fetching next page from Lark API for table " + baseId + "." + tableId, e);
                }
            }

            @Override
            public boolean hasNext()
            {
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
                ListRecordsResponse.RecordItem item = currentPageIterator.next();
                Map<String, Object> result = item.getFields() instanceof HashMap ?
                        item.getFields() : new HashMap<>(item.getFields());
                result.put(RESERVED_RECORD_ID, item.getRecordId());
                result.put(RESERVED_TABLE_ID, tableId);
                result.put(RESERVED_BASE_ID, baseId);
                return result;
            }
        };
    }
}
