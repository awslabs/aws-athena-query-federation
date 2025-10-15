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
package com.amazonaws.athena.connectors.lark.base.metadataProvider;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.PartitionInfoResult;
import com.amazonaws.athena.connectors.lark.base.model.TableSchemaResult;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.model.response.ListFieldResponse;
import com.amazonaws.athena.connectors.lark.base.service.AthenaService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * MetadataProvider implementation that retrieves metadata using the experimental approach:
 * extracting original Base/Table IDs from the Athena query history and calling Lark APIs directly.
 */
public class ExperimentalMetadataProvider
{
    private static final Logger logger = LoggerFactory.getLogger(ExperimentalMetadataProvider.class);

    private final AthenaService athenaService;
    private final LarkBaseService larkBaseService;
    private final ThrottlingInvoker invoker;

    public ExperimentalMetadataProvider(AthenaService athenaService,
                                        LarkBaseService larkBaseService,
                                        ThrottlingInvoker invoker)
    {
        this.athenaService = requireNonNull(athenaService, "athenaService cannot be null");
        this.larkBaseService = requireNonNull(larkBaseService, "larkBaseService cannot be null");
        this.invoker = requireNonNull(invoker, "invoker cannot be null");
    }

    public Optional<TableSchemaResult> getTableSchema(GetTableRequest request)
    {
        logger.info("Experimental Path: Attempting to get schema for {}", request.getTableName());
        Optional<Pair<String, String>> idsOpt = extractOriginalIdsFromQuery(request.getQueryId(), request.getTableName());

        if (idsOpt.isPresent()) {
            String baseId = idsOpt.get().left();
            String tableId = idsOpt.get().right();
            try {
                logger.info("Experimental Path: Getting table fields for baseId: {}, tableId: {}", baseId, tableId);
                List<ListFieldResponse.FieldItem> larkFields = invoker.invoke(() -> larkBaseService.getTableFields(baseId, tableId));
                logger.info("Experimental Path: Got {} fields from Lark", larkFields.size());

                List<AthenaFieldLarkBaseMapping> fieldMappings = new ArrayList<>();

                for (ListFieldResponse.FieldItem field : larkFields) {
                    String larkFieldName = field.getFieldName();
                    if (larkFieldName != null && !larkFieldName.trim().isEmpty()) {
                        logger.info("Experimental Path: Processing field: {}", larkFieldName);
                        UITypeEnum childUIType = field.getFormulaGlueCatalogUITypeEnum();

                        if (childUIType.equals(UITypeEnum.LOOKUP)) {
                            try {
                                Pair<String, String> lookupId = field.getTargetFieldAndTableForLookup();
                                String newTableId = lookupId.right();
                                String newFieldId = lookupId.left();
                                childUIType = larkBaseService.getLookupType(baseId, newTableId, newFieldId);
                            }
                            catch (Exception e) {
                                logger.warn("Experimental Path: Skipping field {} due to error getting lookup type: {}", field.getFieldName(), e.getMessage());
                                continue;
                            }
                        }

                        NestedUIType nestedUIType = new NestedUIType(field.getUIType(), childUIType);
                        fieldMappings.add(new AthenaFieldLarkBaseMapping(request.getTableName().getTableName(), larkFieldName, nestedUIType));
                    }
                }

                if (fieldMappings.isEmpty()) {
                    logger.warn("Experimental Path: No fields found for {}.{}. Cannot build schema.", baseId, tableId);
                    return Optional.empty();
                }

                Schema schema = CommonUtil.buildSchemaFromLarkFields(fieldMappings);
                logger.info("Experimental Path: Built schema: {}", schema);
                return Optional.of(new TableSchemaResult(schema, Collections.emptySet()));
            }
            catch (Exception e) {
                logger.error("Experimental Path: Error fetching fields or building schema for {}.{}: {}", baseId, tableId, e.getMessage(), e);
                return Optional.empty();
            }
        }
        else {
            logger.info("Experimental Path: Could not extract original IDs for {}. Schema not found via this provider.", request.getTableName());
            return Optional.empty();
        }
    }

    public Optional<PartitionInfoResult> getPartitionInfo(TableName tableName, GetTableLayoutRequest request)
    {
        logger.info("Experimental Path: Attempting to get partition info for {}", tableName);
        Optional<Pair<String, String>> idsOpt = extractOriginalIdsFromQuery(request.getQueryId(), tableName);

        if (idsOpt.isPresent()) {
            String baseId = idsOpt.get().left();
            String tableId = idsOpt.get().right();
            List<AthenaFieldLarkBaseMapping> fieldMappings = discoverTableFields(baseId, tableId);

            if (fieldMappings.isEmpty()) {
                logger.warn("Experimental Path: discoverTableFields returned empty mapping for Base='{}', Table='{}'. Likely invalid IDs extracted or API call failed. Returning empty Optional.", baseId, tableId);
                return Optional.empty();
            }

            return Optional.of(new PartitionInfoResult(baseId, tableId, fieldMappings));
        }
        else {
            logger.info("Experimental Path: Could not extract original IDs for {}. Partition info not found via this provider.", tableName);
            return Optional.empty();
        }
    }

    /**
     * Extracts the original case-sensitive Base ID and Table ID from the raw SQL query.
     * (Moved from BaseMetadataHandler)
     */
    private Optional<Pair<String, String>> extractOriginalIdsFromQuery(String queryId, TableName tableName)
    {
        if (queryId == null || queryId.isEmpty()) {
            logger.warn("Experimental Path: Query ID is missing. Cannot extract original IDs.");
            return Optional.empty();
        }
        try {
            String originalQuery = invoker.invoke(() -> athenaService.getAthenaQueryString(queryId));
            if (originalQuery != null) {
                Pair<String, String> originalIds = CommonUtil.extractOriginalIdentifiers(originalQuery, tableName.getSchemaName(), tableName.getTableName());
                if (originalIds.left() != null && originalIds.right() != null) {
                    return Optional.of(originalIds);
                }
                else {
                    logger.info("Experimental Path: Could not extract valid original Base/Table IDs from query for table {}.", tableName);
                    return Optional.empty();
                }
            }
            else {
                logger.warn("Experimental Path: Could not retrieve original query string for query ID {}. Cannot extract IDs.", queryId);
                return Optional.empty();
            }
        }
        catch (InvalidRequestException | TimeoutException e) {
            logger.info("Experimental Path: Could not get original query string for query ID {} (Possibly a utility query?): {}", queryId, e.getMessage());
            return Optional.empty();
        }
        catch (Exception e) {
            logger.error("Experimental Path: Unexpected error retrieving original query string for query ID {}: {}", queryId, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Discovers field mappings dynamically by calling the Lark API.
     * (Moved from BaseMetadataHandler.generateNewFields)
     */
    private List<AthenaFieldLarkBaseMapping> discoverTableFields(String larkBaseId, String larkTableId)
    {
        List<AthenaFieldLarkBaseMapping> fieldMappings = new ArrayList<>();
        logger.info("Experimental Path: Discovering fields for {}-{}", larkBaseId, larkTableId);
        try {
            List<ListFieldResponse.FieldItem> fields = invoker.invoke(() -> larkBaseService.getTableFields(larkBaseId, larkTableId));
            for (ListFieldResponse.FieldItem field : fields) {
                String larkFieldName = field.getFieldName();
                if (larkFieldName != null && !larkFieldName.trim().isEmpty()) {
                    String prestoFieldName = CommonUtil.sanitizeGlueRelatedName(larkFieldName);
                    UITypeEnum childUIType = field.getFormulaGlueCatalogUITypeEnum();

                    if (childUIType.equals(UITypeEnum.LOOKUP)) {
                        try {
                            Pair<String, String> lookupId = field.getTargetFieldAndTableForLookup();
                            String newTableId = lookupId.right();
                            String newFieldId = lookupId.left();
                            childUIType = larkBaseService.getLookupType(larkBaseId, newTableId, newFieldId);
                        }
                        catch (Exception e) {
                            logger.warn("Experimental Path: Skipping field {} due to error getting lookup type: {}", field.getFieldName(), e.getMessage());
                            continue;
                        }
                    }

                    NestedUIType nestedUIType = new NestedUIType(field.getUIType(), childUIType);
                    fieldMappings.add(new AthenaFieldLarkBaseMapping(prestoFieldName, larkFieldName, nestedUIType));
                }
            }
        }
        catch (Exception e) {
            logger.error("Experimental Path: Failed to discover fields for table {}-{}: {}. Returning empty field list.", larkBaseId, larkTableId, e.getMessage(), e);
        }
        logger.info("Experimental Path: Discovered {} fields for {}-{}", fieldMappings.size(), larkBaseId, larkTableId);
        return fieldMappings;
    }
}
