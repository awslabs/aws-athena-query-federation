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
package com.amazonaws.athena.connectors.lark.base.resolver;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.AthenaLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.LarkDatabaseRecord;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.TableDirectInitialized;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.model.response.ListAllTableResponse;
import com.amazonaws.athena.connectors.lark.base.model.response.ListFieldResponse;
import com.amazonaws.athena.connectors.lark.base.service.EnvVarService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import com.amazonaws.athena.connectors.lark.base.service.LarkDriveService;
import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Resolves Lark table mappings based on configurations provided via environment variables
 * (LARK_BASE_SOURCES_ENV_VAR, LARK_DRIVE_SOURCES_ENV_VAR).
 * It interacts with LarkBaseService and LarkDriveService to discover the actual
 * databases, tables, and fields.
 */
public class LarkBaseTableResolver
{
    private static final Logger logger = LoggerFactory.getLogger(LarkBaseTableResolver.class);

    private final EnvVarService envVarService;
    private final LarkBaseService larkBaseService;
    private final LarkDriveService larkDriveService;
    private final ThrottlingInvoker invoker;

    public LarkBaseTableResolver(EnvVarService envVarService,
                                 LarkBaseService larkBaseService,
                                 LarkDriveService larkDriveService,
                                 ThrottlingInvoker invoker)
    {
        this.envVarService = requireNonNull(envVarService, "envVarService cannot be null");
        this.larkBaseService = requireNonNull(larkBaseService, "larkBaseService cannot be null");
        this.larkDriveService = requireNonNull(larkDriveService, "larkDriveService cannot be null");
        this.invoker = requireNonNull(invoker, "invoker cannot be null");
    }

    /**
     * Resolves and discovers all table mappings from configured Lark Base and Drive sources.
     *
     * @return A list of initialized table details including database, table, and columns.
     */
    public List<TableDirectInitialized> resolveTables()
    {
        List<TableDirectInitialized> resolvedMappings = new ArrayList<>();
        // Shared across every source/location processed below: two Lark Bases discovered from
        // different sources (or different locations of the same source) can still collide on the
        // same Athena schema name after sanitization, so this must be tracked globally, not reset
        // per-source - LarkSourceMetadataProvider.findMapping() matches by this name across the
        // whole combined resolvedMappings list.
        Set<String> seenDatabaseNames = new HashSet<>();

        if (envVarService.isActivateLarkBaseSource()) {
            List<TableDirectInitialized> larkBaseTables = resolveFromLarkBaseSource(seenDatabaseNames);
            logger.info("Lark Base source path: Resolved {} table mappings.", larkBaseTables.size());
            resolvedMappings.addAll(larkBaseTables);
        }
        else {
            logger.info("Lark Base source path: Deactivated.");
        }

        if (envVarService.isActivateLarkDriveSource()) {
            List<TableDirectInitialized> larkDriveTables = resolveFromLarkDriveSource(seenDatabaseNames);
            logger.info("Lark Drive source path: Resolved {} table mappings.", larkDriveTables.size());
            resolvedMappings.addAll(larkDriveTables);
        }
        else {
            logger.info("Lark Drive source path: Deactivated.");
        }

        logger.info("Table resolution complete. Total resolved mappings: {}", resolvedMappings.size());
        return resolvedMappings;
    }

    private List<TableDirectInitialized> resolveFromLarkBaseSource(Set<String> seenDatabaseNames)
    {
        Map<String, Set<String>> metadataTableLocations = CommonUtil.constructLarkBaseMappingFromLarkBaseSource(
                envVarService.getLarkBaseSources()
        );
        logger.info("Found {} metadata table location(s) configured for Lark Base Source.", metadataTableLocations.size());

        List<TableDirectInitialized> resolvedTables = new ArrayList<>();
        for (Map.Entry<String, Set<String>> locationEntry : metadataTableLocations.entrySet()) {
            String metadataBaseId = locationEntry.getKey();
            for (String metadataTableId : locationEntry.getValue()) {
                try {
                    List<LarkDatabaseRecord> targetDatabaseRecords = invoker.invoke(() -> larkBaseService.getDatabaseRecords(metadataBaseId, metadataTableId));
                    resolvedTables.addAll(processTargetDatabaseRecords(targetDatabaseRecords, "Base:" + metadataBaseId + "/" + metadataTableId, seenDatabaseNames));
                }
                catch (TimeoutException e) {
                    logger.error("Timeout while reading records from metadata table {}-{}: {}", metadataBaseId, metadataTableId, e.getMessage(), e);
                }
                catch (Exception e) {
                    logger.error("Failed to read or process records from Lark Base metadata table {}-{}: {}", metadataBaseId, metadataTableId, e.getMessage(), e);
                }
            }
        }
        return resolvedTables;
    }

    private List<TableDirectInitialized> resolveFromLarkDriveSource(Set<String> seenDatabaseNames)
    {
        Set<String> metadataTableLocations = new HashSet<>();
        String driveSources = envVarService.getLarkDriveSources();
        if (driveSources != null && !driveSources.trim().isEmpty()) {
            metadataTableLocations.addAll(Arrays.asList(driveSources.split(",")));
        }
        logger.info("Found {} metadata table location(s) configured for Lark Drive Source.", metadataTableLocations.size());

        List<TableDirectInitialized> resolvedTables = new ArrayList<>();
        for (String metadataTableId : metadataTableLocations) {
            if (metadataTableId == null || metadataTableId.trim().isEmpty()) {
                continue;
            }
            try {
                List<LarkDatabaseRecord> targetDatabaseRecords = invoker.invoke(() -> larkDriveService.getLarkBases(metadataTableId));
                resolvedTables.addAll(processTargetDatabaseRecords(targetDatabaseRecords, "Drive:" + metadataTableId, seenDatabaseNames));
            }
            catch (TimeoutException e) {
                logger.error("Timeout while reading records from Drive metadata table {}: {}", metadataTableId, e.getMessage(), e);
            }
            catch (Exception e) {
                logger.error("Failed to read or process records from Lark Drive metadata table {}: {}", metadataTableId, e.getMessage(), e);
            }
        }
        return resolvedTables;
    }

    private List<TableDirectInitialized> processTargetDatabaseRecords(List<LarkDatabaseRecord> targetDatabaseRecords, String sourceDescription, Set<String> seenDatabaseNames) throws TimeoutException
    {
        List<TableDirectInitialized> discoveredTables = new ArrayList<>();

        for (LarkDatabaseRecord record : targetDatabaseRecords) {
            String larkBaseId = record.id();
            // Two distinct Lark Bases can sanitize (or, for the unsanitized metadata-table source,
            // literally coincide) to the same Athena schema name. Without disambiguation, the second
            // one would be permanently shadowed by LarkSourceMetadataProvider.findMapping()'s
            // findFirst() - it would never be reachable via Athena, with no error at all.
            String prestoDbName = CommonUtil.sanitizeGlueRelatedNameWithDedup(record.name(), larkBaseId, seenDatabaseNames);

            logger.info("Processing database record from {}: PrestoName='{}', LarkBaseID='{}'", sourceDescription, prestoDbName, larkBaseId);

            if (isValidIdentifier(prestoDbName) && isValidIdentifier(larkBaseId)) {
                AthenaLarkBaseMapping dbMapping = new AthenaLarkBaseMapping(prestoDbName, larkBaseId);
                Set<String> seenTableNames = new HashSet<>();
                try {
                    List<ListAllTableResponse.BaseItem> tablesFromLark = invoker.invoke(() -> larkBaseService.listTables(larkBaseId));
                    for (ListAllTableResponse.BaseItem table : tablesFromLark) {
                        String larkTableId = table.getTableId();
                        // Same collision risk as database names, but scoped to tables within this one
                        // base (two tables in different bases sharing a name is fine - different schema).
                        String prestoTableName = CommonUtil.sanitizeGlueRelatedNameWithDedup(table.getName(), larkTableId, seenTableNames);

                        if (isValidIdentifier(prestoTableName) && isValidIdentifier(larkTableId)) {
                            AthenaLarkBaseMapping tableMapping = new AthenaLarkBaseMapping(prestoTableName, larkTableId);
                            List<AthenaFieldLarkBaseMapping> fieldMappings = discoverTableFields(larkBaseId, larkTableId);
                            logger.info("Discovered table from {}: PrestoName='{}', LarkBaseID='{}'. Found {} fields.",
                                    sourceDescription, prestoTableName, larkTableId, fieldMappings.size());
                            discoveredTables.add(new TableDirectInitialized(dbMapping, tableMapping, fieldMappings));
                        }
                        else {
                            logger.warn("Skipping invalid table definition from source '{}' in base '{}': PrestoName='{}', LarkTableID='{}'",
                                    sourceDescription, larkBaseId, prestoTableName, larkTableId);
                        }
                    }
                    logger.info("Successfully processed database record from {}: PrestoName='{}', LarkBaseID='{}'. Found {} tables.",
                            sourceDescription, prestoDbName, larkBaseId, tablesFromLark.size());
                }
                catch (TimeoutException e) {
                    logger.error("Timeout while listing tables or fields for base '{}' (Presto name '{}') discovered from {}: {}",
                            larkBaseId, prestoDbName, sourceDescription, e.getMessage());
                    throw e;
                }
                catch (Exception e) {
                    logger.error("Failed to list tables or fields for base '{}' (Presto name '{}') discovered from {}: {}",
                            larkBaseId, prestoDbName, sourceDescription, e.getMessage(), e);
                }
            }
            else {
                logger.warn("Skipping invalid database record from source {}: PrestoName='{}', LarkBaseID='{}'", sourceDescription, prestoDbName, larkBaseId);
            }
        }
        return discoveredTables;
    }

    private List<AthenaFieldLarkBaseMapping> discoverTableFields(String larkBaseId, String larkTableId) throws TimeoutException
    {
        List<AthenaFieldLarkBaseMapping> fieldMappings = new ArrayList<>();
        Set<String> seenFieldNames = new HashSet<>();
        try {
            List<ListFieldResponse.FieldItem> fields = invoker.invoke(() -> larkBaseService.getTableFields(larkBaseId, larkTableId));
            for (ListFieldResponse.FieldItem field : fields) {
                String larkFieldName = field.getFieldName();
                if (isValidIdentifier(larkFieldName)) {
                    String prestoFieldName = CommonUtil.sanitizeGlueRelatedNameWithDedup(larkFieldName, field.getFieldId(), seenFieldNames);
                    // NOTE: getFormulaGlueCatalogUITypeEnum() only returns a meaningful (non-UNKNOWN) value when
                    // the field's own UI type is FORMULA, while getTargetFieldAndTableForLookup() only returns a
                    // usable target when the field's own UI type is LOOKUP. A field cannot be both at once, so
                    // checking the former for LOOKUP and then calling the latter was dead code - a genuine LOOKUP
                    // field's child type could never actually be resolved this way. Branch on the field's own UI
                    // type instead, matching the (correct) logic in glue-lark-base-crawler's BaseLarkBaseCrawlerHandler.
                    UITypeEnum childUIType;
                    if (field.getUIType().equals(UITypeEnum.LOOKUP)) {
                        try {
                            Pair<String, String> lookupId = field.getTargetFieldAndTableForLookup();
                            String newTableId = lookupId.right();
                            String newFieldId = lookupId.left();
                            // Route through the invoker so a cache-miss fetch inside getLookupType gets the same
                            // rate-limit backoff as every other Lark API call here.
                            childUIType = invoker.invoke(() -> larkBaseService.getLookupType(larkBaseId, newTableId, newFieldId));
                        }
                        catch (Exception e) {
                            // Only this one field's error handling - not caught here, this would escape the
                            // whole for-loop into this method's outer catch (below), which returns whatever
                            // fieldMappings had been collected SO FAR and silently drops every field that would
                            // have come after this one in Lark's response - not just this one column. A LOOKUP
                            // whose target is malformed/permission-restricted (getTargetFieldAndTableForLookup
                            // returns Pair.of(null, null), not a thrown exception, for that case - see its
                            // javadoc) is a realistic, not contrived, way to reach this: the resulting
                            // getLookupType(baseId, null, null) call fails against Lark's API. Matches the
                            // per-field isolation ExperimentalMetadataProvider already has for the identical case.
                            logger.warn("Skipping field {} in table {}-{} due to error getting lookup type: {}",
                                    larkFieldName, larkBaseId, larkTableId, e.getMessage());
                            continue;
                        }
                    }
                    else {
                        childUIType = field.getFormulaGlueCatalogUITypeEnum();
                    }

                    NestedUIType nestedUIType = new NestedUIType(field.getUIType(), childUIType);
                    fieldMappings.add(new AthenaFieldLarkBaseMapping(prestoFieldName, larkFieldName, nestedUIType));
                }
                else {
                    logger.warn("Skipping field with invalid name '{}' in table {}-{}", larkFieldName, larkBaseId, larkTableId);
                }
            }
        }
        catch (TimeoutException e) {
            logger.error("Timeout discovering fields for table {}-{}: {}", larkBaseId, larkTableId, e.getMessage());
            throw e;
        }
        catch (Exception e) {
            logger.error("Failed to discover fields for table {}-{}: {}. Returning empty field list.", larkBaseId, larkTableId, e.getMessage(), e);
        }
        return fieldMappings;
    }

    private boolean isValidIdentifier(String identifier)
    {
        return identifier != null && !identifier.trim().isEmpty();
    }
}
