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
package com.amazonaws.athena.connectors.lark.base.util;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_BASE_ID;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_RECORD_ID;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_TABLE_ID;

public final class CommonUtil
{
    private static final Logger logger = LoggerFactory.getLogger(CommonUtil.class);

    private CommonUtil()
    {
        // Prevent instantiation
    }

    public static String sanitizeGlueRelatedName(String tableName)
    {
        return tableName.toLowerCase().replaceAll("[^a-zA-Z0-9$]", "_");
    }

    /**
     * Sanitizes a Lark field name like {@link #sanitizeGlueRelatedName}, but disambiguates the
     * result against names already seen in the current table. Two distinct Lark field names can
     * sanitize down to the same name (e.g. "Segment 5" and "segment 5" both become "segment_5"),
     * which would otherwise make two fields collapse into a single Athena column. Collisions are
     * disambiguated with the Lark field ID, which is always unique.
     *
     * @param fieldName Original Lark field name.
     * @param fieldId Lark field ID, used to disambiguate a name collision.
     * @param seenNames Names already assigned so far for the current table; updated with the result.
     * @return A sanitized name guaranteed unique among everything already added to {@code seenNames}.
     */
    public static String sanitizeGlueRelatedNameWithDedup(String fieldName, String fieldId, Set<String> seenNames)
    {
        String sanitized = sanitizeGlueRelatedName(fieldName);
        if (!seenNames.add(sanitized)) {
            sanitized = sanitized + "_" + sanitizeGlueRelatedName(fieldId);
            seenNames.add(sanitized);
        }
        return sanitized;
    }

    // Comment: LarkBaseId=<larkBaseId>/LarkBaseTableId=<larkTableId>/
    // LarkBaseFieldId=<larkBaseFieldId>/LarkBaseFieldName=<originalFieldName>/
    // LarkBaseFieldType=<larkBaseFieldType>
    // for larkBaseFieldType "Formula", we fill with "Formula<formula-type>"
    public static String extractFieldNameFromComment(String comment)
    {
        if (comment == null || comment.isEmpty()) {
            return null;
        }
        String[] parts = comment.split("/");
        for (String part : parts) {
            if (part.startsWith("LarkBaseFieldName=")) {
                return part.substring("LarkBaseFieldName=".length());
            }
        }
        return null;
    }

    public static NestedUIType extractFieldTypeFromComment(String comment)
    {
        if (comment == null || comment.isEmpty()) {
            return null;
        }
        String[] parts = comment.split("/");
        for (String part : parts) {
            if (part.startsWith("LarkBaseFieldType=")) {
                String rawFieldType = part.substring("LarkBaseFieldType=".length());
                // example: LarkBaseFieldType=Formula<FormulaType>
                if (rawFieldType.startsWith("Formula") || rawFieldType.startsWith("Lookup")) {
                    UITypeEnum uiType = UITypeEnum.fromString(rawFieldType.split("<")[0]);
                    return new NestedUIType(uiType, extractFormulaOrLookup(rawFieldType));
                }

                return new NestedUIType(UITypeEnum.fromString(rawFieldType), UITypeEnum.UNKNOWN);
            }
        }
        return null;
    }

    private static UITypeEnum extractFormulaOrLookup(String raw)
    {
        // example: LarkBaseFieldType=Formula<FormulaType>
        // A chained LOOKUP (one LOOKUP field pointing at another LOOKUP field) is written by the Glue Crawler
        // as a nested string, e.g. "Lookup<Lookup<Number>>" (see BaseLarkBaseCrawlerHandler.getLarkBaseOriginalColumnType,
        // which recurses when the LOOKUP target is itself a LOOKUP). Splitting on the FIRST '<' and taking
        // index [1] only ever captures the immediate next level ("Lookup" here), not the terminal type
        // ("Number") - losing the actual resolved type for any chain of depth >= 2. Extracting from the LAST
        // '<' instead always yields the innermost/terminal type, regardless of nesting depth.
        int lastOpenBracket = raw.lastIndexOf('<');
        if (lastOpenBracket == -1 || lastOpenBracket == raw.length() - 1) {
            return null;
        }

        String innermostType = raw.substring(lastOpenBracket + 1).replace(">", "");
        if (innermostType.isEmpty()) {
            return null;
        }

        return UITypeEnum.fromString(innermostType);
    }

    /**
     * Extracts the original case-sensitive Base ID and Table ID from the raw SQL query.
     * Uses regex to find identifiers, potentially quoted. It looks for the first match
     * ignoring case for the base ID (schema) and then the first match ignoring case
     * for the table ID after the base ID.
     *
     * @param originalQuery The raw SQL query string from Athena.
     * @param lowercaseSchema The schema name (Base ID) provided by Athena (lowercase).
     * @param lowercaseTable The table name (Table ID) provided by Athena (lowercase).
     * @return A Pair containing the extracted original Base ID (left) and Table ID (right),
     * or Pair.of(null, null) if not found.
     */
    public static Pair<String, String> extractOriginalIdentifiers(String originalQuery, String lowercaseSchema, String lowercaseTable)
    {
        // Regex for matching identifiers: can be quoted (") or not (alphanumeric + _)
        // Group 1: Quoted identifier
        // Group 2: Unquoted identifier
        Pattern identifierPattern = Pattern.compile("\"([^\"]+)\"|([a-zA-Z0-9$_]+)");
        Matcher matcher = identifierPattern.matcher(originalQuery);

        String foundBaseId = null;
        int baseIdEndPosition = -1;

        while (matcher.find()) {
            String potentialId = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (potentialId.equalsIgnoreCase(lowercaseSchema)) {
                foundBaseId = potentialId;
                baseIdEndPosition = matcher.end();
                break;
            }
        }

        if (foundBaseId == null) {
            return Pair.of(null, null);
        }

        if (!matcher.find(baseIdEndPosition)) {
            return Pair.of(foundBaseId, null);
        }
        do {
            String potentialId = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (potentialId.equalsIgnoreCase(lowercaseTable)) {
                return Pair.of(foundBaseId, potentialId);
            }
        } while (matcher.find());

        return Pair.of(foundBaseId, null);
    }

    public static Schema buildSchemaFromLarkFields(List<AthenaFieldLarkBaseMapping> larkFields)
    {
        return buildSchemaFromLarkFields(larkFields, false);
    }

    public static Schema buildSchemaFromLarkFields(List<AthenaFieldLarkBaseMapping> larkFields, boolean complexTypeAsJsonString)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        if (larkFields == null || larkFields.isEmpty()) {
            return schemaBuilder.build();
        }

        for (AthenaFieldLarkBaseMapping field : larkFields) {
            Field arrowField = LarkBaseTypeUtils.larkFieldToArrowField(field, complexTypeAsJsonString);

            // field.athenaName() is already sanitized (and deduplicated against name collisions) at
            // discovery time; use it as-is instead of re-sanitizing the raw Lark field name here, which
            // would throw away that deduplication and reintroduce collisions between fields whose names
            // only differ by case/punctuation (e.g. "Segment 5" and "segment 5").
            if (!field.athenaName().equals(arrowField.getName())) {
                arrowField = new Field(field.athenaName(), arrowField.getFieldType(), arrowField.getChildren());
            }

            schemaBuilder.addField(arrowField);
        }
        return schemaBuilder.build();
    }

    /**
     * Constructs a set of LarkBaseMapping objects from the provided environment variables string.
     * The string should be in the format: "<larkbaseId>:<larkTableId>,...<larkBaseId>:<larkTableId>".
     *
     * @param fromEnvVars The environment variables string.
     * @return Map<BaseId, Set<TableId>> mapping.
     */
    public static Map<String, Set<String>> constructLarkBaseMappingFromLarkBaseSource(String fromEnvVars)
    {
        if (fromEnvVars == null || fromEnvVars.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Set<String>> mapOfBaseIdToTableId = new HashMap<>();

        String[] mappings = fromEnvVars.split(",");
        for (String mapping : mappings) {
            String trimmedMapping = mapping.trim();
            if (trimmedMapping.isEmpty()) {
                continue;
            }
            String[] parts = trimmedMapping.split(":", 2);
            if (parts.length == 2) {
                String baseId = parts[0].trim();
                String tableId = parts[1].trim();

                if (baseId.isEmpty() || tableId.isEmpty()) {
                    continue;
                }

                mapOfBaseIdToTableId.computeIfAbsent(baseId, k -> new HashSet<>()).add(tableId);
            }
        }

        return mapOfBaseIdToTableId;
    }

    /**
     * Parses a "schemaName:tableName,schemaName:tableName2,..." environment variable string (used by
     * {@code WHITELIST_TABLES_ENV_VAR} / {@code BLACKLIST_TABLES_ENV_VAR}) into a schema-to-table-names map.
     * Both schema and table names are lowercased so lookups can be done case-insensitively, matching how
     * Athena/Glue names are already normalized elsewhere in this connector.
     *
     * @param fromEnvVar The environment variable string.
     * @return Map of lowercased schema name to a set of lowercased table names, or an empty map if unset.
     */
    public static Map<String, Set<String>> parseSchemaTableGrouping(String fromEnvVar)
    {
        if (fromEnvVar == null || fromEnvVar.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Set<String>> schemaToTableNames = new HashMap<>();

        String[] entries = fromEnvVar.split(",");
        for (String entry : entries) {
            String trimmedEntry = entry.trim();
            if (trimmedEntry.isEmpty()) {
                continue;
            }
            String[] parts = trimmedEntry.split(":", 2);
            if (parts.length == 2) {
                String schemaName = parts[0].trim().toLowerCase();
                String tableName = parts[1].trim().toLowerCase();

                if (schemaName.isEmpty() || tableName.isEmpty()) {
                    continue;
                }

                schemaToTableNames.computeIfAbsent(schemaName, k -> new HashSet<>()).add(tableName);
            }
        }

        return schemaToTableNames;
    }

    /**
     * Decides whether a table should be visible/queryable given the connector's configured whitelist and
     * blacklist. The blacklist always wins: a table listed there is blocked even if it also matches the
     * whitelist. The whitelist only restricts schemas that have at least one entry in it - a schema with no
     * whitelist entries is unrestricted by the whitelist (only the blacklist, if any, applies to it).
     *
     * @param schemaName The schema (database) name, matched case-insensitively.
     * @param tableName The table name, matched case-insensitively.
     * @param whitelistTables Schema-to-table-names map from {@link #parseSchemaTableGrouping}, or empty if unset.
     * @param blacklistTables Schema-to-table-names map from {@link #parseSchemaTableGrouping}, or empty if unset.
     * @return true if the table should be visible/queryable.
     */
    public static boolean isTableAccessAllowed(String schemaName, String tableName,
            Map<String, Set<String>> whitelistTables, Map<String, Set<String>> blacklistTables)
    {
        String normalizedSchema = schemaName == null ? "" : schemaName.toLowerCase();
        String normalizedTable = tableName == null ? "" : tableName.toLowerCase();

        Set<String> blacklistedTables = blacklistTables.get(normalizedSchema);
        if (blacklistedTables != null && blacklistedTables.contains(normalizedTable)) {
            return false;
        }

        Set<String> whitelistedTables = whitelistTables.get(normalizedSchema);
        if (whitelistedTables != null && !whitelistedTables.isEmpty()) {
            return whitelistedTables.contains(normalizedTable);
        }

        return true;
    }

    /**
     * Enhances the original table schema by adding connector-specific reserved fields.
     * These fields provide additional context about the record's origin within Lark Base.
     * (Moved from BaseMetadataHandler.generateNewTableResponse)
     *
     * @param originalSchema The original Schema object from Glue or dynamically built.
     * @return A new Schema object containing the original fields plus the added reserved fields.
     */
    public static Schema addReservedFields(Schema originalSchema)
    {
        List<Field> originalFields = originalSchema.getFields();
        logger.info("addReservedFields: Original fields: {}", originalFields);
        for (Field field : originalFields) {
            logger.info("addReservedFields: Field: {} - Type: {}", field.getName(), field.getType());
        }
        Set<String> existingFieldNamesLower = originalFields.stream()
                .map(f -> f.getName().toLowerCase())
                .collect(Collectors.toSet());

        List<Field> finalFields = new ArrayList<>(originalFields);
        Map<String, String> metadata = Collections.singletonMap("comment", "Reserved column from Lark Base Connector");

        if (!existingFieldNamesLower.contains(RESERVED_RECORD_ID.toLowerCase())) {
            finalFields.add(new Field(
                    RESERVED_RECORD_ID,
                    new FieldType(true, Types.MinorType.VARCHAR.getType(), null, metadata),
                    Collections.emptyList()));
        }

        if (!existingFieldNamesLower.contains(RESERVED_TABLE_ID.toLowerCase())) {
            finalFields.add(new Field(
                    RESERVED_TABLE_ID,
                    new FieldType(true, Types.MinorType.VARCHAR.getType(), null, metadata),
                    Collections.emptyList()));
        }

        if (!existingFieldNamesLower.contains(RESERVED_BASE_ID.toLowerCase())) {
            finalFields.add(new Field(
                    RESERVED_BASE_ID,
                    new FieldType(true, Types.MinorType.VARCHAR.getType(), null, metadata),
                    Collections.emptyList()));
        }

        if (finalFields.size() > originalFields.size()) {
            return new Schema(finalFields, originalSchema.getCustomMetadata());
        }
        else {
            return originalSchema;
        }
    }
}
