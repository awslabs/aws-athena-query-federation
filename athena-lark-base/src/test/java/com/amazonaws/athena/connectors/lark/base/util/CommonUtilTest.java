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
package com.amazonaws.athena.connectors.lark.base.util;

import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.utils.Pair;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class CommonUtilTest {

    @Test
    void testSanitizeGlueRelatedName_LowerCase() {
        // Arrange & Act
        String result = CommonUtil.sanitizeGlueRelatedName("MyTableName");

        // Assert
        assertThat(result).isEqualTo("mytablename");
    }

    @Test
    void testSanitizeGlueRelatedName_SpecialCharacters() {
        // Arrange & Act
        String result = CommonUtil.sanitizeGlueRelatedName("Test@Table#123!");

        // Assert
        assertThat(result).isEqualTo("test_table_123_");
    }

    @Test
    void testSanitizeGlueRelatedName_Spaces() {
        // Arrange & Act
        String result = CommonUtil.sanitizeGlueRelatedName("My Table Name");

        // Assert
        assertThat(result).isEqualTo("my_table_name");
    }

    @Test
    void testSanitizeGlueRelatedName_DollarSign() {
        // Arrange & Act
        String result = CommonUtil.sanitizeGlueRelatedName("Table$Name");

        // Assert - dollar sign is allowed
        assertThat(result).isEqualTo("table$name");
    }

    @Test
    void testSanitizeGlueRelatedName_Numbers() {
        // Arrange & Act
        String result = CommonUtil.sanitizeGlueRelatedName("Table123");

        // Assert
        assertThat(result).isEqualTo("table123");
    }

    @Test
    void testExtractFieldNameFromComment_Valid() {
        // Arrange
        String comment = "LarkBaseId=base123/LarkBaseTableId=tbl456/LarkBaseFieldId=fld789/" +
                "LarkBaseFieldName=My Field/LarkBaseFieldType=Text";

        // Act
        String result = CommonUtil.extractFieldNameFromComment(comment);

        // Assert
        assertThat(result).isEqualTo("My Field");
    }

    @Test
    void testExtractFieldNameFromComment_Null() {
        // Arrange & Act
        String result = CommonUtil.extractFieldNameFromComment(null);

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void testExtractFieldNameFromComment_Empty() {
        // Arrange & Act
        String result = CommonUtil.extractFieldNameFromComment("");

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void testExtractFieldNameFromComment_Missing() {
        // Arrange
        String comment = "LarkBaseId=base123/LarkBaseTableId=tbl456";

        // Act
        String result = CommonUtil.extractFieldNameFromComment(comment);

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void testExtractFieldTypeFromComment_SimpleType() {
        // Arrange
        String comment = "LarkBaseId=base123/LarkBaseFieldType=Text/LarkBaseFieldName=Name";

        // Act
        NestedUIType result = CommonUtil.extractFieldTypeFromComment(comment);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.uiType()).isEqualTo(UITypeEnum.TEXT);
        assertThat(result.childType()).isEqualTo(UITypeEnum.UNKNOWN);
    }

    @Test
    void testExtractFieldTypeFromComment_FormulaType() {
        // Arrange
        String comment = "LarkBaseId=base123/LarkBaseFieldType=Formula<Number>/LarkBaseFieldName=Calc";

        // Act
        NestedUIType result = CommonUtil.extractFieldTypeFromComment(comment);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.uiType()).isEqualTo(UITypeEnum.FORMULA);
        assertThat(result.childType()).isEqualTo(UITypeEnum.NUMBER);
    }

    @Test
    void testExtractFieldTypeFromComment_LookupType() {
        // Arrange
        String comment = "LarkBaseFieldType=Lookup<Text>/LarkBaseFieldName=LinkedField";

        // Act
        NestedUIType result = CommonUtil.extractFieldTypeFromComment(comment);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.uiType()).isEqualTo(UITypeEnum.LOOKUP);
        assertThat(result.childType()).isEqualTo(UITypeEnum.TEXT);
    }

    @Test
    void testExtractFieldTypeFromComment_ChainedLookupType_TwoLevels() {
        // A LOOKUP field whose target is itself a LOOKUP field is written by the Glue Crawler
        // (BaseLarkBaseCrawlerHandler.getLarkBaseOriginalColumnType) as a nested string. The terminal type
        // (Number) must be extracted, not the intermediate "Lookup" level.
        String comment = "LarkBaseFieldType=Lookup<Lookup<Number>>/LarkBaseFieldName=ChainedLookup";

        NestedUIType result = CommonUtil.extractFieldTypeFromComment(comment);

        assertThat(result).isNotNull();
        assertThat(result.uiType()).isEqualTo(UITypeEnum.LOOKUP);
        assertThat(result.childType()).isEqualTo(UITypeEnum.NUMBER);
    }

    @Test
    void testExtractFieldTypeFromComment_ChainedLookupType_ThreeLevels() {
        String comment = "LarkBaseFieldType=Lookup<Lookup<Lookup<DateTime>>>/LarkBaseFieldName=DeepChainedLookup";

        NestedUIType result = CommonUtil.extractFieldTypeFromComment(comment);

        assertThat(result).isNotNull();
        assertThat(result.uiType()).isEqualTo(UITypeEnum.LOOKUP);
        assertThat(result.childType()).isEqualTo(UITypeEnum.DATE_TIME);
    }

    @Test
    void testExtractFieldTypeFromComment_LookupType_UnresolvedTargetFallsBackToUnknown() {
        // The crawler writes "Lookup<NULL>" when the lookup target couldn't be resolved
        // (see BaseLarkBaseCrawlerHandler.getLarkBaseOriginalColumnType's NULL fallback branches).
        String comment = "LarkBaseFieldType=Lookup<NULL>/LarkBaseFieldName=BrokenLookup";

        NestedUIType result = CommonUtil.extractFieldTypeFromComment(comment);

        assertThat(result).isNotNull();
        assertThat(result.uiType()).isEqualTo(UITypeEnum.LOOKUP);
        assertThat(result.childType()).isEqualTo(UITypeEnum.UNKNOWN);
    }

    @Test
    void testExtractFieldTypeFromComment_Null() {
        // Arrange & Act
        NestedUIType result = CommonUtil.extractFieldTypeFromComment(null);

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void testExtractFieldTypeFromComment_Empty() {
        // Arrange & Act
        NestedUIType result = CommonUtil.extractFieldTypeFromComment("");

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void testExtractFieldTypeFromComment_Missing() {
        // Arrange
        String comment = "LarkBaseId=base123/LarkBaseFieldName=Name";

        // Act
        NestedUIType result = CommonUtil.extractFieldTypeFromComment(comment);

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void testExtractOriginalIdentifiers_BothFound() {
        // Arrange
        String query = "SELECT * FROM MyBase.MyTable WHERE id = 1";

        // Act
        Pair<String, String> result = CommonUtil.extractOriginalIdentifiers(query, "mybase", "mytable");

        // Assert
        assertThat(result.left()).isEqualTo("MyBase");
        assertThat(result.right()).isEqualTo("MyTable");
    }

    @Test
    void testExtractOriginalIdentifiers_QuotedIdentifiers() {
        // Arrange
        String query = "SELECT * FROM \"MyBase\".\"MyTable\" WHERE id = 1";

        // Act
        Pair<String, String> result = CommonUtil.extractOriginalIdentifiers(query, "mybase", "mytable");

        // Assert
        assertThat(result.left()).isEqualTo("MyBase");
        assertThat(result.right()).isEqualTo("MyTable");
    }

    @Test
    void testExtractOriginalIdentifiers_BaseNotFound() {
        // Arrange
        String query = "SELECT * FROM SomeOtherBase.Table1";

        // Act & Assert - Pair.of(null, null) throws exception
        try {
            Pair<String, String> result = CommonUtil.extractOriginalIdentifiers(query, "mybase", "table1");
            assertThat(result.left()).isNull();
            assertThat(result.right()).isNull();
        } catch (NullPointerException e) {
            assertThat(e.getMessage()).contains("must not be null");
        }
    }

    @Test
    void testExtractOriginalIdentifiers_TableNotFound() {
        // Arrange
        String query = "SELECT * FROM MyBase.SomeOtherTable";

        // Act & Assert - Pair.of(base, null) throws exception
        try {
            Pair<String, String> result = CommonUtil.extractOriginalIdentifiers(query, "mybase", "mytable");
            assertThat(result.left()).isEqualTo("MyBase");
            assertThat(result.right()).isNull();
        } catch (NullPointerException e) {
            assertThat(e.getMessage()).contains("must not be null");
        }
    }

    @Test
    void testConstructLarkBaseMappingFromLarkBaseSource_Valid() {
        // Arrange
        String envVars = "base1:table1,base1:table2,base2:table3";

        // Act
        Map<String, Set<String>> result = CommonUtil.constructLarkBaseMappingFromLarkBaseSource(envVars);

        // Assert
        assertThat(result).hasSize(2);
        assertThat(result.get("base1")).containsExactlyInAnyOrder("table1", "table2");
        assertThat(result.get("base2")).containsExactlyInAnyOrder("table3");
    }

    @Test
    void testConstructLarkBaseMappingFromLarkBaseSource_Null() {
        // Arrange & Act
        Map<String, Set<String>> result = CommonUtil.constructLarkBaseMappingFromLarkBaseSource(null);

        // Assert
        assertThat(result).isEmpty();
    }

    @Test
    void testConstructLarkBaseMappingFromLarkBaseSource_Empty() {
        // Arrange & Act
        Map<String, Set<String>> result = CommonUtil.constructLarkBaseMappingFromLarkBaseSource("");

        // Assert
        assertThat(result).isEmpty();
    }

    @Test
    void testConstructLarkBaseMappingFromLarkBaseSource_WithSpaces() {
        // Arrange
        String envVars = " base1 : table1 , base2 : table2 ";

        // Act
        Map<String, Set<String>> result = CommonUtil.constructLarkBaseMappingFromLarkBaseSource(envVars);

        // Assert
        assertThat(result).hasSize(2);
        assertThat(result.get("base1")).containsExactly("table1");
        assertThat(result.get("base2")).containsExactly("table2");
    }

    @Test
    void testConstructLarkBaseMappingFromLarkBaseSource_InvalidFormat() {
        // Arrange
        String envVars = "base1:table1,invalid_entry,base2:table2";

        // Act
        Map<String, Set<String>> result = CommonUtil.constructLarkBaseMappingFromLarkBaseSource(envVars);

        // Assert - should skip invalid entry
        assertThat(result).hasSize(2);
        assertThat(result.get("base1")).containsExactly("table1");
        assertThat(result.get("base2")).containsExactly("table2");
    }

    @Test
    void testConstructLarkBaseMappingFromLarkBaseSource_EmptyParts() {
        // Arrange
        String envVars = ":table1,base2:,base3:table3";

        // Act
        Map<String, Set<String>> result = CommonUtil.constructLarkBaseMappingFromLarkBaseSource(envVars);

        // Assert - should skip entries with empty parts
        assertThat(result).hasSize(1);
        assertThat(result.get("base3")).containsExactly("table3");
    }

    @Test
    void testConstructLarkBaseMappingFromLarkBaseSource_EmptyMappings() {
        // Arrange
        String envVars = ",,";

        // Act
        Map<String, Set<String>> result = CommonUtil.constructLarkBaseMappingFromLarkBaseSource(envVars);

        // Assert
        assertThat(result).isEmpty();
    }

    @Test
    void testParseSchemaTableGrouping_Valid() {
        String envVar = "schemaA:table1,schemaA:table2,schemaB:table3";

        Map<String, Set<String>> result = CommonUtil.parseSchemaTableGrouping(envVar);

        assertThat(result).hasSize(2);
        assertThat(result.get("schemaa")).containsExactlyInAnyOrder("table1", "table2");
        assertThat(result.get("schemab")).containsExactlyInAnyOrder("table3");
    }

    @Test
    void testParseSchemaTableGrouping_LowercasesForCaseInsensitiveMatching() {
        String envVar = "SchemaA:TableOne";

        Map<String, Set<String>> result = CommonUtil.parseSchemaTableGrouping(envVar);

        assertThat(result).containsKey("schemaa");
        assertThat(result.get("schemaa")).containsExactly("tableone");
    }

    @Test
    void testParseSchemaTableGrouping_Null() {
        assertThat(CommonUtil.parseSchemaTableGrouping(null)).isEmpty();
    }

    @Test
    void testParseSchemaTableGrouping_Empty() {
        assertThat(CommonUtil.parseSchemaTableGrouping("")).isEmpty();
    }

    @Test
    void testParseSchemaTableGrouping_InvalidEntriesSkipped() {
        String envVar = "schemaA:table1,invalid_entry,:table2,schemaB:";

        Map<String, Set<String>> result = CommonUtil.parseSchemaTableGrouping(envVar);

        assertThat(result).hasSize(1);
        assertThat(result.get("schemaa")).containsExactly("table1");
    }

    @Test
    void testIsTableAccessAllowed_NoWhitelistOrBlacklist_Allowed() {
        boolean result = CommonUtil.isTableAccessAllowed("schemaa", "table1", Collections.emptyMap(), Collections.emptyMap());

        assertThat(result).isTrue();
    }

    @Test
    void testIsTableAccessAllowed_SchemaNotInWhitelist_Unrestricted() {
        // Only schemaB is whitelisted; schemaA has no entries, so it's unrestricted by the whitelist.
        Map<String, Set<String>> whitelist = Map.of("schemab", Set.of("table9"));

        boolean result = CommonUtil.isTableAccessAllowed("schemaa", "table1", whitelist, Collections.emptyMap());

        assertThat(result).isTrue();
    }

    @Test
    void testIsTableAccessAllowed_TableInWhitelistedSchemaAndInList_Allowed() {
        Map<String, Set<String>> whitelist = Map.of("schemaa", Set.of("table1", "table2"));

        boolean result = CommonUtil.isTableAccessAllowed("schemaa", "table1", whitelist, Collections.emptyMap());

        assertThat(result).isTrue();
    }

    @Test
    void testIsTableAccessAllowed_TableInWhitelistedSchemaButNotInList_Blocked() {
        Map<String, Set<String>> whitelist = Map.of("schemaa", Set.of("table1", "table2"));

        boolean result = CommonUtil.isTableAccessAllowed("schemaa", "table3", whitelist, Collections.emptyMap());

        assertThat(result).isFalse();
    }

    @Test
    void testIsTableAccessAllowed_TableBlacklisted_Blocked() {
        Map<String, Set<String>> blacklist = Map.of("schemaa", Set.of("table1"));

        boolean result = CommonUtil.isTableAccessAllowed("schemaa", "table1", Collections.emptyMap(), blacklist);

        assertThat(result).isFalse();
    }

    @Test
    void testIsTableAccessAllowed_BlacklistWinsOverWhitelist() {
        // A table that's both whitelisted and blacklisted must be blocked - deny wins.
        Map<String, Set<String>> whitelist = Map.of("schemaa", Set.of("table1"));
        Map<String, Set<String>> blacklist = Map.of("schemaa", Set.of("table1"));

        boolean result = CommonUtil.isTableAccessAllowed("schemaa", "table1", whitelist, blacklist);

        assertThat(result).isFalse();
    }

    @Test
    void testIsTableAccessAllowed_CaseInsensitiveMatching() {
        Map<String, Set<String>> whitelist = Map.of("schemaa", Set.of("table1"));

        boolean result = CommonUtil.isTableAccessAllowed("SchemaA", "Table1", whitelist, Collections.emptyMap());

        assertThat(result).isTrue();
    }

    @Test
    void testAddReservedFields_NoExisting() {
        // Arrange
        Schema originalSchema = new Schema(Collections.emptyList());

        // Act
        Schema result = CommonUtil.addReservedFields(originalSchema);

        // Assert
        assertThat(result.getFields()).hasSize(3);
        assertThat(result.getFields()).extracting(Field::getName)
                .containsExactlyInAnyOrder("$reserved_record_id", "$reserved_table_id", "$reserved_base_id");
    }

    @Test
    void testAddReservedFields_WithExistingField() {
        // Arrange
        Field existingField = Field.nullable("my_field", org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE);
        Schema originalSchema = new Schema(List.of(existingField));

        // Act
        Schema result = CommonUtil.addReservedFields(originalSchema);

        // Assert
        assertThat(result.getFields()).hasSize(4);
        assertThat(result.getFields().get(0).getName()).isEqualTo("my_field");
    }

    @Test
    void testAddReservedFields_AlreadyHasReservedField() {
        // Arrange
        Field reservedField = Field.nullable("$reserved_record_id", org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE);
        Schema originalSchema = new Schema(List.of(reservedField));

        // Act
        Schema result = CommonUtil.addReservedFields(originalSchema);

        // Assert - should not add duplicate
        assertThat(result.getFields()).hasSize(3); // Original + 2 missing reserved fields
    }

    @Test
    void testAddReservedFields_CaseInsensitive() {
        // Arrange
        Field upperCaseField = Field.nullable("$RESERVED_RECORD_ID", org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE);
        Schema originalSchema = new Schema(List.of(upperCaseField));

        // Act
        Schema result = CommonUtil.addReservedFields(originalSchema);

        // Assert - should not add duplicate (case insensitive check)
        assertThat(result.getFields()).hasSize(3);
    }

    @Test
    void testAddReservedFields_AllReservedFieldsExist() {
        // Arrange
        List<Field> existingFields = List.of(
                Field.nullable("$reserved_record_id", org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE),
                Field.nullable("$reserved_table_id", org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE),
                Field.nullable("$reserved_base_id", org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE)
        );
        Schema originalSchema = new Schema(existingFields);

        // Act
        Schema result = CommonUtil.addReservedFields(originalSchema);

        // Assert - should return original schema
        assertThat(result).isSameAs(originalSchema);
        assertThat(result.getFields()).hasSize(3);
    }

    @Test
    void testAddReservedFields_PreservesMetadata() {
        // Arrange
        Map<String, String> customMetadata = Map.of("key", "value");
        Schema originalSchema = new Schema(Collections.emptyList(), customMetadata);

        // Act
        Schema result = CommonUtil.addReservedFields(originalSchema);

        // Assert
        assertThat(result.getCustomMetadata()).isEqualTo(customMetadata);
    }

    @Test
    void testSanitizeGlueRelatedNameWithDedup_noCollision_returnsPlainSanitizedName() {
        Set<String> seenNames = new HashSet<>();

        String result = CommonUtil.sanitizeGlueRelatedNameWithDedup("Segment 5", "fldxdVXbHa", seenNames);

        assertThat(result).isEqualTo("segment_5");
        assertThat(seenNames).containsExactly("segment_5");
    }

    @Test
    void testSanitizeGlueRelatedNameWithDedup_collision_suffixesWithFieldId() {
        // Reproduces a real production case: two distinct Lark fields named "segment 5" and "Segment 5"
        // (in different grouped columns, "Payable" vs "Receivable") both sanitize to "segment_5".
        Set<String> seenNames = new HashSet<>();

        String first = CommonUtil.sanitizeGlueRelatedNameWithDedup("segment 5", "fldxdVXbHa", seenNames);
        String second = CommonUtil.sanitizeGlueRelatedNameWithDedup("Segment 5", "fldZrayo2s", seenNames);

        assertThat(first).isEqualTo("segment_5");
        assertThat(second).isEqualTo("segment_5_fldzrayo2s");
        assertThat(second).isNotEqualTo(first);
    }

    @Test
    void testBuildSchemaFromLarkFields_usesAlreadyDedupedAthenaName() {
        // field.athenaName() is already sanitized and deduplicated at discovery time. This must be
        // trusted as-is; re-sanitizing the raw larkBaseFieldName here would reintroduce the exact
        // collision that discovery already resolved.
        AthenaFieldLarkBaseMapping first = new AthenaFieldLarkBaseMapping(
                "segment_5", "segment 5", new NestedUIType(UITypeEnum.TEXT, null));
        AthenaFieldLarkBaseMapping second = new AthenaFieldLarkBaseMapping(
                "segment_5_fldzrayo2s", "Segment 5", new NestedUIType(UITypeEnum.TEXT, null));

        Schema schema = CommonUtil.buildSchemaFromLarkFields(List.of(first, second));

        List<String> fieldNames = schema.getFields().stream().map(Field::getName).toList();
        assertThat(fieldNames).containsExactlyInAnyOrder("segment_5", "segment_5_fldzrayo2s");
    }

    @Test
    void testBuildSchemaFromLarkFields_complexTypeAsJsonString_collapsesListStructColumnsToVarchar() {
        AthenaFieldLarkBaseMapping textField = new AthenaFieldLarkBaseMapping(
                "field_text", "Field Text", new NestedUIType(UITypeEnum.TEXT, null));
        AthenaFieldLarkBaseMapping multiSelectField = new AthenaFieldLarkBaseMapping(
                "field_tags", "Field Tags", new NestedUIType(UITypeEnum.MULTI_SELECT, null));
        AthenaFieldLarkBaseMapping urlField = new AthenaFieldLarkBaseMapping(
                "field_url", "Field Url", new NestedUIType(UITypeEnum.URL, null));

        Schema schema = CommonUtil.buildSchemaFromLarkFields(List.of(textField, multiSelectField, urlField), true);

        assertThat(schema.findField("field_text").getType()).isEqualTo(org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE);
        assertThat(schema.findField("field_tags").getType()).isEqualTo(org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE);
        assertThat(schema.findField("field_tags").getChildren()).isEmpty();
        assertThat(schema.findField("field_url").getType()).isEqualTo(org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE);
        assertThat(schema.findField("field_url").getChildren()).isEmpty();
    }

    @Test
    void testBuildSchemaFromLarkFields_complexTypeAsJsonStringFalse_listStructColumnsUnaffected() {
        // Regression guard: default (false) behavior is unaffected by the flag's existence.
        AthenaFieldLarkBaseMapping multiSelectField = new AthenaFieldLarkBaseMapping(
                "field_tags", "Field Tags", new NestedUIType(UITypeEnum.MULTI_SELECT, null));

        Schema schema = CommonUtil.buildSchemaFromLarkFields(List.of(multiSelectField), false);

        assertThat(schema.findField("field_tags").getType()).isEqualTo(org.apache.arrow.vector.types.pojo.ArrowType.List.INSTANCE);
    }
}
