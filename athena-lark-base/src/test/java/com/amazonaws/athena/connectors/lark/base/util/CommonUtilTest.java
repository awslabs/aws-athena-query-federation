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
}
