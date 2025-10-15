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
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LarkBaseTypeUtilsTest {

    // Test larkFieldToArrowMinorType for VARCHAR types
    @Test
    void testLarkFieldToArrowMinorType_Text() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "test_field", "Test Field", new NestedUIType(UITypeEnum.TEXT, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.VARCHAR);
    }

    @Test
    void testLarkFieldToArrowMinorType_Barcode() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "barcode_field", "Barcode", new NestedUIType(UITypeEnum.BARCODE, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.VARCHAR);
    }

    @Test
    void testLarkFieldToArrowMinorType_SingleSelect() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "status", "Status", new NestedUIType(UITypeEnum.SINGLE_SELECT, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.VARCHAR);
    }

    // Test larkFieldToArrowMinorType for DECIMAL types
    @Test
    void testLarkFieldToArrowMinorType_Number() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "amount", "Amount", new NestedUIType(UITypeEnum.NUMBER, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.DECIMAL);
    }

    @Test
    void testLarkFieldToArrowMinorType_Currency() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "price", "Price", new NestedUIType(UITypeEnum.CURRENCY, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.DECIMAL);
    }

    // Test larkFieldToArrowMinorType for TINYINT type
    @Test
    void testLarkFieldToArrowMinorType_Rating() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "stars", "Stars", new NestedUIType(UITypeEnum.RATING, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.TINYINT);
    }

    // Test larkFieldToArrowMinorType for DATEMILLI types
    @Test
    void testLarkFieldToArrowMinorType_DateTime() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "created_at", "Created At", new NestedUIType(UITypeEnum.DATE_TIME, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.DATEMILLI);
    }

    @Test
    void testLarkFieldToArrowMinorType_CreatedTime() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "created_time", "Created Time", new NestedUIType(UITypeEnum.CREATED_TIME, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.DATEMILLI);
    }

    // Test larkFieldToArrowMinorType for BIT type
    @Test
    void testLarkFieldToArrowMinorType_Checkbox() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "is_active", "Is Active", new NestedUIType(UITypeEnum.CHECKBOX, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.BIT);
    }

    // Test larkFieldToArrowMinorType for LIST types
    @Test
    void testLarkFieldToArrowMinorType_MultiSelect() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "tags", "Tags", new NestedUIType(UITypeEnum.MULTI_SELECT, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.LIST);
    }

    @Test
    void testLarkFieldToArrowMinorType_User() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "assignees", "Assignees", new NestedUIType(UITypeEnum.USER, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.LIST);
    }

    // Test larkFieldToArrowMinorType for STRUCT types
    @Test
    void testLarkFieldToArrowMinorType_Url() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "website", "Website", new NestedUIType(UITypeEnum.URL, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.STRUCT);
    }

    @Test
    void testLarkFieldToArrowMinorType_Location() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "address", "Address", new NestedUIType(UITypeEnum.LOCATION, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.STRUCT);
    }

    // Test larkFieldToArrowMinorType for FORMULA type
    @Test
    void testLarkFieldToArrowMinorType_FormulaWithNumber() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "calc_field", "Calculated", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.NUMBER));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.DECIMAL);
    }

    @Test
    void testLarkFieldToArrowMinorType_FormulaWithText() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "calc_text", "Calculated Text", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.TEXT));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.VARCHAR);
    }

    // Test larkFieldToArrowMinorType for UNKNOWN type (default)
    @Test
    void testLarkFieldToArrowMinorType_Unknown() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "unknown_field", "Unknown", new NestedUIType(UITypeEnum.UNKNOWN, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.VARCHAR);
    }

    // Test getLarkListChildField for MULTI_SELECT
    @Test
    void testGetLarkListChildField_MultiSelect() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "tags", "Tags", new NestedUIType(UITypeEnum.MULTI_SELECT, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
    }

    // Test getLarkListChildField for USER
    @Test
    void testGetLarkListChildField_User() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "users", "Users", new NestedUIType(UITypeEnum.USER, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("user_info");
        assertThat(result.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(result.getChildren()).hasSize(4);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("email", "en_name", "id", "name");
    }

    // Test getLarkListChildField for GROUP_CHAT
    @Test
    void testGetLarkListChildField_GroupChat() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "chats", "Chats", new NestedUIType(UITypeEnum.GROUP_CHAT, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("group_chat_info");
        assertThat(result.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(result.getChildren()).hasSize(3);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("avatar_url", "id", "name");
    }

    // Test getLarkListChildField for ATTACHMENT
    @Test
    void testGetLarkListChildField_Attachment() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "files", "Files", new NestedUIType(UITypeEnum.ATTACHMENT, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("attachment_info");
        assertThat(result.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(result.getChildren()).hasSize(6);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("file_token", "name", "size", "tmp_url", "type", "url");
    }

    // Test getLarkListChildField for SINGLE_LINK
    @Test
    void testGetLarkListChildField_SingleLink() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "linked", "Linked", new NestedUIType(UITypeEnum.SINGLE_LINK, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("linked_record");
        assertThat(result.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(result.getChildren()).hasSize(5);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("record_ids", "table_id", "text", "text_arr", "type");
    }

    // Test getLarkListChildField for LOOKUP
    @Test
    void testGetLarkListChildField_LookupWithText() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.TEXT));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
    }

    // Test getLarkStructChildFields for URL
    @Test
    void testGetLarkStructChildFields_Url() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "website", "Website", new NestedUIType(UITypeEnum.URL, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).hasSize(2);
        assertThat(result).extracting(Field::getName).containsExactly("link", "text");
        assertThat(result).allMatch(f -> f.getType().equals(ArrowType.Utf8.INSTANCE));
    }

    // Test getLarkStructChildFields for LOCATION
    @Test
    void testGetLarkStructChildFields_Location() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "address", "Address", new NestedUIType(UITypeEnum.LOCATION, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).hasSize(7);
        assertThat(result).extracting(Field::getName)
                .containsExactly("address", "adname", "cityname", "full_address", "location", "name", "pname");
    }

    // Test getLarkStructChildFields for CREATED_USER
    @Test
    void testGetLarkStructChildFields_CreatedUser() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "creator", "Creator", new NestedUIType(UITypeEnum.CREATED_USER, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).hasSize(4);
        assertThat(result).extracting(Field::getName)
                .containsExactly("id", "name", "en_name", "email");
    }

    // Test getLarkStructChildFields for MODIFIED_USER
    @Test
    void testGetLarkStructChildFields_ModifiedUser() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "modifier", "Modifier", new NestedUIType(UITypeEnum.MODIFIED_USER, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).hasSize(4);
        assertThat(result).extracting(Field::getName)
                .containsExactly("id", "name", "en_name", "email");
    }

    // Test getLarkStructChildFields for default case
    @Test
    void testGetLarkStructChildFields_Default() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "text_field", "Text", new NestedUIType(UITypeEnum.TEXT, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).isEmpty();
    }

    // Test larkFieldToArrowField for simple VARCHAR field
    @Test
    void testLarkFieldToArrowField_Text() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "name_field", "Name Field", new NestedUIType(UITypeEnum.TEXT, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Name Field");
        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
        assertThat(result.isNullable()).isTrue();
        assertThat(result.getChildren()).isEmpty();
    }

    // Test larkFieldToArrowField for DECIMAL field
    @Test
    void testLarkFieldToArrowField_Number() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "amount", "Amount", new NestedUIType(UITypeEnum.NUMBER, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Amount");
        assertThat(result.getType()).isInstanceOf(ArrowType.Decimal.class);
        ArrowType.Decimal decimalType = (ArrowType.Decimal) result.getType();
        assertThat(decimalType.getPrecision()).isEqualTo(38);
        assertThat(decimalType.getScale()).isEqualTo(18);
    }

    // Test larkFieldToArrowField for BIT field
    @Test
    void testLarkFieldToArrowField_Checkbox() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "is_active", "Is Active", new NestedUIType(UITypeEnum.CHECKBOX, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Is Active");
        assertThat(result.getType()).isEqualTo(ArrowType.Bool.INSTANCE);
    }

    // Test larkFieldToArrowField for LIST field
    @Test
    void testLarkFieldToArrowField_MultiSelect() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "tags", "Tags", new NestedUIType(UITypeEnum.MULTI_SELECT, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Tags");
        assertThat(result.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(result.getChildren()).hasSize(1);
        assertThat(result.getChildren().get(0).getName()).isEqualTo("item");
    }

    // Test larkFieldToArrowField for STRUCT field
    @Test
    void testLarkFieldToArrowField_Url() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "website", "Website", new NestedUIType(UITypeEnum.URL, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Website");
        assertThat(result.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(result.getChildren()).hasSize(2);
    }

    // Test larkFieldToArrowField for TINYINT field
    @Test
    void testLarkFieldToArrowField_Rating() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "stars", "Stars", new NestedUIType(UITypeEnum.RATING, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Stars");
        assertThat(result.getType()).isInstanceOf(ArrowType.Int.class);
        ArrowType.Int intType = (ArrowType.Int) result.getType();
        assertThat(intType.getBitWidth()).isEqualTo(8);
    }
}
