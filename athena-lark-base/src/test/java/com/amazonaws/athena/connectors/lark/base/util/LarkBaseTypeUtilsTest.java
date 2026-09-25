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

    @Test
    void testLarkFieldToArrowMinorType_SingleLink() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "linked", "Linked", new NestedUIType(UITypeEnum.SINGLE_LINK, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.STRUCT);
    }

    @Test
    void testLarkFieldToArrowMinorType_DuplexLink() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "duplex", "Duplex", new NestedUIType(UITypeEnum.DUPLEX_LINK, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.STRUCT);
    }

    // Test larkFieldToArrowMinorType for LOOKUP type
    // This is the test that would have caught the dead-code bug where LarkBaseTableResolver and
    // ExperimentalMetadataProvider could never actually populate childType() for a genuine LOOKUP field: with
    // no case LOOKUP here, every LOOKUP field silently fell through to the default VARCHAR branch instead of LIST.
    @Test
    void testLarkFieldToArrowMinorType_LookupWithTextTarget() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.TEXT));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.LIST);
    }

    @Test
    void testLarkFieldToArrowMinorType_LookupWithNumberTarget() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.NUMBER));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.LIST);
    }

    @Test
    void testLarkFieldToArrowMinorType_LookupWithUnresolvedTarget() {
        // childType is UNKNOWN when the target couldn't be resolved (broken reference, API error, cycle).
        // The field must still be a LIST at the top level - only the child element type falls back to string.
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.UNKNOWN));

        Types.MinorType result = LarkBaseTypeUtils.larkFieldToArrowMinorType(field);

        assertThat(result).isEqualTo(Types.MinorType.LIST);
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
        assertThat(result.getChildren()).hasSize(5);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("avatar_url", "email", "en_name", "id", "name");
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

    // Test getLarkListChildField for CREATED_USER
    @Test
    void testGetLarkListChildField_CreatedUser() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "creator", "Creator", new NestedUIType(UITypeEnum.CREATED_USER, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("user_info");
        assertThat(result.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(result.getChildren()).hasSize(5);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("avatar_url", "email", "en_name", "id", "name");
    }

    // Test getLarkListChildField for MODIFIED_USER
    @Test
    void testGetLarkListChildField_ModifiedUser() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "modifier", "Modifier", new NestedUIType(UITypeEnum.MODIFIED_USER, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("user_info");
        assertThat(result.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(result.getChildren()).hasSize(5);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("avatar_url", "email", "en_name", "id", "name");
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

    @Test
    void testGetLarkListChildField_LookupWithNumber() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.NUMBER));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(new ArrowType.Decimal(38, 18, 128));
    }

    @Test
    void testGetLarkListChildField_LookupWithCurrency() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.CURRENCY));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getType()).isEqualTo(new ArrowType.Decimal(38, 18, 128));
    }

    @Test
    void testGetLarkListChildField_LookupWithCheckbox() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.CHECKBOX));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(ArrowType.Bool.INSTANCE);
    }

    @Test
    void testGetLarkListChildField_LookupWithDateTime() {
        // Must match the top-level DATE_TIME mapping (DATEMILLI/Date, not Timestamp) - a table resolved
        // via the direct/experimental path and the same table crawled into Glue must agree on this LOOKUP
        // field's Arrow type, or predicates/serialization would behave differently depending on which
        // metadata-resolution path served the query (confirmed disagreement, now fixed).
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.DATE_TIME));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(Types.MinorType.DATEMILLI.getType());
    }

    @Test
    void testGetLarkListChildField_LookupWithRating() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.RATING));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(Types.MinorType.TINYINT.getType());
    }

    @Test
    void testGetLarkListChildField_LookupWithUnresolvedTarget_fallsBackToString() {
        // childType UNKNOWN (target couldn't be resolved) must fall back to a plain string element,
        // matching the Glue Crawler path's "array<string>" fallback - not throw or produce a null type.
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
    }

    @Test
    void testGetLarkListChildField_LookupWithNullTarget_fallsBackToString() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, null));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
    }

    // A LOOKUP whose target is itself LIST-shaped (USER, ATTACHMENT, MULTI_SELECT, ...) must doubly-nest
    // - matches the crawler's Glue type for the same field: UITypeEnum.LOOKUP wraps the target's own
    // "array<...>" Glue type in another array ("array<array<struct<...>>>" for Lookup<User>), since a
    // LOOKUP aggregates one target-shaped value per linked record and the target itself is already a
    // list. Before this fix, only scalar LOOKUP targets were handled - a Lookup<User> silently collapsed
    // to a flat List<Utf8> instead of List<List<Struct<...>>>, discarding the target's real shape.
    @Test
    void testGetLarkListChildField_LookupWithUserTarget_doublyNestsListOfUserStruct() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.USER));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(result.getChildren()).hasSize(1);
        Field innerListChild = result.getChildren().get(0);
        assertThat(innerListChild.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(innerListChild.getChildren()).extracting(Field::getName)
                .containsExactly("avatar_url", "email", "en_name", "id", "name");
    }

    // A LOOKUP whose target is STRUCT-shaped (URL, LOCATION, ...) nests once - matches the crawler's
    // "array<struct<...>>" Glue type for the same field, since URL/LOCATION aren't list-shaped
    // themselves the way USER/ATTACHMENT are.
    @Test
    void testGetLarkListChildField_LookupWithUrlTarget_nestsStructOnce() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.URL));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getName()).isEqualTo("item");
        assertThat(result.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("link", "text", "type");
    }

    // getLarkListChildField/getLarkStructChildFields must unwrap FORMULA the same way
    // larkFieldToArrowMinorType already does - matches the crawler path, which builds a FORMULA's Glue
    // type by calling the *target* UI type's own getGlueCatalogType (e.g. Formula<User> gets the same
    // "array<struct<...>>" Glue type a plain USER field would). Without unwrapping here, the MinorType
    // would correctly come out as LIST/STRUCT (via larkFieldToArrowMinorType's own unwrapping) but the
    // child structure would still be built from uiType=FORMULA, which neither method switches on, so it
    // silently fell back to a generic string child instead of the target type's real shape.
    @Test
    void testGetLarkListChildField_FormulaWithUserTarget_resolvesUserStructChildren() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "calc_owner", "Calculated Owner", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.USER));

        Field result = LarkBaseTypeUtils.getLarkListChildField(field);

        assertThat(result.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(result.getChildren()).extracting(Field::getName)
                .containsExactly("avatar_url", "email", "en_name", "id", "name");
    }

    @Test
    void testGetLarkStructChildFields_FormulaWithUrlTarget_resolvesUrlStructChildren() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "calc_link", "Calculated Link", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.URL));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).extracting(Field::getName).containsExactly("link", "text", "type");
    }

    @Test
    void testLarkFieldToArrowField_FormulaWithUserTarget_producesListOfUserStruct() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "calc_owner", "Calculated Owner", new NestedUIType(UITypeEnum.FORMULA, UITypeEnum.USER));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(result.getChildren()).hasSize(1);
        Field listChild = result.getChildren().get(0);
        assertThat(listChild.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
        assertThat(listChild.getChildren()).extracting(Field::getName)
                .containsExactly("avatar_url", "email", "en_name", "id", "name");
    }

    // End-to-end: larkFieldToArrowField for a LOOKUP field must produce a LIST Field whose single child carries
    // the resolved target type - this is the full schema shape Athena actually sees for a LOOKUP column.
    @Test
    void testLarkFieldToArrowField_LookupWithNumber_producesListOfDecimal() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "lookup_field", "Lookup Field", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.NUMBER));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Lookup Field");
        assertThat(result.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(result.getChildren()).hasSize(1);
        assertThat(result.getChildren().get(0).getType()).isEqualTo(new ArrowType.Decimal(38, 18, 128));
    }

    // Test getLarkStructChildFields for URL
    @Test
    void testGetLarkStructChildFields_Url() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "website", "Website", new NestedUIType(UITypeEnum.URL, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).hasSize(3);
        assertThat(result).extracting(Field::getName).containsExactly("link", "text", "type");
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

    // Test getLarkStructChildFields for SINGLE_LINK
    @Test
    void testGetLarkStructChildFields_SingleLink() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "linked", "Linked", new NestedUIType(UITypeEnum.SINGLE_LINK, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).hasSize(1);
        assertThat(result).extracting(Field::getName)
                .containsExactly("link_record_ids");
        // Verify that link_record_ids is a LIST
        Field linkRecordIdsField = result.get(0);
        assertThat(linkRecordIdsField.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(linkRecordIdsField.getChildren()).hasSize(1);
        assertThat(linkRecordIdsField.getChildren().get(0).getName()).isEqualTo("item");
    }

    // Test getLarkStructChildFields for DUPLEX_LINK
    @Test
    void testGetLarkStructChildFields_DuplexLink() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "duplex", "Duplex", new NestedUIType(UITypeEnum.DUPLEX_LINK, UITypeEnum.UNKNOWN));

        List<Field> result = LarkBaseTypeUtils.getLarkStructChildFields(field);

        assertThat(result).hasSize(1);
        assertThat(result).extracting(Field::getName)
                .containsExactly("link_record_ids");
        // Verify that link_record_ids is a LIST
        Field linkRecordIdsField = result.get(0);
        assertThat(linkRecordIdsField.getType()).isEqualTo(ArrowType.List.INSTANCE);
        assertThat(linkRecordIdsField.getChildren()).hasSize(1);
        assertThat(linkRecordIdsField.getChildren().get(0).getName()).isEqualTo("item");
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
        assertThat(result.getChildren()).hasSize(3);
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

    // Test larkFieldToArrowField for a top-level DATE_TIME field. larkFieldToArrowMinorType never
    // returns null (it falls back to VARCHAR by default), so this must fall through to the DATEMILLI
    // default case rather than any dead "minorType == null" special-casing - locks in the type that
    // RegistererExtractor's DATEMILLI extractor (and the Glue-crawler path's "timestamp" column, which
    // the SDK also resolves to DATEMILLI) actually expect.
    @Test
    void testLarkFieldToArrowField_DateTime() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "created_at", "Created At", new NestedUIType(UITypeEnum.DATE_TIME, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field);

        assertThat(result.getName()).isEqualTo("Created At");
        assertThat(result.getType()).isEqualTo(Types.MinorType.DATEMILLI.getType());
    }

    // Tests for the complexTypeAsJsonString flag (BaseConstants.DOES_ACTIVATE_COMPLEX_TYPE_AS_JSON_STRING_ENV_VAR):
    // a List/Struct-shaped field becomes plain VARCHAR instead, sidestepping Athena's own engine crash on
    // any WHERE constraint (including IS NOT NULL) referencing a List/Struct-typed column.

    @Test
    void testLarkFieldToArrowField_ComplexTypeAsJsonString_ListField_BecomesVarcharWithNoChildren() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "tags", "Tags", new NestedUIType(UITypeEnum.MULTI_SELECT, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field, true);

        assertThat(result.getName()).isEqualTo("Tags");
        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
        assertThat(result.getChildren()).isEmpty();
    }

    @Test
    void testLarkFieldToArrowField_ComplexTypeAsJsonString_StructField_BecomesVarcharWithNoChildren() {
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "website", "Website", new NestedUIType(UITypeEnum.URL, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field, true);

        assertThat(result.getName()).isEqualTo("Website");
        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
        assertThat(result.getChildren()).isEmpty();
    }

    @Test
    void testLarkFieldToArrowField_ComplexTypeAsJsonString_FormulaWrappingListTarget_BecomesVarchar() {
        // A LOOKUP wrapping a List/Struct-shaped target (e.g. Lookup<User>) resolves to LIST via the
        // same larkFieldToArrowMinorType unwrapping FORMULA/LOOKUP already do - the flag check runs
        // after that resolution, so the wrapped case needs no separate handling.
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "assignees", "Assignees", new NestedUIType(UITypeEnum.LOOKUP, UITypeEnum.USER));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field, true);

        assertThat(result.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
        assertThat(result.getChildren()).isEmpty();
    }

    @Test
    void testLarkFieldToArrowField_ComplexTypeAsJsonStringFalse_ListFieldStaysList() {
        // Regression guard: the flag must be opt-in - default (false) behavior is unaffected.
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "tags", "Tags", new NestedUIType(UITypeEnum.MULTI_SELECT, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field, false);

        assertThat(result.getType()).isEqualTo(ArrowType.List.INSTANCE);
    }

    @Test
    void testLarkFieldToArrowField_ComplexTypeAsJsonString_ScalarFieldUnaffected() {
        // The flag only touches LIST/STRUCT MinorTypes - a plain scalar field (e.g. NUMBER) must not be
        // coerced to VARCHAR by it.
        AthenaFieldLarkBaseMapping field = new AthenaFieldLarkBaseMapping(
                "amount", "Amount", new NestedUIType(UITypeEnum.NUMBER, UITypeEnum.UNKNOWN));

        Field result = LarkBaseTypeUtils.larkFieldToArrowField(field, true);

        assertThat(result.getType()).isInstanceOf(ArrowType.Decimal.class);
    }
}
