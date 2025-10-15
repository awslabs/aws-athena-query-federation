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
package com.amazonaws.athena.connectors.lark.base.model.enums;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UITypeEnumTest {

    @Test
    void testGetUiType() {
        // Assert
        assertThat(UITypeEnum.TEXT.getUiType()).isEqualTo("Text");
        assertThat(UITypeEnum.BARCODE.getUiType()).isEqualTo("Barcode");
        assertThat(UITypeEnum.SINGLE_SELECT.getUiType()).isEqualTo("SingleSelect");
        assertThat(UITypeEnum.NUMBER.getUiType()).isEqualTo("Number");
        assertThat(UITypeEnum.FORMULA.getUiType()).isEqualTo("Formula");
        assertThat(UITypeEnum.LOOKUP.getUiType()).isEqualTo("Lookup");
        assertThat(UITypeEnum.UNKNOWN.getUiType()).isEqualTo("unknown");
    }

    @Test
    void testFromStringExactMatch() {
        // Test exact matches
        assertThat(UITypeEnum.fromString("Text")).isEqualTo(UITypeEnum.TEXT);
        assertThat(UITypeEnum.fromString("Barcode")).isEqualTo(UITypeEnum.BARCODE);
        assertThat(UITypeEnum.fromString("SingleSelect")).isEqualTo(UITypeEnum.SINGLE_SELECT);
        assertThat(UITypeEnum.fromString("Phone")).isEqualTo(UITypeEnum.PHONE);
        assertThat(UITypeEnum.fromString("Number")).isEqualTo(UITypeEnum.NUMBER);
        assertThat(UITypeEnum.fromString("AutoNumber")).isEqualTo(UITypeEnum.AUTO_NUMBER);
        assertThat(UITypeEnum.fromString("Progress")).isEqualTo(UITypeEnum.PROGRESS);
        assertThat(UITypeEnum.fromString("Currency")).isEqualTo(UITypeEnum.CURRENCY);
        assertThat(UITypeEnum.fromString("Rating")).isEqualTo(UITypeEnum.RATING);
        assertThat(UITypeEnum.fromString("MultiSelect")).isEqualTo(UITypeEnum.MULTI_SELECT);
        assertThat(UITypeEnum.fromString("User")).isEqualTo(UITypeEnum.USER);
        assertThat(UITypeEnum.fromString("GroupChat")).isEqualTo(UITypeEnum.GROUP_CHAT);
        assertThat(UITypeEnum.fromString("Attachment")).isEqualTo(UITypeEnum.ATTACHMENT);
        assertThat(UITypeEnum.fromString("Formula")).isEqualTo(UITypeEnum.FORMULA);
        assertThat(UITypeEnum.fromString("SingleLink")).isEqualTo(UITypeEnum.SINGLE_LINK);
        assertThat(UITypeEnum.fromString("DuplexLink")).isEqualTo(UITypeEnum.DUPLEX_LINK);
        assertThat(UITypeEnum.fromString("DateTime")).isEqualTo(UITypeEnum.DATE_TIME);
        assertThat(UITypeEnum.fromString("CreatedTime")).isEqualTo(UITypeEnum.CREATED_TIME);
        assertThat(UITypeEnum.fromString("ModifiedTime")).isEqualTo(UITypeEnum.MODIFIED_TIME);
        assertThat(UITypeEnum.fromString("Checkbox")).isEqualTo(UITypeEnum.CHECKBOX);
        assertThat(UITypeEnum.fromString("Url")).isEqualTo(UITypeEnum.URL);
        assertThat(UITypeEnum.fromString("Location")).isEqualTo(UITypeEnum.LOCATION);
        assertThat(UITypeEnum.fromString("CreatedUser")).isEqualTo(UITypeEnum.CREATED_USER);
        assertThat(UITypeEnum.fromString("ModifiedUser")).isEqualTo(UITypeEnum.MODIFIED_USER);
        assertThat(UITypeEnum.fromString("Email")).isEqualTo(UITypeEnum.EMAIL);
        assertThat(UITypeEnum.fromString("Lookup")).isEqualTo(UITypeEnum.LOOKUP);
        assertThat(UITypeEnum.fromString("Button")).isEqualTo(UITypeEnum.BUTTON);
        assertThat(UITypeEnum.fromString("Stage")).isEqualTo(UITypeEnum.STAGE);
        assertThat(UITypeEnum.fromString("unknown")).isEqualTo(UITypeEnum.UNKNOWN);
    }

    @Test
    void testFromStringCaseInsensitive() {
        // Test case insensitivity
        assertThat(UITypeEnum.fromString("text")).isEqualTo(UITypeEnum.TEXT);
        assertThat(UITypeEnum.fromString("TEXT")).isEqualTo(UITypeEnum.TEXT);
        assertThat(UITypeEnum.fromString("TexT")).isEqualTo(UITypeEnum.TEXT);
        assertThat(UITypeEnum.fromString("number")).isEqualTo(UITypeEnum.NUMBER);
        assertThat(UITypeEnum.fromString("NUMBER")).isEqualTo(UITypeEnum.NUMBER);
        assertThat(UITypeEnum.fromString("formula")).isEqualTo(UITypeEnum.FORMULA);
        assertThat(UITypeEnum.fromString("FORMULA")).isEqualTo(UITypeEnum.FORMULA);
        assertThat(UITypeEnum.fromString("lookup")).isEqualTo(UITypeEnum.LOOKUP);
        assertThat(UITypeEnum.fromString("LOOKUP")).isEqualTo(UITypeEnum.LOOKUP);
    }

    @Test
    void testFromStringUnknownValue() {
        // Test unknown values return UNKNOWN
        assertThat(UITypeEnum.fromString("InvalidType")).isEqualTo(UITypeEnum.UNKNOWN);
        assertThat(UITypeEnum.fromString("SomeRandomText")).isEqualTo(UITypeEnum.UNKNOWN);
        assertThat(UITypeEnum.fromString("")).isEqualTo(UITypeEnum.UNKNOWN);
        assertThat(UITypeEnum.fromString("   ")).isEqualTo(UITypeEnum.UNKNOWN);
        assertThat(UITypeEnum.fromString("NotAValidUIType")).isEqualTo(UITypeEnum.UNKNOWN);
    }

    @Test
    void testFromStringNull() {
        // Test null input - should return UNKNOWN (not throw exception)
        assertThat(UITypeEnum.fromString(null)).isEqualTo(UITypeEnum.UNKNOWN);
    }

    @Test
    void testValuesMethod() {
        // Verify all enum values are accessible
        UITypeEnum[] values = UITypeEnum.values();
        assertThat(values).isNotEmpty();
        assertThat(values).contains(
            UITypeEnum.TEXT,
            UITypeEnum.NUMBER,
            UITypeEnum.FORMULA,
            UITypeEnum.LOOKUP,
            UITypeEnum.UNKNOWN
        );
        assertThat(values.length).isEqualTo(29); // Total number of enum constants
    }

    @Test
    void testValueOfMethod() {
        // Test valueOf method
        assertThat(UITypeEnum.valueOf("TEXT")).isEqualTo(UITypeEnum.TEXT);
        assertThat(UITypeEnum.valueOf("NUMBER")).isEqualTo(UITypeEnum.NUMBER);
        assertThat(UITypeEnum.valueOf("FORMULA")).isEqualTo(UITypeEnum.FORMULA);
        assertThat(UITypeEnum.valueOf("LOOKUP")).isEqualTo(UITypeEnum.LOOKUP);
        assertThat(UITypeEnum.valueOf("UNKNOWN")).isEqualTo(UITypeEnum.UNKNOWN);
    }

    @Test
    void testAllEnumConstantsHaveUiType() {
        // Verify all enum constants have a non-null uiType
        for (UITypeEnum uiType : UITypeEnum.values()) {
            assertThat(uiType.getUiType()).isNotNull();
            assertThat(uiType.getUiType()).isNotEmpty();
        }
    }

    @Test
    void testFromStringForAllEnumValues() {
        // Test that fromString works for all enum values
        for (UITypeEnum uiType : UITypeEnum.values()) {
            UITypeEnum result = UITypeEnum.fromString(uiType.getUiType());
            assertThat(result).isEqualTo(uiType);
        }
    }

    @Test
    void testSpecialUITypes() {
        // Test special/unavailable UI types
        assertThat(UITypeEnum.BUTTON.getUiType()).isEqualTo("Button");
        assertThat(UITypeEnum.STAGE.getUiType()).isEqualTo("Stage");
    }
}
