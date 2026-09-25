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
package com.amazonaws.athena.connectors.lark.base.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AthenaLarkBaseMappingTest {

    @Test
    void testConstructorAndAccessors() {
        // Arrange
        String athenaName = "employee_table";
        String larkBaseId = "tbl123456";

        // Act
        AthenaLarkBaseMapping mapping = new AthenaLarkBaseMapping(athenaName, larkBaseId);

        // Assert
        assertThat(mapping.athenaName()).isEqualTo(athenaName);
        assertThat(mapping.larkBaseId()).isEqualTo(larkBaseId);
    }

    @Test
    void testRecordEquality() {
        // Arrange
        AthenaLarkBaseMapping mapping1 = new AthenaLarkBaseMapping("test_table", "tbl123");
        AthenaLarkBaseMapping mapping2 = new AthenaLarkBaseMapping("test_table", "tbl123");
        AthenaLarkBaseMapping mapping3 = new AthenaLarkBaseMapping("other_table", "tbl456");

        // Assert
        assertThat(mapping1).isEqualTo(mapping2);
        assertThat(mapping1.hashCode()).isEqualTo(mapping2.hashCode());
        assertThat(mapping1).isNotEqualTo(mapping3);
    }

    @Test
    void testRecordToString() {
        // Arrange
        AthenaLarkBaseMapping mapping = new AthenaLarkBaseMapping("my_table", "tbl789");

        // Act
        String toString = mapping.toString();

        // Assert
        assertThat(toString).contains("AthenaLarkBaseMapping");
        assertThat(toString).contains("my_table");
        assertThat(toString).contains("tbl789");
    }

    @Test
    void testWithNullValues() {
        // Act
        AthenaLarkBaseMapping mapping = new AthenaLarkBaseMapping(null, null);

        // Assert
        assertThat(mapping.athenaName()).isNull();
        assertThat(mapping.larkBaseId()).isNull();
    }

    @Test
    void testWithEmptyStrings() {
        // Act
        AthenaLarkBaseMapping mapping = new AthenaLarkBaseMapping("", "");

        // Assert
        assertThat(mapping.athenaName()).isEmpty();
        assertThat(mapping.larkBaseId()).isEmpty();
    }

    @Test
    void testWithSpecialCharacters() {
        // Arrange
        String athenaName = "table_with_underscores_123";
        String larkBaseId = "tbl-abc-123-xyz";

        // Act
        AthenaLarkBaseMapping mapping = new AthenaLarkBaseMapping(athenaName, larkBaseId);

        // Assert
        assertThat(mapping.athenaName()).isEqualTo(athenaName);
        assertThat(mapping.larkBaseId()).isEqualTo(larkBaseId);
    }
}
