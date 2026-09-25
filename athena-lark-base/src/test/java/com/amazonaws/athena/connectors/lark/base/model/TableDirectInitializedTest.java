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

import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TableDirectInitializedTest {

    @Test
    void testConstructorAndAccessors() {
        // Arrange
        AthenaLarkBaseMapping database = new AthenaLarkBaseMapping("my_database", "db123");
        AthenaLarkBaseMapping table = new AthenaLarkBaseMapping("my_table", "tbl456");

        NestedUIType uiType = new NestedUIType(UITypeEnum.TEXT, null);
        AthenaFieldLarkBaseMapping column1 = new AthenaFieldLarkBaseMapping("id", "field_id", uiType);
        AthenaFieldLarkBaseMapping column2 = new AthenaFieldLarkBaseMapping("name", "field_name", uiType);
        List<AthenaFieldLarkBaseMapping> columns = List.of(column1, column2);

        // Act
        TableDirectInitialized result = new TableDirectInitialized(database, table, columns);

        // Assert
        assertThat(result.database()).isEqualTo(database);
        assertThat(result.table()).isEqualTo(table);
        assertThat(result.columns()).isEqualTo(columns);
        assertThat(result.columns()).hasSize(2);
    }

    @Test
    void testWithEmptyColumns() {
        // Arrange
        AthenaLarkBaseMapping database = new AthenaLarkBaseMapping("test_db", "db001");
        AthenaLarkBaseMapping table = new AthenaLarkBaseMapping("test_table", "tbl001");
        List<AthenaFieldLarkBaseMapping> emptyColumns = Collections.emptyList();

        // Act
        TableDirectInitialized result = new TableDirectInitialized(database, table, emptyColumns);

        // Assert
        assertThat(result.database()).isEqualTo(database);
        assertThat(result.table()).isEqualTo(table);
        assertThat(result.columns()).isEmpty();
    }

    @Test
    void testWithSingleColumn() {
        // Arrange
        AthenaLarkBaseMapping database = new AthenaLarkBaseMapping("db", "db_id");
        AthenaLarkBaseMapping table = new AthenaLarkBaseMapping("tbl", "tbl_id");

        NestedUIType uiType = new NestedUIType(UITypeEnum.NUMBER, null);
        AthenaFieldLarkBaseMapping column = new AthenaFieldLarkBaseMapping("age", "field_age", uiType);
        List<AthenaFieldLarkBaseMapping> columns = List.of(column);

        // Act
        TableDirectInitialized result = new TableDirectInitialized(database, table, columns);

        // Assert
        assertThat(result.columns()).hasSize(1);
        assertThat(result.columns().get(0)).isEqualTo(column);
    }

    @Test
    void testWithMultipleColumns() {
        // Arrange
        AthenaLarkBaseMapping database = new AthenaLarkBaseMapping("analytics_db", "db999");
        AthenaLarkBaseMapping table = new AthenaLarkBaseMapping("users", "tbl999");

        NestedUIType textType = new NestedUIType(UITypeEnum.TEXT, null);
        NestedUIType numberType = new NestedUIType(UITypeEnum.NUMBER, null);

        List<AthenaFieldLarkBaseMapping> columns = List.of(
            new AthenaFieldLarkBaseMapping("user_id", "fld_id", numberType),
            new AthenaFieldLarkBaseMapping("username", "fld_username", textType),
            new AthenaFieldLarkBaseMapping("email", "fld_email", textType),
            new AthenaFieldLarkBaseMapping("age", "fld_age", numberType)
        );

        // Act
        TableDirectInitialized result = new TableDirectInitialized(database, table, columns);

        // Assert
        assertThat(result.columns()).hasSize(4);
        assertThat(result.columns().get(0).athenaName()).isEqualTo("user_id");
        assertThat(result.columns().get(3).athenaName()).isEqualTo("age");
    }

    @Test
    void testRecordEquality() {
        // Arrange
        AthenaLarkBaseMapping db = new AthenaLarkBaseMapping("db", "db1");
        AthenaLarkBaseMapping tbl = new AthenaLarkBaseMapping("tbl", "tbl1");
        NestedUIType uiType = new NestedUIType(UITypeEnum.TEXT, null);
        List<AthenaFieldLarkBaseMapping> cols = List.of(
            new AthenaFieldLarkBaseMapping("col1", "fld1", uiType)
        );

        TableDirectInitialized result1 = new TableDirectInitialized(db, tbl, cols);
        TableDirectInitialized result2 = new TableDirectInitialized(db, tbl, cols);

        // Assert
        assertThat(result1).isEqualTo(result2);
        assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
    }

    @Test
    void testRecordToString() {
        // Arrange
        AthenaLarkBaseMapping db = new AthenaLarkBaseMapping("my_db", "db_123");
        AthenaLarkBaseMapping tbl = new AthenaLarkBaseMapping("my_tbl", "tbl_456");
        List<AthenaFieldLarkBaseMapping> cols = Collections.emptyList();

        TableDirectInitialized result = new TableDirectInitialized(db, tbl, cols);

        // Act
        String toString = result.toString();

        // Assert
        assertThat(toString).contains("TableDirectInitialized");
        assertThat(toString).contains("database");
        assertThat(toString).contains("table");
        assertThat(toString).contains("columns");
    }

    @Test
    void testWithMutableListOfColumns() {
        // Arrange
        AthenaLarkBaseMapping db = new AthenaLarkBaseMapping("db", "db1");
        AthenaLarkBaseMapping tbl = new AthenaLarkBaseMapping("tbl", "tbl1");

        NestedUIType uiType = new NestedUIType(UITypeEnum.TEXT, null);
        List<AthenaFieldLarkBaseMapping> mutableColumns = new ArrayList<>();
        mutableColumns.add(new AthenaFieldLarkBaseMapping("col1", "fld1", uiType));
        mutableColumns.add(new AthenaFieldLarkBaseMapping("col2", "fld2", uiType));

        // Act
        TableDirectInitialized result = new TableDirectInitialized(db, tbl, mutableColumns);

        // Assert
        assertThat(result.columns()).hasSize(2);
        assertThat(result.columns().get(0).athenaName()).isEqualTo("col1");
        assertThat(result.columns().get(1).athenaName()).isEqualTo("col2");
    }
}
