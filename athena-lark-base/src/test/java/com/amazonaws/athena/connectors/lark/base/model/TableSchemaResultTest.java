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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TableSchemaResultTest {

    @Test
    void testConstructorWithValidParameters() {
        // Arrange
        Field field1 = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(field1, field2));
        Set<String> partitionColumns = new HashSet<>(Set.of("id"));

        // Act
        TableSchemaResult result = new TableSchemaResult(schema, partitionColumns);

        // Assert
        assertThat(result.schema()).isEqualTo(schema);
        assertThat(result.partitionColumns()).isEqualTo(partitionColumns);
        assertThat(result.partitionColumns()).containsExactly("id");
    }

    @Test
    void testConstructorWithNullPartitionColumns() {
        // Arrange
        Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));

        // Act
        TableSchemaResult result = new TableSchemaResult(schema, null);

        // Assert
        assertThat(result.schema()).isEqualTo(schema);
        assertThat(result.partitionColumns()).isNotNull();
        assertThat(result.partitionColumns()).isEmpty();
    }

    @Test
    void testConstructorWithEmptyPartitionColumns() {
        // Arrange
        Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        Set<String> emptyPartitions = Collections.emptySet();

        // Act
        TableSchemaResult result = new TableSchemaResult(schema, emptyPartitions);

        // Assert
        assertThat(result.schema()).isEqualTo(schema);
        assertThat(result.partitionColumns()).isEmpty();
    }

    @Test
    void testConstructorWithMultiplePartitionColumns() {
        // Arrange
        Field field1 = new Field("year", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field2 = new Field("month", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field field3 = new Field("day", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field1, field2, field3));
        Set<String> partitionColumns = new HashSet<>(Set.of("year", "month", "day"));

        // Act
        TableSchemaResult result = new TableSchemaResult(schema, partitionColumns);

        // Assert
        assertThat(result.schema()).isEqualTo(schema);
        assertThat(result.partitionColumns()).containsExactlyInAnyOrder("year", "month", "day");
    }

    @Test
    void testRecordEquality() {
        // Arrange
        Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        Set<String> partitions = Set.of("id");

        TableSchemaResult result1 = new TableSchemaResult(schema, partitions);
        TableSchemaResult result2 = new TableSchemaResult(schema, partitions);

        // Assert
        assertThat(result1).isEqualTo(result2);
        assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
    }

    @Test
    void testRecordToString() {
        // Arrange
        Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        Set<String> partitions = Set.of("id");

        TableSchemaResult result = new TableSchemaResult(schema, partitions);

        // Assert
        String toString = result.toString();
        assertThat(toString).contains("TableSchemaResult");
        assertThat(toString).contains("schema");
        assertThat(toString).contains("partitionColumns");
    }
}
