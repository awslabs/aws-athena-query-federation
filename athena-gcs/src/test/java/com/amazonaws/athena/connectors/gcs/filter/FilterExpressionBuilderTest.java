/*-
 * #%L
 * Amazon Athena GCS Connector
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs.filter;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.GcsTestUtils;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.glue.model.Column;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class FilterExpressionBuilderTest
{
    private static final String PARTITION1 = "partition1";
    private static final String VALUE1 = "value1";

    @Test
    public void getConstraintsForPartitionedColumns_withYearConstraint_returnsPartitionValues()
    {
        Map<String, Optional<Set<String>>> result = FilterExpressionBuilder.getConstraintsForPartitionedColumns(
            com.google.common.collect.ImmutableList.of(Column.builder().name("year").build()),
                new Constraints(GcsTestUtils.createSummaryWithLValueRangeEqual("year", new ArrowType.Utf8(), "1"),
                        Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));
        assertEquals(result.size(), 1);
        assertEquals(result.get("year").get(), com.google.common.collect.ImmutableSet.of("1"));
        assertEquals(result.get("yeAr").get(), com.google.common.collect.ImmutableSet.of("1"));
    }

    @Test
    public void getConstraintsForPartitionedColumns_withDuplicateEmptyPartitions_returnsEmptyOptional()
    {
        //Create duplicate column names to trigger merge function with both values empty
        Map<String, Optional<Set<String>>> result = FilterExpressionBuilder.getConstraintsForPartitionedColumns(
            com.google.common.collect.ImmutableList.of(
                Column.builder().name(PARTITION1).build(),
                Column.builder().name(PARTITION1).build() // Duplicate to trigger merge
            ),
            new Constraints(Collections.emptyMap(),
                    Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));
        
        //Both values are empty, merge should return empty Optional
        assertEquals(1, result.size());
        assertFalse(result.get(PARTITION1).isPresent());
    }

    @Test
    public void getConstraintsForPartitionedColumns_withDuplicatePartitions_mergesConstraintSets()
    {
        //Create duplicate column names with constraints to trigger merge function with sets
        Map<String, Optional<Set<String>>> result = FilterExpressionBuilder.getConstraintsForPartitionedColumns(
            com.google.common.collect.ImmutableList.of(
                Column.builder().name(PARTITION1).build(),
                Column.builder().name(PARTITION1).build() // Duplicate to trigger merge
            ),
            new Constraints(GcsTestUtils.createSummaryWithLValueRangeEqual(PARTITION1, new ArrowType.Utf8(), VALUE1),
                    Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));
        
        //Values have sets, merge should combine them
        assertEquals(1, result.size());
        assertTrue(result.get(PARTITION1).isPresent());
        assertEquals(com.google.common.collect.ImmutableSet.of(VALUE1), result.get(PARTITION1).get());
    }
}
