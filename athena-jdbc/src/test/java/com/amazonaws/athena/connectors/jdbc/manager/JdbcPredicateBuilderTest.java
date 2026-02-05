/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

public class JdbcPredicateBuilderTest
{
    private JdbcQueryFactory queryFactory;
    private JdbcPredicateBuilder predicateBuilder;
    private List<TypeAndValue> parameterValues;
    private BlockAllocator allocator;
    private ArrowType type;

    private static class TestJdbcPredicateBuilder extends JdbcPredicateBuilder
    {
        protected TestJdbcPredicateBuilder(String quoteChar, JdbcQueryFactory queryFactory)
        {
            super(quoteChar, queryFactory);
        }
    }

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
        type = Types.MinorType.INT.getType();
        queryFactory = mock(JdbcQueryFactory.class);
        predicateBuilder = new TestJdbcPredicateBuilder("\"", queryFactory);
        parameterValues = new ArrayList<>();

        // Setup mock templates
        when(queryFactory.getQueryTemplate("null_predicate"))
                .thenAnswer(inv -> new ST("<columnName> <if(isNull)>IS NULL<else>IS NOT NULL<endif>"));
        when(queryFactory.getQueryTemplate("comparison_predicate"))
                .thenAnswer(inv -> new ST("<columnName> <operator> ?"));
        when(queryFactory.getQueryTemplate("in_predicate"))
                .thenAnswer(inv -> new ST("<columnName> IN (<counts:{p|?}; separator=\",\">)"));
        when(queryFactory.getQueryTemplate("range_predicate"))
                .thenAnswer(inv -> new ST("(<conjuncts; separator=\" AND \">)"));
        when(queryFactory.getQueryTemplate("or_predicate"))
                .thenAnswer(inv -> new ST("(<disjuncts; separator=\" OR \">)"));
    }

    @Test
    public void buildConjuncts_EqualityConstraint_ReturnsEqualityPredicate()
    {
        Field col1 = FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build();
        List<Field> columns = ImmutableList.of(col1);

        ValueSet valueSet = SortedRangeSet.of(Range.equal(allocator, Types.MinorType.INT.getType(), 10));
        Constraints constraints = new Constraints(ImmutableMap.of("col1", valueSet), Collections.emptyList(), Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        Split split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());

        List<String> conjuncts = predicateBuilder.buildConjuncts(columns, constraints, parameterValues, split);

        Assert.assertEquals(1, conjuncts.size());
        Assert.assertEquals("(\"col1\" = ?)", conjuncts.get(0));
        Assert.assertEquals(1, parameterValues.size());
        Assert.assertEquals(10, parameterValues.get(0).getValue());
    }

    @Test
    public void toPredicate_InClauseConstraint_ReturnsInPredicate()
    {
        ValueSet valueSet = SortedRangeSet.of(false,
                Range.equal(allocator, type, 10),
                Range.equal(allocator, type, 20),
                Range.equal(allocator, type, 30)
        );

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("(\"col1\" IN (?,?,?))", predicate);
        Assert.assertEquals(3, parameterValues.size());
        Assert.assertEquals(10, parameterValues.get(0).getValue());
        Assert.assertEquals(20, parameterValues.get(1).getValue());
        Assert.assertEquals(30, parameterValues.get(2).getValue());
    }

    @Test
    public void toPredicate_RangeConstraint_ReturnsAndPredicate()
    {
        ValueSet valueSet = SortedRangeSet.of(Range.range(allocator, type, 10, true, 20, false));

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("((\"col1\" >= ? AND \"col1\" < ?))", predicate);
        Assert.assertEquals(2, parameterValues.size());
        Assert.assertEquals(10, parameterValues.get(0).getValue());
        Assert.assertEquals(20, parameterValues.get(1).getValue());
    }

    @Test
    public void toPredicate_EqualityAndNullAllowed_ReturnsOrNullPredicate()
    {
        // IS NULL OR col1 = 10
        Range range = Range.equal(allocator, type, 10);
        ValueSet valueSet = SortedRangeSet.of(true, range);

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("(\"col1\" IS NULL OR \"col1\" = ?)", predicate);
        Assert.assertEquals(1, parameterValues.size());
    }

    @Test
    public void toPredicate_IsNullConstraint_ReturnsIsNullPredicate()
    {
        ValueSet valueSet = SortedRangeSet.onlyNull(type);

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("\"col1\" IS NULL", predicate);
        Assert.assertEquals(0, parameterValues.size());
    }

    @Test
    public void toPredicate_IsNotNullConstraint_ReturnsIsNotNullPredicate()
    {
        // IS NOT NULL is ALL ranges, null NOT allowed
        ValueSet valueSet = SortedRangeSet.notNull(allocator, type);

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("\"col1\" IS NOT NULL", predicate);
        Assert.assertEquals(0, parameterValues.size());
    }

    @Test
    public void toPredicate_AboveLowBound_ReturnsGreaterThanPredicate()
    {
        Range range = Range.greaterThan(allocator, type, 10);
        ValueSet valueSet = SortedRangeSet.of(range);

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("((\"col1\" > ?))", predicate);
        Assert.assertEquals(1, parameterValues.size());
        Assert.assertEquals(10, parameterValues.get(0).getValue());
    }

    @Test
    public void toPredicate_ExactlyHighBound_ReturnsLessThanOrEqualPredicate()
    {
        Range range = Range.lessThanOrEqual(allocator, type, 20);
        ValueSet valueSet = SortedRangeSet.of(range);

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("((\"col1\" <= ?))", predicate);
        Assert.assertEquals(1, parameterValues.size());
        Assert.assertEquals(20, parameterValues.get(0).getValue());
    }

    @Test
    public void toPredicate_BelowHighBound_ReturnsLessThanPredicate()
    {
        Range range = Range.lessThan(allocator, type, 20);
        ValueSet valueSet = SortedRangeSet.of(range);

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("((\"col1\" < ?))", predicate);
        Assert.assertEquals(1, parameterValues.size());
        Assert.assertEquals(20, parameterValues.get(0).getValue());
    }

    @Test
    public void toPredicate_MultipleRanges_ReturnsOrPredicate()
    {
        ValueSet valueSet = SortedRangeSet.of(false,
                Range.range(allocator, type, 10, true, 20, false),
                Range.range(allocator, type, 30, true, 40, false)
        );

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("((\"col1\" >= ? AND \"col1\" < ?) OR (\"col1\" >= ? AND \"col1\" < ?))", predicate);
        Assert.assertEquals(4, parameterValues.size());
    }

    @Test
    public void buildConjuncts_PartitionColumn_SkipsColumn()
    {
        Field col1 = FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build();
        Field partitionCol = FieldBuilder.newBuilder("partition_col", Types.MinorType.INT.getType()).build();
        List<Field> columns = ImmutableList.of(col1, partitionCol);

        ValueSet valueSet = SortedRangeSet.of(Range.equal(allocator, Types.MinorType.INT.getType(), 10));
        Constraints constraints = new Constraints(ImmutableMap.of("col1", valueSet, "partition_col", valueSet), Collections.emptyList(), Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        Split split = mock(Split.class);
        when(split.getProperties()).thenReturn(ImmutableMap.of("partition_col", "2024"));

        List<String> conjuncts = predicateBuilder.buildConjuncts(columns, constraints, parameterValues, split);

        Assert.assertEquals(1, conjuncts.size());
        Assert.assertEquals("(\"col1\" = ?)", conjuncts.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void toPredicate_InvalidLowBound_ThrowsException()
    {
        Range mockRange = mock(Range.class, RETURNS_DEEP_STUBS);
        when(mockRange.isSingleValue()).thenReturn(false);
        when(mockRange.getLow().isLowerUnbounded()).thenReturn(false);
        when(mockRange.getLow().getBound()).thenReturn(com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound.BELOW);
        
        SortedRangeSet valueSet = mock(SortedRangeSet.class, RETURNS_DEEP_STUBS);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(mockRange));

        predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);
    }

    @Test(expected = IllegalArgumentException.class)
    public void toPredicate_InvalidHighBound_ThrowsException()
    {
        Range mockRange = mock(Range.class, RETURNS_DEEP_STUBS);
        when(mockRange.isSingleValue()).thenReturn(false);
        when(mockRange.getLow().isLowerUnbounded()).thenReturn(true);
        when(mockRange.getHigh().isUpperUnbounded()).thenReturn(false);
        when(mockRange.getHigh().getBound()).thenReturn(com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound.ABOVE);
        
        SortedRangeSet valueSet = mock(SortedRangeSet.class, RETURNS_DEEP_STUBS);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(mockRange));

        predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);
    }

    @Test
    public void toPredicate_NoneAndNullAllowed_ReturnsIsNull()
    {
        ValueSet valueSet = SortedRangeSet.onlyNull(type);

        String predicate = predicateBuilder.toPredicate("col1", valueSet, type, parameterValues);

        Assert.assertEquals("\"col1\" IS NULL", predicate);
        Assert.assertEquals(0, parameterValues.size());
    }
}
