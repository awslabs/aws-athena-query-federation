/*-
 * #%L
 * athena-timestream
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
package com.amazonaws.athena.connectors.timestream.query;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PredicateBuilderTest
{
    private BlockAllocator allocator;
    
    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void quoteColumn_columnName_wrapsInDoubleQuotes()
    {
        assertEquals("\"measure_name\"", PredicateBuilder.quoteColumn("measure_name"));
    }

    @Test
    public void quoteColumn_doubleQuoteInName_escapedByDoubling()
    {
        assertEquals("\"a\"\"b\"", PredicateBuilder.quoteColumn("a\"b"));
    }
    
    @Test
    public void escapeSqlStringLiteral_valueContainsApostrophe_doublesApostrophe()
    {
        assertEquals("O''Brien", PredicateBuilder.escapeSqlStringLiteral("O'Brien"));
    }

    @Test
    public void buildConjucts_equatableVarcharWithApostrophe_escapesLiteralsInInClause()
    {
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("region",
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                        .add("us-east-1")
                        .add("O'Brien")
                        .build()));
        assertEquals(1, conjuncts.size());
        assertEquals("(\"region\" IN ('us-east-1','O''Brien'))", conjuncts.get(0));
    }

    @Test
    public void buildConjucts_sortedRangeVarcharSingleValueWithApostrophe_escapesLiteralInEquality()
    {
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("hostname",
                SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(),
                        ImmutableList.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "host-keE37's-test")),
                        false)));
        assertEquals(1, conjuncts.size());
        assertEquals("(\"hostname\" = 'host-keE37''s-test')", conjuncts.get(0));
    }

    @Test
    public void buildConjucts_intGreaterThanRange_returnsComparisonConjunct()
    {
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("col1",
                SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                        ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false)));
        assertEquals(1, conjuncts.size());
        assertEquals("((\"col1\" > 1))", conjuncts.get(0));
    }

    @Test
    public void buildConjucts_columnNameContainsDoubleQuote_outputsEscapedQuotedIdentifier()
    {
        String columnWithQuote = "evil\" OR 1=1 --";
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints(columnWithQuote,
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                        .add("x")
                        .build()));
        assertEquals(1, conjuncts.size());
        assertTrue(conjuncts.get(0).contains("\"evil\"\" OR 1=1 --\""));
        assertTrue(conjuncts.get(0).contains("'x'"));
    }

    @Test
    public void buildConjucts_dateMilliGreaterThanRange_formatsTimestampWithNanosecondPrecision()
    {
        LocalDateTime ts = LocalDateTime.of(2024, 4, 5, 9, 31, 12, 142000000);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("time0",
                SortedRangeSet.copyOf(Types.MinorType.DATEMILLI.getType(),
                        ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(), ts)), false)));
        assertEquals(1, conjuncts.size());
        assertEquals("((\"time0\" > '2024-04-05 09:31:12.142000000'))", conjuncts.get(0));
    }

    @Test
    public void buildConjucts_onlyNullSortedRangeSet_returnsIsNullPredicate()
    {
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("col",
                SortedRangeSet.onlyNull(Types.MinorType.INT.getType())));
        assertEquals(1, conjuncts.size());
        assertEquals("(\"col\" IS NULL)", conjuncts.get(0));
    }

    @Test
    public void buildConjucts_notNullSortedRangeSet_returnsIsNotNullPredicate()
    {
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("col",
                SortedRangeSet.notNull(allocator, Types.MinorType.INT.getType())));
        assertEquals(1, conjuncts.size());
        assertEquals("(\"col\" IS NOT NULL)", conjuncts.get(0));
    }

    @Test
    public void buildConjucts_nullAllowedWithIntGreaterThan_returnsIsNullOrComparisonPredicate()
    {
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("col",
                SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                        ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), true)));
        assertEquals(1, conjuncts.size());
        assertEquals("((\"col\" IS NULL) OR (\"col\" > 1))", conjuncts.get(0));
    }

    @Test
    public void buildConjucts_emptySummary_returnsEmptyConjunctList()
    {
        assertTrue(PredicateBuilder.buildConjucts(constraints(Collections.emptyMap())).isEmpty());
    }
    
    @Test(expected = AthenaConnectorException.class)
    public void buildConjucts_sortedRangeSetNoneWithoutNull_throwsAthenaConnectorException()
    {
        PredicateBuilder.buildConjucts(constraints("col",
                SortedRangeSet.none(Types.MinorType.INT.getType())));
    }

    @Test
    public void buildConjucts_equatableValueSetEmpty_returnsInClauseWithNoLiterals()
    {
        EquatableValueSet empty = EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .build();
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints("c", empty));
        assertEquals(1, conjuncts.size());
        assertEquals("(\"c\" IN ())", conjuncts.get(0));
    }

    private static Constraints constraints(Map<String, ValueSet> summary)
    {
        return new Constraints(
                summary,
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null);
    }
    
    private static Constraints constraints(String column, ValueSet valueSet)
    {
        Map<String, ValueSet> summary = new LinkedHashMap<>();
        summary.put(column, valueSet);
        return constraints(summary);
    }
}
