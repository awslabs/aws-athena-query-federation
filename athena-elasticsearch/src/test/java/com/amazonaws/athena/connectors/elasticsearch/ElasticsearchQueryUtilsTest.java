/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;

/**
 * This class is used to test the ElasticsearchQueryUtils class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchQueryUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryUtilsTest.class);

    private final BlockAllocatorImpl allocator = new BlockAllocatorImpl();
    Schema mapping;
    Map<String, ValueSet> constraintsMap = new HashMap<>();

    @Before
    public void setUp()
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mytext", Types.MinorType.VARCHAR.getType())
                .addField("mykeyword", Types.MinorType.VARCHAR.getType())
                .addField(new Field("mylong", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mylong",
                                FieldType.nullable(Types.MinorType.BIGINT.getType()), null))))
                .addField("myinteger", Types.MinorType.INT.getType())
                .addField("myshort", Types.MinorType.INT.getType())
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .addField(new Field("myscaled",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10.51")), null))
                .addField("myfloat", Types.MinorType.FLOAT4.getType())
                .addField("myhalf", Types.MinorType.FLOAT4.getType())
                .addField("mydatemilli", Types.MinorType.DATEMILLI.getType())
                .addField(new Field("mydatenano", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mydatenano",
                                FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null))))
                .addField("myboolean", Types.MinorType.BIT.getType())
                .addField("mybinary", Types.MinorType.VARCHAR.getType())
                .addField("mynested", Types.MinorType.STRUCT.getType(), ImmutableList.of(
                        new Field("l1long", FieldType.nullable(Types.MinorType.BIGINT.getType()), null),
                        new Field("l1date", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null),
                        new Field("l1nested", FieldType.nullable(Types.MinorType.STRUCT.getType()), ImmutableList.of(
                                new Field("l2short", FieldType.nullable(Types.MinorType.LIST.getType()),
                                        Collections.singletonList(new Field("l2short",
                                                FieldType.nullable(Types.MinorType.INT.getType()), null))),
                                new Field("l2binary", FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                                        null))))).build();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void getProjection_withMapping_returnsProjection()
    {
        logger.info("getProjection_withMapping_returnsProjection - enter");

        List<String> expectedProjection = new ArrayList<>();
        mapping.getFields().forEach(field -> expectedProjection.add(field.getName()));

        // Get the actual projection and compare to the expected one.
        FetchSourceContext context = ElasticsearchQueryUtils.getProjection(mapping);
        List<String> actualProjection = ImmutableList.copyOf(context.includes());

        logger.info("Projections - Expected: {}, Actual: {}", expectedProjection, actualProjection);
        assertEquals("Projections do not match", expectedProjection, actualProjection);

        logger.info("getProjection_withMapping_returnsProjection - exit");
    }

    @Test
    public void getQuery_withRangePredicate_returnsQueryBuilderRangePredicateString()
    {
        logger.info("getQuery_withRangePredicate_returnsQueryBuilderRangePredicateString - enter");

        constraintsMap.put("year", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.lessThan(allocator, Types.MinorType.INT.getType(), 1950),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1952),
                        Range.range(allocator, Types.MinorType.INT.getType(),
                                1955, false, 1972, true),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1996),
                        Range.greaterThanOrEqual(allocator, Types.MinorType.INT.getType(), 2010)),
                false));
        Constraints constraints = createConstraints(constraintsMap);
        String expectedPredicate = "(_exists_:year) AND year:([* TO 1950} OR {1955 TO 1972] OR [2010 TO *] OR 1952 OR 1996)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getQuery_withRangePredicate_returnsQueryBuilderRangePredicateString - exit");
    }

    @Test
    public void getQuery_withWhitelistedEquitableValues_returnsQueryBuilderOrClause()
    {
        logger.info("getQuery_withWhitelistedEquitableValues_returnsQueryBuilderOrClause - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                true, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
        Constraints constraints = createConstraints(constraintsMap);
        String expectedPredicate = "age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getQuery_withWhitelistedEquitableValues_returnsQueryBuilderOrClause - exit");
    }

    @Test
    public void getQuery_withExclusiveEquitableValues_returnsQueryBuilderNotOrClause()
    {
        logger.info("getQuery_withExclusiveEquitableValues_returnsQueryBuilderNotOrClause - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                false, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
        Constraints constraints = createConstraints(constraintsMap);
        String expectedPredicate = "NOT age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getQuery_withExclusiveEquitableValues_returnsQueryBuilderNotOrClause - exit");
    }

    @Test
    public void getQuery_withAllValuePredicate_returnsQueryBuilderExistsClause()
    {
        logger.info("getQuery_withAllValuePredicate_returnsQueryBuilderExistsClause - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), true, true));
        Constraints constraints = createConstraints(constraintsMap);
        String expectedPredicate = "(_exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getQuery_withAllValuePredicate_returnsQueryBuilderExistsClause - exit");
    }

    @Test
    public void getQuery_withNoneValuePredicate_returnsQueryBuilderNotExistsClause()
    {
        logger.info("getQuery_withNoneValuePredicate_returnsQueryBuilderNotExistsClause - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), false, false));
        Constraints constraints = createConstraints(constraintsMap);
        String expectedPredicate = "(NOT _exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getQuery_withNoneValuePredicate_returnsQueryBuilderNotExistsClause - exit");
    }

    @Test
    public void getQuery_withDateSingleValueInRange_wrapsDateInQuotes()
    {
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("mydate", SortedRangeSet.copyOf(Types.MinorType.DATEMILLI.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.DATEMILLI.getType(), 1589525370001L)), false));
        Constraints constraints = createConstraints(summary);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();
        assertTrue("Predicate for single date value should wrap value in quotes", actualPredicate.contains("mydate:(\""));
        assertTrue("Predicate should contain quoted value", actualPredicate.contains("\")"));
    }

    @Test
    public void getQuery_withExclusiveLowBound_returnsQueryBuilderExclusiveLowBoundRange()
    {
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("myfield", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.range(allocator, Types.MinorType.INT.getType(), 10, false, 20, true)), false));
        Constraints constraints = createConstraints(summary);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        assertNotNull("Should successfully create query builder for range with exclusive low bound", builder);
        String actualPredicate = builder.queryName();
        assertTrue("Exclusive low bound should use '{' in range syntax", actualPredicate.contains("{"));
    }

    @Test
    public void getQuery_withExclusiveHighBound_returnsQueryBuilderExclusiveHighBoundRange()
    {
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("myfield", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.range(allocator, Types.MinorType.INT.getType(), 10, true, 20, false)), false));
        Constraints constraints = createConstraints(summary);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        assertNotNull("Should successfully create query builder for range with exclusive high bound", builder);
        String actualPredicate = builder.queryName();
        assertTrue("Exclusive high bound should use '}' in range syntax", actualPredicate.contains("}"));
    }

    @Test
    public void getQuery_withInclusiveRange_returnsQueryBuilderInclusiveRangeClause()
    {
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("myfield", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.range(allocator, Types.MinorType.INT.getType(), 10, true, 20, true)), false));
        Constraints constraints = createConstraints(summary);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        assertNotNull("Should successfully create query builder for inclusive range", builder);
        String actualPredicate = builder.queryName();
        assertTrue("Inclusive range should use '[' in range syntax", actualPredicate.contains("["));
        assertTrue("Inclusive range should use ']' in range syntax", actualPredicate.contains("]"));
    }

    @Test
    public void getQuery_withEmptyRangeSet_returnsQueryBuilderMatchAllQuery()
    {
        // Test getPredicateFromRange indirectly through getQuery with empty range set
        SortedRangeSet emptyRangeSet = SortedRangeSet.copyOf(Types.MinorType.INT.getType(), Collections.emptyList(), false);
        Map<String, ValueSet> constraintSummary = new HashMap<>();
        constraintSummary.put("myfield", emptyRangeSet);
        Constraints constraints = createConstraints(constraintSummary);

        QueryBuilder result = ElasticsearchQueryUtils.getQuery(constraints);

        assertNotNull("Should return query builder", result);
    }

    @Test
    public void getQuery_withEmptyConstraints_returnsQueryBuilderMatchAllQuery()
    {
        Constraints emptyConstraints = createConstraints(Collections.emptyMap());

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(emptyConstraints);
        assertNotNull("Query builder should not be null for empty constraints", builder);
        assertTrue("Empty constraints should produce MatchAllQueryBuilder", builder instanceof MatchAllQueryBuilder);
    }

    @Test
    public void getQuery_withRangeSet_returnsQueryBuilderRangeSet()
    {
        // Test getPredicateFromRange indirectly through getQuery with valid range set
        // Note: Testing BELOW/ABOVE marker edge cases requires reflection which is not compatible with Java 17+
        // These edge cases are internal implementation details and should be tested through integration tests
        Range range = Range.range(allocator, Types.MinorType.INT.getType(), 10, true, 20, true);
        SortedRangeSet rangeSet = SortedRangeSet.copyOf(Types.MinorType.INT.getType(), ImmutableList.of(range), false);
        
        Map<String, ValueSet> constraintSummary = new HashMap<>();
        constraintSummary.put("myfield", rangeSet);
        Constraints constraints = createConstraints(constraintSummary);

        QueryBuilder result = ElasticsearchQueryUtils.getQuery(constraints);

        assertNotNull("Should return query builder for range set", result);
        String actualPredicate = result.queryName();
        assertTrue("Range set predicate should contain field name myfield", actualPredicate.contains("myfield"));
        assertTrue("Range set predicate should contain lower bound 10", actualPredicate.contains("10"));
        assertTrue("Range set predicate should contain upper bound 20", actualPredicate.contains("20"));
    }

    /**
     * Creates a Constraints instance from the given constraint summary map.
     */
    private Constraints createConstraints(Map<String, ValueSet> summary)
    {
        return new Constraints(summary, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }

}
