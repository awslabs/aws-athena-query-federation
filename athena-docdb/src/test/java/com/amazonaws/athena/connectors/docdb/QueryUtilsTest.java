/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.Operator;
import com.google.common.collect.ImmutableList;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryUtilsTest
{
    private final BlockAllocatorImpl allocator = new BlockAllocatorImpl();

    @Test
    public void testMakePredicateWithSortedRangeSet()
    {
        Field field = new Field("year", FieldType.nullable(new ArrowType.Int(32, true)), null);

        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.lessThan(allocator, Types.MinorType.INT.getType(), 1950),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1952),
                        Range.range(allocator, Types.MinorType.INT.getType(), 1955, false, 1972, true),
                        Range.greaterThanOrEqual(allocator, Types.MinorType.INT.getType(), 2010)),
                false
        );
        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);
        Document expected = new Document("$or", ImmutableList.of(
                new Document("year", new Document("$lt", 1950)),
                new Document("year", new Document("$gt", 1955).append("$lte", 1972)),
                new Document("year", new Document("$gte", 2010)),
                new Document("year", new Document("$eq", 1952))
        ));
        assertEquals(expected, result);
    }

    @Test
    public void testMakePredicateWithId()
    {
        Field field = new Field("_id", FieldType.nullable(new ArrowType.Utf8()), null);

        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "4ecbe7f9e8c1c9092c000027")),
                false
        );
        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);
        Document expected =
                new Document("_id", new Document("$eq", new ObjectId("4ecbe7f9e8c1c9092c000027")));
        assertEquals(expected, result);
    }

    @Test
    public void testParseFilter()
    {
        String jsonFilter = "{ \"field\": { \"$eq\": \"value\" } }";

        Document result = QueryUtils.parseFilter(jsonFilter);
        assertNotNull(result);
        assertEquals("value", ((Document) result.get("field")).get("$eq"));
    }

    @Test
    public void testParseFilterInvalidJson()
    {
        String invalidJsonFilter = "{ field: { $eq: value } }";

        assertThrows(IllegalArgumentException.class, () -> {
            QueryUtils.parseFilter(invalidJsonFilter);
        });
    }

    @ParameterizedTest
    @MethodSource("inputTestMakeQueryFromPlanPredicateProvider")
    public void testMakeQueryFromPlanWithDifferentOperators(final Operator operator, final Object value,
                                                           final String expectedMongoOp, final ArrowType arrowType)
    {
        ColumnPredicate pred = new ColumnPredicate("colX", operator, value, arrowType);
        Map<String, List<ColumnPredicate>> predicates = Collections.singletonMap("colX",
                Collections.singletonList(pred));

        Document result = QueryUtils.makeQueryFromPlan(predicates);

        assertNotNull(result);
        Document colDoc = (Document) result.get("colX");
        assertTrue(colDoc.containsKey(expectedMongoOp));
        if (value != null && !operator.equals(Operator.IS_NULL) && !operator.equals(Operator.IS_NOT_NULL)) {
            assertEquals(value, colDoc.get(expectedMongoOp));
        }
    }

    @Test
    public void testBuildFilterPredicatesFromPlan_withNoRelations()
    {
        // Empty plan
        Plan emptyPlan = Plan.newBuilder().build();
        Map<String, List<ColumnPredicate>> result = QueryUtils.buildFilterPredicatesFromPlan(emptyPlan);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildFilterPredicatesFromPlan_withNullPlan()
    {
        Map<String, List<ColumnPredicate>> result = QueryUtils.buildFilterPredicatesFromPlan(null);
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("multipleEqualPredicatesProvider")
    public void testMakeQueryFromPlanWithMultipleEqualPredicates(List<Object> values, List<Object> expectedInValues)
    {
        List<ColumnPredicate> predicatesList = values.stream()
                .map(value -> new ColumnPredicate("colX", Operator.EQUAL, value, new ArrowType.Int(32, true)))
                .collect(Collectors.toList());
        Map<String, List<ColumnPredicate>> predicates = Collections.singletonMap("colX", predicatesList);
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        assertNotNull(result);
        Document colDoc = (Document) result.get("colX");
        if (values.size() == 1) {
            assertTrue(colDoc.containsKey("$eq"));
            assertEquals(values.get(0), colDoc.get("$eq"));
        } else {
            assertTrue(colDoc.containsKey("$in"));
            assertEquals(expectedInValues, colDoc.get("$in"));
        }
    }

    // Parameterized test for mixed predicate combinations
    @ParameterizedTest
    @MethodSource("mixedPredicatesProvider")
    public void testMakeQueryFromPlanWithMixedPredicates(List<ColumnPredicate> predicatesList,
                                                         String expectedOperator,
                                                         Object expectedContent)
    {
        Map<String, List<ColumnPredicate>> predicates = Collections.singletonMap("colX", predicatesList);
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        assertNotNull(result);
        if ("$and".equals(expectedOperator) || "$or".equals(expectedOperator)) {
            assertTrue(result.containsKey(expectedOperator));
            List<Document> conditions = (List<Document>) result.get(expectedOperator);
            assertEquals(expectedContent, conditions);
        } else {
            Document colDoc = (Document) result.get("colX");
            assertTrue(colDoc.containsKey(expectedOperator));
            assertEquals(expectedContent, colDoc.get(expectedOperator));
        }
    }

    @Test
    public void testMakeQueryFromPlanWithMultipleColumns()
    {
        List<ColumnPredicate> col1Predicates = Arrays.asList(
                new ColumnPredicate("col1", Operator.EQUAL, 123, new ArrowType.Int(32, true)),
                new ColumnPredicate("col1", Operator.EQUAL, 456, new ArrowType.Int(32, true))
        );
        List<ColumnPredicate> col2Predicates = Collections.singletonList(
                new ColumnPredicate("col2", Operator.GREATER_THAN, 100, new ArrowType.Int(32, true))
        );
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("col1", col1Predicates);
        predicates.put("col2", col2Predicates);
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        assertNotNull(result);
        assertEquals(2, result.size());
        Document col1Doc = (Document) result.get("col1");
        assertTrue(col1Doc.containsKey("$in"));
        assertEquals(Arrays.asList(123, 456), col1Doc.get("$in"));
        Document col2Doc = (Document) result.get("col2");
        assertTrue(col2Doc.containsKey("$gt"));
        assertEquals(100, col2Doc.get("$gt"));
    }

    @Test
    public void testMakeQueryFromPlanWithEmptyPredicates()
    {
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testMakeQueryFromPlanWithNullPredicates()
    {
        Document result = QueryUtils.makeQueryFromPlan(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    private static Stream<Arguments> multipleEqualPredicatesProvider()
    {
        return Stream.of(
                // Single value (should use $eq)
                Arguments.of(Arrays.asList(123), Arrays.asList(123)),
                // Multiple values (should use $in)
                Arguments.of(Arrays.asList(123, 456), Arrays.asList(123, 456)),
                Arguments.of(Arrays.asList(123, 456, 789), Arrays.asList(123, 456, 789)),
                // String values
                Arguments.of(Arrays.asList("a", "b"), Arrays.asList("a", "b"))
        );
    }

    private static Stream<Arguments> mixedPredicatesProvider()
    {
        return Stream.of(
                // Multiple EQUAL predicates (IN condition)
                Arguments.of(
                        Arrays.asList(
                                new ColumnPredicate("colX", Operator.EQUAL, 123, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", Operator.EQUAL, 456, new ArrowType.Int(32, true))
                        ),
                        "$in",
                        Arrays.asList(123, 456)
                ),
                // IN + GT (should use $and)
                Arguments.of(
                        Arrays.asList(
                                new ColumnPredicate("colX", Operator.EQUAL, 123, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", Operator.EQUAL, 456, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", Operator.GREATER_THAN, 100, new ArrowType.Int(32, true))
                        ),
                        "$and",
                        Arrays.asList(
                                new Document("colX", new Document("$in", Arrays.asList(123, 456))),
                                new Document("colX", new Document("$gt", 100))
                        )
                ),
                // Multiple non-EQUAL predicates (should use $or)
                Arguments.of(
                        Arrays.asList(
                                new ColumnPredicate("colX", Operator.GREATER_THAN, 100, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", Operator.LESS_THAN, 200, new ArrowType.Int(32, true))
                        ),
                        "$or",
                        Arrays.asList(
                                new Document("colX", new Document("$gt", 100)),
                                new Document("colX", new Document("$lt", 200))
                        )
                )
        );
    }

    private static Stream<Arguments> inputTestMakeQueryFromPlanPredicateProvider()
    {
        return Stream.of(
                Arguments.of(Operator.EQUAL, 42, "$eq", new ArrowType.Int(32, true)),
                Arguments.of(Operator.NOT_EQUAL, 100, "$ne", new ArrowType.Int(32, true)),
                Arguments.of(Operator.GREATER_THAN, 50, "$gt", new ArrowType.Int(32, true)),
                Arguments.of(Operator.GREATER_THAN_OR_EQUAL_TO, 75, "$gte", new ArrowType.Int(32, true)),
                Arguments.of(Operator.LESS_THAN, 25, "$lt", new ArrowType.Int(32, true)),
                Arguments.of(Operator.LESS_THAN_OR_EQUAL_TO, 30, "$lte",
                        new ArrowType.Int(32, true)),
                Arguments.of(Operator.EQUAL, "testString", "$eq", new ArrowType.Utf8()),
                Arguments.of(Operator.IS_NULL, null, "$eq", new ArrowType.Utf8()),      // isNullPredicate
                Arguments.of(Operator.IS_NOT_NULL, null, "$ne", new ArrowType.Utf8())   // isNotNullPredicate
        );
    }
}

