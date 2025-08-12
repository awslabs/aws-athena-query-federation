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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

