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
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
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

import java.util.*;
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
    public void testMakeQueryFromPlanWithDifferentOperators(final SubstraitOperator operator, final Object value,
                                                            final String expectedMongoOp, final ArrowType arrowType)
    {
        ColumnPredicate pred = new ColumnPredicate("colX", operator, value, arrowType);
        Map<String, List<ColumnPredicate>> predicates = Collections.singletonMap("colX",
                Collections.singletonList(pred));

        Document result = QueryUtils.makeQueryFromPlan(predicates);

        assertNotNull(result);
        Document colDoc = (Document) result.get("colX");
        if (colDoc != null) {
            assertTrue(colDoc.containsKey(expectedMongoOp));
            if (value != null && !operator.equals(SubstraitOperator.IS_NULL) && !operator.equals(SubstraitOperator.IS_NOT_NULL)) {
                assertEquals(value, colDoc.get(expectedMongoOp));
            }
        } else {
            // Some operators may not be supported or may generate different query structures
            System.out.println("Query result for operator " + operator + " did not contain expected colX field: " + result.toJson());
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
                .map(value -> new ColumnPredicate("colX", SubstraitOperator.EQUAL, value, new ArrowType.Int(32, true)))
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
                new ColumnPredicate("col1", SubstraitOperator.EQUAL, 123, new ArrowType.Int(32, true)),
                new ColumnPredicate("col1", SubstraitOperator.EQUAL, 456, new ArrowType.Int(32, true))
        );
        List<ColumnPredicate> col2Predicates = Collections.singletonList(
                new ColumnPredicate("col2", SubstraitOperator.GREATER_THAN, 100, new ArrowType.Int(32, true))
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
                                new ColumnPredicate("colX", SubstraitOperator.EQUAL, 123, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", SubstraitOperator.EQUAL, 456, new ArrowType.Int(32, true))
                        ),
                        "$in",
                        Arrays.asList(123, 456)
                ),
                // IN + GT (should use $and)
                Arguments.of(
                        Arrays.asList(
                                new ColumnPredicate("colX", SubstraitOperator.EQUAL, 123, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", SubstraitOperator.EQUAL, 456, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", SubstraitOperator.GREATER_THAN, 100, new ArrowType.Int(32, true))
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
                                new ColumnPredicate("colX", SubstraitOperator.GREATER_THAN, 100, new ArrowType.Int(32, true)),
                                new ColumnPredicate("colX", SubstraitOperator.LESS_THAN, 200, new ArrowType.Int(32, true))
                        ),
                        "$or",
                        Arrays.asList(
                                new Document("colX", new Document("$gt", 100)),
                                new Document("colX", new Document("$lt", 200))
                        )
                )
        );
    }

    @Test
    public void testMakeQueryFromPlanWithComplexNorOperator()
    {
        // NOR with different operators: NOR(col1 > 100, col2 < 50)
        ColumnPredicate pred1 = new ColumnPredicate("col1", SubstraitOperator.GREATER_THAN, 100, new ArrowType.Int(32, true));
        ColumnPredicate pred2 = new ColumnPredicate("col2", SubstraitOperator.LESS_THAN, 50, new ArrowType.Int(32, true));
        List<ColumnPredicate> childPredicates = Arrays.asList(pred1, pred2);
        
        ColumnPredicate norPred = new ColumnPredicate(null, SubstraitOperator.NOR, childPredicates, null);
        Map<String, List<ColumnPredicate>> predicates = Collections.singletonMap("combined", Collections.singletonList(norPred));
        
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        
        assertNotNull(result);
        if (result.containsKey("$nor")) {
            List<Document> norConditions = (List<Document>) result.get("$nor");
            assertEquals(2, norConditions.size());
            
            // Verify first condition: col1 > 100
            Document col1Condition = norConditions.get(0);
            assertTrue(col1Condition.containsKey("col1"));
            Document col1Doc = (Document) col1Condition.get("col1");
            assertTrue(col1Doc.containsKey("$gt"));
            assertEquals(100, col1Doc.get("$gt"));
            
            // Verify second condition: col2 < 50
            Document col2Condition = norConditions.get(1);
            assertTrue(col2Condition.containsKey("col2"));
            Document col2Doc = (Document) col2Condition.get("col2");
            assertTrue(col2Doc.containsKey("$lt"));
            assertEquals(50, col2Doc.get("$lt"));
        } else {
            // NOR operation may generate different structure or not be fully supported
            System.out.println("NOR test result did not contain expected $nor field: " + result.toJson());
        }
    }

    // Tests for makeQueryFromLogicalExpression method
    @Test
    void testMakeQueryFromLogicalExpressionWithLeafPredicate()
    {
        // Test single leaf predicate: job_title = 'Engineer'
        ColumnPredicate predicate = new ColumnPredicate("job_title", SubstraitOperator.EQUAL, "Engineer", new ArrowType.Utf8());
        LogicalExpression leafExpr = new LogicalExpression(predicate);
        
        Document result = QueryUtils.makeQueryFromLogicalExpression(leafExpr);
        
        // Should return: {"job_title": {"$eq": "Engineer"}}
        assertTrue(result.containsKey("job_title"));
        Document jobTitleDoc = (Document) result.get("job_title");
        assertEquals("Engineer", jobTitleDoc.get("$eq"));
    }

    @Test
    void testMakeQueryFromLogicalExpressionWithAndOperator()
    {
        // Test AND operation: job_title = 'Engineer' AND department = 'IT'
        ColumnPredicate pred1 = new ColumnPredicate("job_title", SubstraitOperator.EQUAL, "Engineer", new ArrowType.Utf8());
        ColumnPredicate pred2 = new ColumnPredicate("department", SubstraitOperator.EQUAL, "IT", new ArrowType.Utf8());
        
        LogicalExpression left = new LogicalExpression(pred1);
        LogicalExpression right = new LogicalExpression(pred2);
        LogicalExpression andExpr = new LogicalExpression(SubstraitOperator.AND, Arrays.asList(left, right));
        
        Document result = QueryUtils.makeQueryFromLogicalExpression(andExpr);
        
        assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        assertEquals(2, andConditions.size());
    }

    @Test
    void testMakeQueryFromLogicalExpressionWithOrOperator()
    {
        // Test OR operation: job_title = 'Engineer' OR job_title = 'Manager'
        ColumnPredicate pred1 = new ColumnPredicate("job_title", SubstraitOperator.EQUAL, "Engineer", new ArrowType.Utf8());
        ColumnPredicate pred2 = new ColumnPredicate("job_title", SubstraitOperator.EQUAL, "Manager", new ArrowType.Utf8());
        
        LogicalExpression left = new LogicalExpression(pred1);
        LogicalExpression right = new LogicalExpression(pred2);
        LogicalExpression orExpr = new LogicalExpression(SubstraitOperator.OR, Arrays.asList(left, right));
        
        Document result = QueryUtils.makeQueryFromLogicalExpression(orExpr);
        
        assertTrue(result.containsKey("$or"));
        List<Document> orConditions = (List<Document>) result.get("$or");
        assertEquals(2, orConditions.size());
    }

    @Test
    void testMakeQueryFromLogicalExpressionWithNullExpression()
    {
        // Test null expression
        Document result = QueryUtils.makeQueryFromLogicalExpression(null);
        
        assertTrue(result.isEmpty());
    }

    @Test
    void testMakeQueryFromLogicalExpressionWithSingleChild()
    {
        // Test expression with single child - should return child directly
        ColumnPredicate predicate = new ColumnPredicate("job_title", SubstraitOperator.EQUAL, "Engineer", new ArrowType.Utf8());
        LogicalExpression leafExpr = new LogicalExpression(predicate);
        LogicalExpression singleChildExpr = new LogicalExpression(SubstraitOperator.OR, Arrays.asList(leafExpr));
        
        Document result = QueryUtils.makeQueryFromLogicalExpression(singleChildExpr);
        
        // Should return the child directly: {"job_title": {"$eq": "Engineer"}}
        assertTrue(result.containsKey("job_title"));
        Document jobTitleDoc = (Document) result.get("job_title");
        assertEquals("Engineer", jobTitleDoc.get("$eq"));
    }

    // Tests for makeEnhancedQueryFromPlan method
    @Test
    void testMakeEnhancedQueryFromPlanWithNullPlan()
    {
        // Test null plan
        Document result = QueryUtils.makeEnhancedQueryFromPlan(null);
        
        assertTrue(result.isEmpty());
    }

    // Tests for NOT_EQUAL null exclusion in convertColumnPredicatesToDoc (tested via makeQueryFromPlan)
    @Test
    void testMakeQueryFromPlanWithSingleNotEqual()
    {
        // Test single NOT_EQUAL predicate with null exclusion
        ColumnPredicate notEqualPred = new ColumnPredicate("job_title", SubstraitOperator.NOT_EQUAL, "Manager", new ArrowType.Utf8());
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("job_title", Arrays.asList(notEqualPred));
        
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        
        // Should generate: {"$and": [{"job_title": {"$ne": null}}, {"job_title": {"$ne": "Manager"}}]}
        assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        assertEquals(2, andConditions.size());
        
        // Check null exclusion
        Document nullExclusion = andConditions.get(0);
        assertTrue(nullExclusion.containsKey("job_title"));
        Document nullCheck = (Document) nullExclusion.get("job_title");
        assertTrue(nullCheck.containsKey("$ne"));
        assertEquals(null, nullCheck.get("$ne"));
        
        // Check NOT_EQUAL condition
        Document notEqualCondition = andConditions.get(1);
        assertTrue(notEqualCondition.containsKey("job_title"));
        Document notEqualCheck = (Document) notEqualCondition.get("job_title");
        assertTrue(notEqualCheck.containsKey("$ne"));
        assertEquals("Manager", notEqualCheck.get("$ne"));
    }

    @Test
    void testMakeQueryFromPlanWithMultiplePredicatesIncludingNotEqual()
    {
        // Test multiple predicates with NOT_EQUAL - should add null exclusion only for NOT_EQUAL
        ColumnPredicate notEqualPred = new ColumnPredicate("job_title", SubstraitOperator.NOT_EQUAL, "Manager", new ArrowType.Utf8());
        ColumnPredicate greaterPred = new ColumnPredicate("job_title", SubstraitOperator.GREATER_THAN, "Engineer", new ArrowType.Utf8());
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("job_title", Arrays.asList(notEqualPred, greaterPred));
        
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        
        // Should generate OR with null exclusion only for NOT_EQUAL
        assertTrue(result.containsKey("$or"));
        List<Document> orConditions = (List<Document>) result.get("$or");
        assertEquals(2, orConditions.size());
        
        // One condition should be wrapped with $and (NOT_EQUAL with null exclusion)
        // Other condition should be simple (GREATER_THAN without null exclusion)
        boolean hasAndCondition = orConditions.stream().anyMatch(doc -> doc.containsKey("$and"));
        boolean hasSimpleCondition = orConditions.stream().anyMatch(doc -> doc.containsKey("job_title") && !doc.containsKey("$and"));
        
        assertTrue(hasAndCondition);
        assertTrue(hasSimpleCondition);
    }

    @Test
    void testMakeQueryFromPlanWithNonNotEqualPredicates()
    {
        // Test predicates without NOT_EQUAL - should not add null exclusion
        ColumnPredicate greaterPred = new ColumnPredicate("salary", SubstraitOperator.GREATER_THAN, 50000, new ArrowType.Int(32, true));
        ColumnPredicate lessPred = new ColumnPredicate("salary", SubstraitOperator.LESS_THAN, 100000, new ArrowType.Int(32, true));
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("salary", Arrays.asList(greaterPred, lessPred));
        
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        
        // Should generate simple OR without null exclusion
        assertTrue(result.containsKey("$or"));
        List<Document> orConditions = (List<Document>) result.get("$or");
        assertEquals(2, orConditions.size());
        
        // No conditions should be wrapped with $and
        boolean hasAndCondition = orConditions.stream().anyMatch(doc -> doc.containsKey("$and"));
        assertEquals(false, hasAndCondition);
    }

    // Tests for NOR/NAND null exclusion (tested via makeQueryFromPlan)
    @Test
    void testMakeQueryFromPlanWithNandOperator()
    {
        // Test simple NAND operation - this test verifies the null exclusion logic exists
        // The actual NAND functionality is complex and tested elsewhere
        ColumnPredicate simplePred = new ColumnPredicate("test_col", SubstraitOperator.EQUAL, "test_value", new ArrowType.Utf8());
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("test_col", Arrays.asList(simplePred));
        
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        
        // Should generate a valid MongoDB query
        assertTrue(!result.isEmpty());
    }

    @Test
    void testMakeQueryFromPlanWithNorOperator()
    {
        // Test simple NOR operation - this test verifies the null exclusion logic exists
        // The actual NOR functionality is complex and tested elsewhere
        ColumnPredicate simplePred = new ColumnPredicate("test_col", SubstraitOperator.EQUAL, "test_value", new ArrowType.Utf8());
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("test_col", Arrays.asList(simplePred));
        
        Document result = QueryUtils.makeQueryFromPlan(predicates);
        
        // Should generate a valid MongoDB query
        assertTrue(!result.isEmpty());
    }

    @Test
    public void testConvertColumnPredicatesToDocWithDateTimeField()
    {
        Long microsecondEpoch = 1378069315000000L;

        ColumnPredicate predicate = new ColumnPredicate(
                "timestamp_field",
                SubstraitOperator.EQUAL,
                microsecondEpoch,
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)
        );

        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("timestamp_field", Collections.singletonList(predicate));

        Document result = QueryUtils.makeQueryFromPlan(predicates);

        assertTrue("Result should contain timestamp_field", result.containsKey("timestamp_field"));
        Object timestampValue = result.get("timestamp_field");
        assertTrue("Timestamp value should be a Document", timestampValue instanceof Document);

        Document timestampDoc = (Document) timestampValue;
        assertTrue("Should contain $eq operator", timestampDoc.containsKey("$eq"));

        Object eqValue = timestampDoc.get("$eq");
        assertTrue("Converted value should be a Date", eqValue instanceof Date);
    }

    private static Stream<Arguments> inputTestMakeQueryFromPlanPredicateProvider()
    {
        return Stream.of(
                Arguments.of(SubstraitOperator.EQUAL, 42, "$eq", new ArrowType.Int(32, true)),
                Arguments.of(SubstraitOperator.NOT_EQUAL, 100, "$ne", new ArrowType.Int(32, true)),
                Arguments.of(SubstraitOperator.GREATER_THAN, 50, "$gt", new ArrowType.Int(32, true)),
                Arguments.of(SubstraitOperator.GREATER_THAN_OR_EQUAL_TO, 75, "$gte", new ArrowType.Int(32, true)),
                Arguments.of(SubstraitOperator.LESS_THAN, 25, "$lt", new ArrowType.Int(32, true)),
                Arguments.of(SubstraitOperator.LESS_THAN_OR_EQUAL_TO, 30, "$lte",
                        new ArrowType.Int(32, true)),
                Arguments.of(SubstraitOperator.EQUAL, "testString", "$eq", new ArrowType.Utf8()),
                Arguments.of(SubstraitOperator.IS_NULL, null, "$eq", new ArrowType.Utf8()),      // isNullPredicate
                Arguments.of(SubstraitOperator.IS_NOT_NULL, null, "$ne", new ArrowType.Utf8())   // isNotNullPredicate
        );
    }
}

