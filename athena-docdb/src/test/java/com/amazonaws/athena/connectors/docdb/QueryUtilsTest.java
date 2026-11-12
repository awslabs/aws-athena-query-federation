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
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    // Tests for makeQueryFromLogicalExpression method
    @Test
    public void testMakeQueryFromLogicalExpressionWithLeafPredicate()
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
    public void testMakeQueryFromLogicalExpressionWithAndOperator()
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
    public void testMakeQueryFromLogicalExpressionWithOrOperator()
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
    public void testMakeQueryFromLogicalExpressionWithNullExpression()
    {
        // Test null expression
        Document result = QueryUtils.makeQueryFromLogicalExpression(null);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testMakeQueryFromLogicalExpressionWithSingleChild()
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

    @Test
    public void testMakeEnhancedQueryFromPlanWithNullPlan()
    {
        Document result = QueryUtils.makeEnhancedQueryFromPlan(null);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_SingleEqual()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE employee_name = 'John Doe'
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkazwYSzAYKlgU6kwUKGBIWChQUFRYXGBkaGxwdHh8gISIjJCUmJxKIAxKFAwoCCgAS1gIK0wIKAgoAErACCgNfaWQKAmlkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCg10aW1lc3RhbXBfY29sCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoJY291bnRfY29sCgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USewoEYgIQAQoEKgIQAQoECgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFigICGAEKBGICEAEKBGICEAEKBFoCEAEKBDoCEAEKBDoCEAEKBCoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEYAjoaChhtb25nb2RiX2Jhc2ljX2NvbGxlY3Rpb24aJhokGgQKAhABIgwaChIICgQSAggDIgAiDhoMCgpiCEpvaG4gRG9lGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("employee_name"));
        Document employeeDoc = (Document) result.get("employee_name");
        assertEquals("John Doe", employeeDoc.get("$eq"));
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_SingleNotEqual()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE job_title != 'Manager'
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFxoVCAEaEW5vdF9lcXVhbDphbnlfYW55Gs4GEssGCpUFOpIFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicShwMShAMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGiUaIxoECgIQASIMGgoSCAoEEgIIAyIAIg0aCwoJYgdNYW5hZ2VyGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        // Should include null exclusion for NOT_EQUAL
        Assertions.assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        assertEquals(2, andConditions.size());
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_GreaterThan()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE id > 100
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmd0OmFueV9hbnkaxwYSxAYKjgU6iwUKGBIWChQUFRYXGBkaGxwdHh8gISIjJCUmJxKAAxL9AgoCCgAS1gIK0wIKAgoAErACCgNfaWQKAmlkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCg10aW1lc3RhbXBfY29sCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoJY291bnRfY29sCgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USewoEYgIQAQoEKgIQAQoECgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFigICGAEKBGICEAEKBGICEAEKBFoCEAEKBDoCEAEKBDoCEAEKBCoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEYAjoaChhtb25nb2RiX2Jhc2ljX2NvbGxlY3Rpb24aHhocGgQKAhABIgwaChIICgQSAggBIgAiBhoECgIoZBoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgASA19pZBICaWQSCWlzX2FjdGl2ZRINZW1wbG95ZWVfbmFtZRIJam9iX3RpdGxlEgdhZGRyZXNzEglqb2luX2RhdGUSDXRpbWVzdGFtcF9jb2wSCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0Egljb3VudF9jb2wSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("id"));
        Document idDoc = (Document) result.get("id");
        assertEquals(100, idDoc.get("$gt"));
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_GreaterThanOrEqual()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE bonus >= 5000.0
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2d0ZTphbnlfYW55GuoGEucGCrEFOq4FChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSowMSoAMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGkEaPxoECgIQASIMGgoSCAoEEgIICiIAIikaJ1olCgRaAhABEhsKGcIBFgoQUMMAAAAAAAAAAAAAAAAAABAFGAEYAhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgASA19pZBICaWQSCWlzX2FjdGl2ZRINZW1wbG95ZWVfbmFtZRIJam9iX3RpdGxlEgdhZGRyZXNzEglqb2luX2RhdGUSDXRpbWVzdGFtcF9jb2wSCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0Egljb3VudF9jb2wSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("bonus"));
        Document bonusDoc = (Document) result.get("bonus");
        assertEquals(5000.0, bonusDoc.get("$gte"));
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_LessThan()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE code < 500
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmx0OmFueV9hbnkayAYSxQYKjwU6jAUKGBIWChQUFRYXGBkaGxwdHh8gISIjJCUmJxKBAxL+AgoCCgAS1gIK0wIKAgoAErACCgNfaWQKAmlkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCg10aW1lc3RhbXBfY29sCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoJY291bnRfY29sCgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USewoEYgIQAQoEKgIQAQoECgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFigICGAEKBGICEAEKBGICEAEKBFoCEAEKBDoCEAEKBDoCEAEKBCoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEYAjoaChhtb25nb2RiX2Jhc2ljX2NvbGxlY3Rpb24aHxodGgQKAhABIgwaChIICgQSAggNIgAiBxoFCgMo9AMaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAEgNfaWQSAmlkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEg10aW1lc3RhbXBfY29sEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIJY291bnRfY29sEgZhbW91bnQSB2JhbGFuY2USBHJhdGUSCmRpZmZlcmVuY2U=";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("code"));
        Document codeDoc = (Document) result.get("code");
        assertEquals(500, codeDoc.get("$lt"));
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_LessThanOrEqual()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE balance <= 10000
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2x0ZTphbnlfYW55GtQGEtEGCpsFOpgFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSjQMSigMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGisaKRoECgIQASIMGgoSCAoEEgIIESIAIhMaEVoPCgQ6AhABEgUKAyiQThgCGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("balance"));
        Document balanceDoc = (Document) result.get("balance");
        assertEquals(10000, balanceDoc.get("$lte"));
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_IsNull()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE address IS NULL
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2lzX251bGw6YW55Gr8GErwGCoYFOoMFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicS+AIS9QIKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGhYaFBoECgIQAiIMGgoSCAoEEgIIBSIAGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("address"));
        Document addressDoc = (Document) result.get("address");
        Assertions.assertTrue(addressDoc.containsKey("$eq"));
        assertNull(addressDoc.get("$eq"));
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_IsNotNull()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE salary IS NOT NULL
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEaD2lzX25vdF9udWxsOmFueRq/BhK8BgqGBTqDBQoYEhYKFBQVFhcYGRobHB0eHyAhIiMkJSYnEvgCEvUCCgIKABLWAgrTAgoCCgASsAIKA19pZAoCaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKDXRpbWVzdGFtcF9jb2wKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0Cgljb3VudF9jb2wKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRJ7CgRiAhABCgQqAhABCgQKAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEWgIQAQoEOgIQAQoEOgIQAQoEKgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQARgCOhoKGG1vbmdvZGJfYmFzaWNfY29sbGVjdGlvbhoWGhQaBAoCEAIiDBoKEggKBBICCAkiABoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgASA19pZBICaWQSCWlzX2FjdGl2ZRINZW1wbG95ZWVfbmFtZRIJam9iX3RpdGxlEgdhZGRyZXNzEglqb2luX2RhdGUSDXRpbWVzdGFtcF9jb2wSCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0Egljb3VudF9jb2wSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("salary"));
        Document salaryDoc = (Document) result.get("salary");
        Assertions.assertTrue(salaryDoc.containsKey("$ne"));
        assertNull(salaryDoc.get("$ne"));
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_MultipleEqualValues()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE job_title IN ('Engineer', 'Manager', 'Analyst')
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARoHb3I6Ym9vbBIVGhMIAhABGg1lcXVhbDphbnlfYW55GtwHEtkHCqMGOqAGChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSlQQSkgQKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGrIBGq8BGgQKAhABIjcaNRozCAEaBAoCEAEiDBoKEggKBBICCAQiACIbGhlaFwoEYgIQARINCguqAQhFbmdpbmVlchgCIjYaNBoyCAEaBAoCEAEiDBoKEggKBBICCAQiACIaGhhaFgoEYgIQARIMCgqqAQdNYW5hZ2VyGAIiNho0GjIIARoECgIQASIMGgoSCAoEEgIIBCIAIhoaGFoWCgRiAhABEgwKCqoBB0FuYWx5c3QYAhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgASA19pZBICaWQSCWlzX2FjdGl2ZRINZW1wbG95ZWVfbmFtZRIJam9iX3RpdGxlEgdhZGRyZXNzEglqb2luX2RhdGUSDXRpbWVzdGFtcF9jb2wSCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0Egljb3VudF9jb2wSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$or"));
        List<Document> orConditions = (List<Document>) result.get("$or");
        assertEquals(3, orConditions.size());

        // Verify each OR condition contains job_title with different values
        boolean hasEngineer = orConditions.stream().anyMatch(doc ->
                doc.containsKey("job_title") &&
                        ((Document) doc.get("job_title")).get("$eq").equals("Engineer"));
        boolean hasManager = orConditions.stream().anyMatch(doc ->
                doc.containsKey("job_title") &&
                        ((Document) doc.get("job_title")).get("$eq").equals("Manager"));
        boolean hasAnalyst = orConditions.stream().anyMatch(doc ->
                doc.containsKey("job_title") &&
                        ((Document) doc.get("job_title")).get("$eq").equals("Analyst"));

        Assertions.assertTrue(hasEngineer);
        Assertions.assertTrue(hasManager);
        Assertions.assertTrue(hasAnalyst);
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_EqualAndGreaterThan()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE (id = 100 OR id = 200) AND id > 50
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSDxoNCAEQARoHb3I6Ym9vbBIVGhMIAhACGg1lcXVhbDphbnlfYW55EhIaEAgCEAMaCmd0OmFueV9hbnkargcSqwcK9QU68gUKGBIWChQUFRYXGBkaGxwdHh8gISIjJCUmJxLnAxLkAwoCCgAS1gIK0wIKAgoAErACCgNfaWQKAmlkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCg10aW1lc3RhbXBfY29sCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoJY291bnRfY29sCgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USewoEYgIQAQoEKgIQAQoECgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFigICGAEKBGICEAEKBGICEAEKBFoCEAEKBDoCEAEKBDoCEAEKBCoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEKBGICEAEKBDoCEAEYAjoaChhtb25nb2RiX2Jhc2ljX2NvbGxlY3Rpb24ahAEagQEaBAoCEAEiVRpTGlEIARoECgIQASIiGiAaHggCGgQKAhABIgwaChIICgQSAggBIgAiBhoECgIoZCIjGiEaHwgCGgQKAhABIgwaChIICgQSAggBIgAiBxoFCgMoyAEiIhogGh4IAxoECgIQASIMGgoSCAoEEgIIASIAIgYaBAoCKDIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAEgNfaWQSAmlkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEg10aW1lc3RhbXBfY29sEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIJY291bnRfY29sEgZhbW91bnQSB2JhbGFuY2USBHJhdGUSCmRpZmZlcmVuY2U=";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        assertEquals(2, andConditions.size());
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_CrossColumnAnd()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE employee_name = 'John' AND job_title = 'Engineer'
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSFRoTCAIQARoNZXF1YWw6YW55X2FueRqFBxKCBwrMBTrJBQoYEhYKFBQVFhcYGRobHB0eHyAhIiMkJSYnEr4DErsDCgIKABLWAgrTAgoCCgASsAIKA19pZAoCaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKDXRpbWVzdGFtcF9jb2wKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0Cgljb3VudF9jb2wKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRJ7CgRiAhABCgQqAhABCgQKAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEWgIQAQoEOgIQAQoEOgIQAQoEKgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQARgCOhoKGG1vbmdvZGJfYmFzaWNfY29sbGVjdGlvbhpcGloaBAoCEAEiJhokGiIIARoECgIQASIMGgoSCAoEEgIIAyIAIgoaCAoGYgRKb2huIioaKBomCAEaBAoCEAEiDBoKEggKBBICCAQiACIOGgwKCmIIRW5naW5lZXIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAEgNfaWQSAmlkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEg10aW1lc3RhbXBfY29sEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIJY291bnRfY29sEgZhbW91bnQSB2JhbGFuY2USBHJhdGUSCmRpZmZlcmVuY2U=";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        assertEquals(2, andConditions.size());
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_CrossColumnOr()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE employee_name = 'John' OR job_title = 'Manager'
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARoHb3I6Ym9vbBIVGhMIAhABGg1lcXVhbDphbnlfYW55GoUHEoIHCswFOskFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSvgMSuwMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGlwaWhoECgIQASImGiQaIggBGgQKAhABIgwaChIICgQSAggDIgAiChoICgZiBEpvaG4iKhooGiYIARoECgIQASIMGgoSCAoEEgIIBCIAIg4aDAoKYghFbmdpbmVlchoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgASA19pZBICaWQSCWlzX2FjdGl2ZRINZW1wbG95ZWVfbmFtZRIJam9iX3RpdGxlEgdhZGRyZXNzEglqb2luX2RhdGUSDXRpbWVzdGFtcF9jb2wSCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0Egljb3VudF9jb2wSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$or"));
        List<Document> orConditions = (List<Document>) result.get("$or");
        assertEquals(2, orConditions.size());
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_NestedAndOr()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE (employee_name = 'John' OR employee_name = 'Jane') AND (job_title = 'Engineer' OR job_title = 'Manager')
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSDxoNCAEQARoHb3I6Ym9vbBIVGhMIAhACGg1lcXVhbDphbnlfYW55GvYHEvMHCr0GOroGChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSrwQSrAQKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGswBGskBGgQKAhABIlwaWhpYCAEaBAoCEAEiJhokGiIIAhoECgIQASIMGgoSCAoEEgIIAyIAIgoaCAoGYgRKb2huIiYaJBoiCAIaBAoCEAEiDBoKEggKBBICCAMiACIKGggKBmIESmFuZSJjGmEaXwgBGgQKAhABIioaKBomCAIaBAoCEAEiDBoKEggKBBICCAQiACIOGgwKCmIIRW5naW5lZXIiKRonGiUIAhoECgIQASIMGgoSCAoEEgIIBCIAIg0aCwoJYgdNYW5hZ2VyGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        assertEquals(2, andConditions.size());

        // Each AND condition should be an OR
        for (Document condition : andConditions) {
            Assertions.assertTrue(condition.containsKey("$or"));
        }
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_NotIn()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE job_title NOT IN ('Intern', 'Contractor')
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIbm90OmJvb2wSDxoNCAEQARoHb3I6Ym9vbBIVGhMIAhACGg1lcXVhbDphbnlfYW55GrMHErAHCvoFOvcFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicS7AMS6QMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGokBGoYBGgQKAhABIn4afBp6CAEaBAoCEAEiNRozGjEIAhoECgIQASIMGgoSCAoEEgIIBCIAIhkaF1oVCgRiAhABEgsKCaoBBkludGVybhgCIjkaNxo1CAIaBAoCEAEiDBoKEggKBBICCAQiACIdGhtaGQoEYgIQARIPCg2qAQpDb250cmFjdG9yGAIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAEgNfaWQSAmlkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEg10aW1lc3RhbXBfY29sEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIJY291bnRfY29sEgZhbW91bnQSB2JhbGFuY2USBHJhdGUSCmRpZmZlcmVuY2U=";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        assertEquals(2, andConditions.size());

        Document nullExclusion = andConditions.get(0);
        Assertions.assertTrue(nullExclusion.containsKey("job_title"));
        Document nullCheck = (Document) nullExclusion.get("job_title");
        Assertions.assertTrue(nullCheck.containsKey("$ne"));
        assertNull(nullCheck.get("$ne"));

        Document norCondition = andConditions.get(1);
        Assertions.assertTrue(norCondition.containsKey("$nor"));
        List<Document> norValues = (List<Document>) norCondition.get("$nor");
        assertEquals(2, norValues.size());

        boolean hasIntern = norValues.stream().anyMatch(doc ->
                doc.containsKey("job_title") &&
                        ((Document) doc.get("job_title")).get("$eq").equals("Intern"));
        boolean hasContractor = norValues.stream().anyMatch(doc ->
                doc.containsKey("job_title") &&
                        ((Document) doc.get("job_title")).get("$eq").equals("Contractor"));

        Assertions.assertTrue(hasIntern);
        Assertions.assertTrue(hasContractor);
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_NotAnd()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE NOT (employee_name = 'John' AND job_title = 'Manager')
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIbm90OmJvb2wSEBoOCAEQARoIYW5kOmJvb2wSFRoTCAIQAhoNZXF1YWw6YW55X2FueRqSBxKPBwrZBTrWBQoYEhYKFBQVFhcYGRobHB0eHyAhIiMkJSYnEssDEsgDCgIKABLWAgrTAgoCCgASsAIKA19pZAoCaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKDXRpbWVzdGFtcF9jb2wKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0Cgljb3VudF9jb2wKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRJ7CgRiAhABCgQqAhABCgQKAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEWgIQAQoEOgIQAQoEOgIQAQoEKgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQARgCOhoKGG1vbmdvZGJfYmFzaWNfY29sbGVjdGlvbhppGmcaBAoCEAEiXxpdGlsIARoECgIQASImGiQaIggCGgQKAhABIgwaChIICgQSAggDIgAiChoICgZiBEpvaG4iKRonGiUIAhoECgIQASIMGgoSCAoEEgIIBCIAIg0aCwoJYgdNYW5hZ2VyGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        // Should include null exclusions + NOR condition
        Assertions.assertTrue(andConditions.size() >= 2);

        // Should contain $nor condition
        boolean hasNor = andConditions.stream().anyMatch(doc -> doc.containsKey("$nor"));
        Assertions.assertTrue(hasNor);
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_NotOr()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE NOT (employee_name = 'John' OR job_title = 'Manager')
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIbm90OmJvb2wSDxoNCAEQARoHb3I6Ym9vbBIVGhMIAhACGg1lcXVhbDphbnlfYW55GpIHEo8HCtkFOtYFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSywMSyAMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGmkaZxoECgIQASJfGl0aWwgBGgQKAhABIiYaJBoiCAIaBAoCEAEiDBoKEggKBBICCAMiACIKGggKBmIESm9obiIpGicaJQgCGgQKAhABIgwaChIICgQSAggEIgAiDRoLCgliB01hbmFnZXIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAEgNfaWQSAmlkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEg10aW1lc3RhbXBfY29sEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIJY291bnRfY29sEgZhbW91bnQSB2JhbGFuY2USBHJhdGUSCmRpZmZlcmVuY2U=";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("$and"));
        List<Document> andConditions = (List<Document>) result.get("$and");
        // Should include null exclusions + NOR condition
        Assertions.assertTrue(andConditions.size() >= 2);

        // Should contain $nor condition
        boolean hasNor = andConditions.stream().anyMatch(doc -> doc.containsKey("$nor"));
        Assertions.assertTrue(hasNor);
    }

    @Test
    public void testMakeEnhancedQueryFromPlan_TimestampColumn()
    {
        // SQL: SELECT * FROM mongodb_basic_collection WHERE timestamp_col > TIMESTAMP '2023-01-01 00:00:00'
        String substraitPlanString = "ChwIARIYL2Z1bmN0aW9uc19kYXRldGltZS55YW1sEhAaDggBGgpndDpwdHNfcHRzGtsGEtgGCqIFOp8FChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSlAMSkQMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGjIaMBoECgIQASIMGgoSCAoEEgIIByIAIhoaGFoWCgWKAgIYAhILCglwgIC1oIil/AIYAhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgASA19pZBICaWQSCWlzX2FjdGl2ZRINZW1wbG95ZWVfbmFtZRIJam9iX3RpdGxlEgdhZGRyZXNzEglqb2luX2RhdGUSDXRpbWVzdGFtcF9jb2wSCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0Egljb3VudF9jb2wSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==";

        final QueryPlan queryPlan = createQueryPlan(substraitPlanString);
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());

        Document result = QueryUtils.makeEnhancedQueryFromPlan(plan);

        assertNotNull(result);
        Assertions.assertTrue(result.containsKey("timestamp_col"));
        Document timestampDoc = (Document) result.get("timestamp_col");
        Assertions.assertTrue(timestampDoc.containsKey("$gt"));
    }

    private QueryPlan createQueryPlan(String substraitPlanString)
    {
        return new QueryPlan("1.0", substraitPlanString);
    }
}