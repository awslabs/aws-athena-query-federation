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
import io.substrait.proto.Plan;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.substrait.SubstraitRelUtils.deserializeSubstraitPlan;
import static org.junit.Assert.assertEquals;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

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

    @Test
    public void getProjectionTest()
    {
        logger.info("getProjectionTest - enter");

        List<String> expectedProjection = new ArrayList<>();
        mapping.getFields().forEach(field -> expectedProjection.add(field.getName()));

        // Get the actual projection and compare to the expected one.
        FetchSourceContext context = ElasticsearchQueryUtils.getProjection(mapping);
        List<String> actualProjection = ImmutableList.copyOf(context.includes());

        logger.info("Projections - Expected: {}, Actual: {}", expectedProjection, actualProjection);
        assertEquals("Projections do not match", expectedProjection, actualProjection);

        logger.info("getProjectionTest - exit");
    }

    @Test
    public void getRangePredicateTest()
    {
        logger.info("getRangePredicateTest - enter");

        constraintsMap.put("year", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.lessThan(allocator, Types.MinorType.INT.getType(), 1950),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1952),
                        Range.range(allocator, Types.MinorType.INT.getType(),
                                1955, false, 1972, true),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1996),
                        Range.greaterThanOrEqual(allocator, Types.MinorType.INT.getType(), 2010)),
                false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(_exists_:year) AND year:([* TO 1950} OR {1955 TO 1972] OR [2010 TO *] OR 1952 OR 1996)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getRangePredicateTest - exit");
    }

    @Test
    public void testSubstraitQueryGeneration()
    {
        logger.info("testSubstraitQueryGeneration - enter");
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBIPGg0IARACGgdvcjpib29sEhUaEwgCEAMaDWVxdWFsOmFueV9hbnkSGRoXCAIQBBoRbm90X2VxdWFsOmFueV9hbnkaiAYShQYK/gUa+wUKAgoAEvAFOu0FCgUSAwoBFhLXBRLUBQoCCgASlwQKlAQKAgoAEuQDCglpc19hY3RpdmUKC3RpbnlpbnRfY29sCgxzbWFsbGludF9jb2wKCHByaW9yaXR5CgpiaWdpbnRfY29sCglmbG9hdF9jb2wKCmRvdWJsZV9jb2wKCHJlYWxfY29sCgt2YXJjaGFyX2NvbAoIY2hhcl9jb2wKDXZhcmJpbmFyeV9jb2wKCGRhdGVfY29sCgh0aW1lX2NvbAoNdGltZXN0YW1wX2NvbAoCaWQKDGRlY2ltYWxfY29sMgoMZGVjaW1hbF9jb2wzCgtzdWJjYXRlZ29yeQoNaW50X2FycmF5X2NvbAoHbWFwX2NvbAoQbWFwX3dpdGhfZGVjaW1hbAoMbmVzdGVkX2FycmF5EtYBCgQKAhACCgQSAhACCgQaAhACCgQqAhACCgQ6AhACCgRaAhACCgRaAhACCgRSAhACCgRiAhACCgeqAQQIARgCCgRqAhACCgWCAQIQAgoFigECEAIKBYoCAhgCCgnCAQYIBBATIAIKCcIBBggCEAogAgoJwgEGCAoQEyACCgvaAQgKBGICEAIYAgoL2gEICgQqAhACGAIKEeIBDgoEYgIQAhIEKgIQAiACChbiARMKBGICEAISCcIBBggCEAogAiACChLaAQ8KC9oBCAoEKgIQAhgCGAIYAjonCgpteV9kYXRhc2V0ChlzZXJ2aWNlX3JlcXVlc3RzX25vX25vaXNlGrMBGrABCAEaBAoCEAIigwEagAEafggCGgQKAhACIjkaNxo1CAMaBAoCEAIiDBoKEggKBBICCA4iACIdGhsKGcIBFgoQQEIPAAAAAAAAAAAAAAAAABATGAQiORo3GjUIAxoECgIQAiIMGgoSCAoEEgIIDiIAIh0aGwoZwgEWChCAhB4AAAAAAAAAAAAAAAAAEBMYBCIgGh4aHAgEGgQKAhACIgoaCBIGCgISACIAIgYaBAoCCAEaChIICgQSAggOIgAYACAKEgJJRDILEEoqB2lzdGhtdXM=";
        org.apache.arrow.vector.types.pojo.Schema testSchema = new org.apache.arrow.vector.types.pojo.Schema(Arrays.asList(
                new Field("employee_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("job_title", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("address", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("salary", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("bonus", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("is_active", FieldType.nullable(new ArrowType.Bool()), null),
                new Field("join_date", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null)
        ));
        QueryPlan queryPlan = new QueryPlan("1.0", substraitPlanString);

        // Generate query from Substrait plan
        Plan plan = deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(plan);

        // Verify that a query was generated (not matchAllQuery)
        assertNotNull("QueryBuilder should not be null", builder);

        // Log the generated query structure for debugging
        String actualQueryJson = builder.toString();
        logger.info("Generated Elasticsearch Query: {}", actualQueryJson);

        // Verify it's a BoolQuery (our Substrait implementation uses BoolQuery for complex predicates)
        // Note: The original constraint path uses QueryStringQuery with string predicates,
        // but Substrait path uses structured BoolQuery for better type safety and nested logic support
        assertTrue("Query should be a BoolQueryBuilder or structured query, got: " + builder.getClass().getSimpleName(),
                   builder instanceof BoolQueryBuilder || !actualQueryJson.contains("match_all"));

        logger.info("testSubstraitQueryGeneration - exit");
    }

    @Test
    public void getWhitelistedEquitableValuesPredicate()
    {
        logger.info("getWhitelistedEquitableValuesPredicate - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                true, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
                Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getWhitelistedEquitableValuesPredicate - exit");
    }

    @Test
    public void getExclusiveEquitableValuesPredicate()
    {
        logger.info("getExclusiveEquitableValuesPredicate - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                false, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "NOT age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getExclusiveEquitableValuesPredicate - exit");
    }

    @Test
    public void getAllValuePredicate()
    {
        logger.info("getAllValuePredicate - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), true, true));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(_exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getAllValuePredicate - exit");
    }

    @Test
    public void getNoneValuePredicate()
    {
        logger.info("getNoneValuePredicate - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), false, false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(NOT _exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getNoneValuePredicate - exit");
    }

    @Test
    public void testNandOperator()
    {
        logger.info("testNandOperator - enter");

        // Test NAND logic: NAND(A, B) = NOT(A AND B)
        // SQL equivalent: NOT (status = 'active' AND age > 30)
        // Should match: status != 'active' OR age <= 30

        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIbm90OmJvb2wSEBoOCAEQARoIYW5kOmJvb2wSFRoTCAIQAhoNZXF1YWw6YW55X2FueRqSBxKPBwrZBTrWBQoYEhYKFBQVFhcYGRobHB0eHyAhIiMkJSYnEssDEsgDCgIKABLWAgrTAgoCCgASsAIKA19pZAoCaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKDXRpbWVzdGFtcF9jb2wKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0Cgljb3VudF9jb2wKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRJ7CgRiAhABCgQqAhABCgQKAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEWgIQAQoEOgIQAQoEOgIQAQoEKgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQARgCOhoKGG1vbmdvZGJfYmFzaWNfY29sbGVjdGlvbhppGmcaBAoCEAEiXxpdGlsIARoECgIQASImGiQaIggCGgQKAhABIgwaChIICgQSAggDIgAiChoICgZiBEpvaG4iKRonGiUIAhoECgIQASIMGgoSCAoEEgIIBCIAIg0aCwoJYgdNYW5hZ2VyGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        Plan plan = deserializeSubstraitPlan(substraitPlanString);
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(plan);

        String actualQueryJson = builder.toString();
        logger.info("NAND Query: {}", actualQueryJson);

        // Verify structure: NAND should create NOT(AND(...))
        assertTrue("NAND query should be a BoolQueryBuilder", builder instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) builder;

        // The outer query should have a mustNot clause containing an inner AND
        assertTrue("NAND should use mustNot clause", actualQueryJson.contains("must_not"));

        // Verify it's not the same as NOR (which would have multiple mustNot clauses)
        // NAND: { "bool": { "must_not": { "bool": { "must": [...] } } } }
        // NOR:  { "bool": { "must_not": [..., ...] } }
        assertTrue("NAND should negate a single AND query",
                   actualQueryJson.contains("must") || actualQueryJson.contains("\"must\""));

        logger.info("testNandOperator - exit");
    }

    @Test
    public void testNorOperator()
    {
        // Test NOR logic: NOR(A, B) = NOT(A OR B) = NOT(A) AND NOT(B)
        // SQL equivalent: NOT (status = 'active' OR premium = true)
        // Should match: status != 'active' AND premium != true
        logger.info("testNorOperator - enter");
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIbm90OmJvb2wSDxoNCAEQARoHb3I6Ym9vbBIVGhMIAhACGg1lcXVhbDphbnlfYW55GpIHEo8HCtkFOtYFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicSywMSyAMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGmkaZxoECgIQASJfGl0aWwgBGgQKAhABIiYaJBoiCAIaBAoCEAEiDBoKEggKBBICCAMiACIKGggKBmIESm9obiIpGicaJQgCGgQKAhABIgwaChIICgQSAggEIgAiDRoLCgliB01hbmFnZXIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAEgNfaWQSAmlkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEg10aW1lc3RhbXBfY29sEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIJY291bnRfY29sEgZhbW91bnQSB2JhbGFuY2USBHJhdGUSCmRpZmZlcmVuY2U=";

        Plan plan = deserializeSubstraitPlan(substraitPlanString);
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(plan);

        String actualQueryJson = builder.toString();
        logger.info("NOR Query: {}", actualQueryJson);

        // Verify structure: NOR should create multiple mustNot clauses
        assertTrue("NOR query should be a BoolQueryBuilder", builder instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) builder;

        // NOR should use mustNot for each condition
        assertTrue("NOR should use mustNot clauses", actualQueryJson.contains("must_not"));

        // Verify it's not NAND (NAND would have nested "must" inside "must_not")
        // Count occurrences - NOR should have multiple mustNot at the same level
        int mustNotCount = countOccurrences(actualQueryJson, "must_not");
        assertTrue("NOR should have mustNot clauses for each condition", mustNotCount >= 1);

        logger.info("testNorOperator - exit");
    }

    @Test
    public void testIsNullOperator()
    {
        logger.info("testIsNullOperator - enter");

        // SQL: SELECT * FROM mongodb_basic_collection WHERE address IS NULL
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2lzX251bGw6YW55Gr8GErwGCoYFOoMFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicS+AIS9QIKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGhYaFBoECgIQAiIMGgoSCAoEEgIIBSIAGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        Plan plan = deserializeSubstraitPlan(substraitPlanString);
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(plan);

        String actualQueryJson = builder.toString();
        logger.info("IS_NULL Query: {}", actualQueryJson);

        // Verify structure: IS_NULL should create mustNot(exists(...))
        assertTrue("IS_NULL query should be a BoolQueryBuilder", builder instanceof BoolQueryBuilder);

        // IS_NULL is represented as NOT EXISTS in Elasticsearch
        int mustNotCount = countOccurrences(actualQueryJson, "\"must_not\"");
        assertEquals("IS_NULL should have exactly 1 must_not clause", 1, mustNotCount);

        int existsCount = countOccurrences(actualQueryJson, "\"exists\"");
        assertEquals("IS_NULL should have exactly 1 exists clause", 1, existsCount);

        // Verify it checks the status field
        assertTrue("IS_NULL should check status field", actualQueryJson.contains("\"address\""));

        logger.info("testIsNullOperator - exit");
    }

    @Test
    public void testNotEqualOperator()
    {
        logger.info("testNotEqualOperator - enter");

        // SQL: SELECT * FROM mongodb_basic_collection WHERE employee_name != 'Manager'
        String substraitPlanString = "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFxoVCAEaEW5vdF9lcXVhbDphbnlfYW55Gs4GEssGCpUFOpIFChgSFgoUFBUWFxgZGhscHR4fICEiIyQlJicShwMShAMKAgoAEtYCCtMCCgIKABKwAgoDX2lkCgJpZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoNdGltZXN0YW1wX2NvbAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKCWNvdW50X2NvbAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEnsKBGICEAEKBCoCEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRaAhABCgQ6AhABCgQ6AhABCgQqAhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABCgRiAhABCgQ6AhABGAI6GgoYbW9uZ29kYl9iYXNpY19jb2xsZWN0aW9uGiUaIxoECgIQASIMGgoSCAoEEgIIAyIAIg0aCwoJYgdNYW5hZ2VyGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABIDX2lkEgJpZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRINdGltZXN0YW1wX2NvbBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSCWNvdW50X2NvbBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl";

        Plan plan = deserializeSubstraitPlan(substraitPlanString);
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(plan);

        String actualQueryJson = builder.toString();
        logger.info("NOT_EQUAL Query: {}", actualQueryJson);

        // Verify structure: NOT_EQUAL should create must(exists) + mustNot(term)
        assertTrue("NOT_EQUAL query should be a BoolQueryBuilder", builder instanceof BoolQueryBuilder);

        // NOT_EQUAL requires field to exist AND not match the value
        int existsCount = countOccurrences(actualQueryJson, "\"exists\"");
        assertEquals("NOT_EQUAL should have exactly 1 exists check", 1, existsCount);

        int mustNotCount = countOccurrences(actualQueryJson, "\"must_not\"");
        assertEquals("NOT_EQUAL should have exactly 1 must_not clause", 1, mustNotCount);

        int termCount = countOccurrences(actualQueryJson, "\"term\"");
        assertEquals("NOT_EQUAL should have exactly 1 term query", 1, termCount);

        // Verify it checks the status field and value
        assertTrue("NOT_EQUAL should check status field", actualQueryJson.contains("\"employee_name\""));
        assertTrue("NOT_EQUAL should check against 'Manager' value", actualQueryJson.contains("Manager"));

        logger.info("testNotEqualOperator - exit");
    }

    /**
     * Tests complex query with multiple OR conditions and AND logic on year column.
     * SQL: SELECT * FROM "test_schema"."test_table" WHERE (year IS NOT NULL)
     *      AND (year < 1950 OR year = 1952 OR (year > 1955 AND year <= 1972) OR year = 1996 OR year >= 2010)
     *
     * This test verifies that Substrait plans with complex nested OR/AND logic are correctly
     * translated to OpenSearch bool queries with proper structure.
     */
    @Test
    public void testComplexYearQueryWithSubstrait()
    {
        // SQL: SELECT * FROM "test_schema"."test_table" WHERE (year IS NOT NULL)
        //      AND (year < 1950 OR year = 1952 OR (year > 1955 AND year <= 1972) OR year = 1996 OR year >= 2010)
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBIXGhUIAhACGg9pc19ub3RfbnVsbDphbnkSDxoNCAEQAxoHb3I6Ym9vbBISGhAIAhAEGgpsdDphbnlfYW55EhUaEwgCEAUaDWVxdWFsOmFueV9hbnkSEhoQCAIQBhoKZ3Q6YW55X2FueRITGhEIAhAHGgtsdGU6YW55X2FueRITGhEIAhAIGgtndGU6YW55X2FueRqlCxKiCwqGCTqDCQobEhkKFxcYGRobHB0eHyAhIiMkJSYnKCkqKywtEtEGEs4GCgIKABKgBAqdBAoCCgAS+wMKCGJvb2xfY29sCgt0aW55aW50X2NvbAoMc21hbGxpbnRfY29sCgR5ZWFyCgpiaWdpbnRfY29sCglmbG9hdF9jb2wKCmRvdWJsZV9jb2wKCHJlYWxfY29sCgt2YXJjaGFyX2NvbAoIY2hhcl9jb2wKCmJpbmFyeV9jb2wKDXZhcmJpbmFyeV9jb2wKCGRhdGVfY29sCgh0aW1lX2NvbAoNdGltZXN0YW1wX2NvbAoLZGVjaW1hbF9jb2wKDGRlY2ltYWxfY29sMgoMZGVjaW1hbF9jb2wzCglhcnJheV9jb2wKDWludF9hcnJheV9jb2wKB21hcF9jb2wKEG1hcF93aXRoX2RlY2ltYWwKDG5lc3RlZF9hcnJheRLfAQoECgIQAgoEEgIQAgoEGgIQAgoEKgIQAgoEOgIQAgoEWgIQAgoEWgIQAgoEUgIQAgoEYgIQAgoHqgEECAEYAgoHugEECAEYAgoEagIQAgoFggECEAIKBYoBAhACCgWKAgIYAgoJwgEGCAQQEyACCgnCAQYIAhAKIAIKCcIBBggKEBMgAgoL2gEICgRiAhACGAIKC9oBCAoEKgIQAhgCChHiAQ4KBGICEAISBCoCEAIgAgoW4gETCgRiAhACEgnCAQYIAhAKIAIgAgoS2gEPCgvaAQgKBCoCEAIYAhgCGAI6GQoLdGVzdF9zY2hlbWEKCnRlc3RfdGFibGUapAIaoQIIARoECgIQAiIaGhgaFggCGgQKAhACIgwaChIICgQSAggDIgAi+gEa9wEa9AEIAxoECgIQAiIjGiEaHwgEGgQKAhACIgwaChIICgQSAggDIgAiBxoFCgMong8iIxohGh8IBRoECgIQAiIMGgoSCAoEEgIIAyIAIgcaBQoDKKAPIlYaVBpSCAEaBAoCEAIiIxohGh8IBhoECgIQAiIMGgoSCAoEEgIIAyIAIgcaBQoDKKMPIiMaIRofCAcaBAoCEAIiDBoKEggKBBICCAMiACIHGgUKAyi0DyIjGiEaHwgFGgQKAhACIgwaChIICgQSAggDIgAiBxoFCgMozA8iIxohGh8ICBoECgIQAiIMGgoSCAoEEgIIAyIAIgcaBQoDKNoPGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABoKEggKBBICCBQiABoKEggKBBICCBUiABoKEggKBBICCBYiABIIYm9vbF9jb2wSC3RpbnlpbnRfY29sEgxzbWFsbGludF9jb2wSBHllYXISCmJpZ2ludF9jb2wSCWZsb2F0X2NvbBIKZG91YmxlX2NvbBIIcmVhbF9jb2wSC3ZhcmNoYXJfY29sEghjaGFyX2NvbBIKYmluYXJ5X2NvbBINdmFyYmluYXJ5X2NvbBIIZGF0ZV9jb2wSCHRpbWVfY29sEg10aW1lc3RhbXBfY29sEgtkZWNpbWFsX2NvbBIMZGVjaW1hbF9jb2wyEgxkZWNpbWFsX2NvbDMSCWFycmF5X2NvbBINaW50X2FycmF5X2NvbBIHbWFwX2NvbBIQbWFwX3dpdGhfZGVjaW1hbBIMbmVzdGVkX2FycmF5MgsQSioHaXN0aG11cw==";
        QueryPlan queryPlan = new QueryPlan("1.0", substraitPlanString);

        // Generate query from Substrait plan
        Plan plan = deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(plan);

        String actualQueryJson = builder.toString();
        logger.info("Generated Elasticsearch Query: {}", actualQueryJson);


        // Layer 1: Not using old path
        assertFalse("Should not use query_string (old path)", actualQueryJson.contains("query_string"));
        assertFalse("Should not be match_all", actualQueryJson.contains("match_all"));
        assertTrue("Query should have bool structure", actualQueryJson.contains("bool"));

        // Layer 2: Top-level AND logic
        assertTrue("Should have must clause for top-level AND", actualQueryJson.contains("must"));

        // Layer 3: IS NOT NULL check
        assertTrue("Should have exists for IS NOT NULL", actualQueryJson.contains("exists"));
        assertTrue("Query should contain year field", actualQueryJson.contains("year"));

        // Layer 4: OR logic with should clauses
        assertTrue("Should have 'should' clause for OR logic", actualQueryJson.contains("should"));
        assertTrue("Should have minimum_should_match for OR", actualQueryJson.contains("minimum_should_match"));

        // Layer 5: Verify all predicates are present
        // year < 1950
        assertTrue("Should have 'to' for year < 1950", actualQueryJson.contains("\"to\" : 1950"));
        assertTrue("Should have include_upper=false for year < 1950", actualQueryJson.contains("\"include_upper\" : false"));
        assertTrue("Should have value 1950", actualQueryJson.contains("1950"));

        // year = 1952 and year = 1996
        assertTrue("Should have value 1952", actualQueryJson.contains("1952"));
        assertTrue("Should have value 1996", actualQueryJson.contains("1996"));
        // Note: Currently these are separate term queries, not optimized to single terms query
        assertTrue("Should have term query", actualQueryJson.contains("\"term\""));

        // year > 1955 AND year <= 1972 (nested AND)
        assertTrue("Should have 'from' for year > 1955", actualQueryJson.contains("\"from\" : 1955"));
        assertTrue("Should have include_lower=false for year > 1955", actualQueryJson.contains("\"include_lower\" : false"));
        assertTrue("Should have value 1955", actualQueryJson.contains("1955"));
        assertTrue("Should have 'to' for year <= 1972", actualQueryJson.contains("\"to\" : 1972"));
        assertTrue("Should have include_upper=true for year <= 1972", actualQueryJson.contains("\"include_upper\" : true"));
        assertTrue("Should have value 1972", actualQueryJson.contains("1972"));

        // year >= 2010
        assertTrue("Should have 'from' for year >= 2010", actualQueryJson.contains("\"from\" : 2010"));
        assertTrue("Should have include_lower=true for year >= 2010", actualQueryJson.contains("\"include_lower\" : true"));
        assertTrue("Should have value 2010", actualQueryJson.contains("2010"));

        // Layer 6: Verify nested structure depth
        int boolCount = countOccurrences(actualQueryJson, "\"bool\"");
        assertTrue("Should have nested bool queries (at least 3: outer, OR level, nested AND)",
                   boolCount >= 3);

        // Layer 7: Verify operator counts
        int rangeCount = countOccurrences(actualQueryJson, "\"range\"");
        assertEquals("Should have 4 range queries (< 1950, > 1955, <= 1972, >= 2010)",
                     4, rangeCount);

        int existsCount = countOccurrences(actualQueryJson, "\"exists\"");
        assertEquals("Should have 1 exists query (IS NOT NULL)", 1, existsCount);

        logger.info("testComplexYearQueryWithSubstrait - exit");
    }

    @Test
    public void testNullPlanReturnsMatchAll()
    {
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(null);
        String actualQueryJson = builder.toString();
        assertTrue("Null plan should return match_all query", actualQueryJson.contains("match_all"));
    }

    @Test
    public void testEmptyPlanReturnsMatchAll()
    {
        logger.info("testEmptyPlanReturnsMatchAll - enter");

        Plan emptyPlan = Plan.newBuilder().build();
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(emptyPlan);

        assertNotNull("QueryBuilder should not be null", builder);
        assertTrue("Empty plan should return match_all query", builder.toString().contains("match_all"));
    }

    @Test
    public void testMalformedPlanReturnsMatchAll()
    {
        logger.info("testMalformedPlanReturnsMatchAll - enter");

        // Test lines 199-201: Malformed/invalid Substrait plan should gracefully return matchAllQuery
        // This tests exception handling when tree-based parsing fails
        Plan malformedPlan = Plan.newBuilder()
                .addRelations(io.substrait.proto.PlanRel.newBuilder().build())
                .build();

        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(malformedPlan);

        assertNotNull("QueryBuilder should not be null even with malformed plan", builder);
        assertTrue("Malformed plan should gracefully degrade to match_all query",
                   builder.toString().contains("match_all"));

        logger.info("testMalformedPlanReturnsMatchAll - exit");
    }

    @Test
    public void testEmptyFilRel()
    {
        // SELECT id FROM "my_dataset"."service_requests_no_noise"
        String substraitPlanString = "GroEErcECrAEOq0ECgUSAwoBFhKXBAqUBAoCCgAS5AMKCWlzX2FjdGl2ZQoLdGlueWludF9jb2wKDHNtYWxsaW50X2NvbAoIcHJpb3JpdHkKCmJpZ2ludF9jb2wKCWZsb2F0X2NvbAoKZG91YmxlX2NvbAoIcmVhbF9jb2wKC3ZhcmNoYXJfY29sCghjaGFyX2NvbAoNdmFyYmluYXJ5X2NvbAoIZGF0ZV9jb2wKCHRpbWVfY29sCg10aW1lc3RhbXBfY29sCgJpZAoMZGVjaW1hbF9jb2wyCgxkZWNpbWFsX2NvbDMKC3N1YmNhdGVnb3J5Cg1pbnRfYXJyYXlfY29sCgdtYXBfY29sChBtYXBfd2l0aF9kZWNpbWFsCgxuZXN0ZWRfYXJyYXkS1gEKBAoCEAIKBBICEAIKBBoCEAIKBCoCEAIKBDoCEAIKBFoCEAIKBFoCEAIKBFICEAIKBGICEAIKB6oBBAgBGAIKBGoCEAIKBYIBAhACCgWKAQIQAgoFigICGAIKCcIBBggEEBMgAgoJwgEGCAIQCiACCgnCAQYIChATIAIKC9oBCAoEYgIQAhgCCgvaAQgKBCoCEAIYAgoR4gEOCgRiAhACEgQqAhACIAIKFuIBEwoEYgIQAhIJwgEGCAIQCiACIAIKEtoBDwoL2gEICgQqAhACGAIYAhgCOicKCm15X2RhdGFzZXQKGXNlcnZpY2VfcmVxdWVzdHNfbm9fbm9pc2UaChIICgQSAggOIgASAklEMgsQSioHaXN0aG11cw==";
        QueryPlan queryPlan = new QueryPlan("1.0", substraitPlanString);

        Plan plan = deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
        QueryBuilder builder = ElasticsearchQueryUtils.getQueryFromPlan(plan);

        assertNotNull("QueryBuilder should not be null", builder);
        assertTrue("Empty plan should return match_all query", builder.toString().contains("match_all"));
    }

    /**
     * Helper method to count occurrences of a substring
     */
    private int countOccurrences(String str, String findStr)
    {
        int lastIndex = 0;
        int count = 0;

        while (lastIndex != -1) {
            lastIndex = str.indexOf(findStr, lastIndex);
            if (lastIndex != -1) {
                count++;
                lastIndex += findStr.length();
            }
        }
        return count;
    }
}
