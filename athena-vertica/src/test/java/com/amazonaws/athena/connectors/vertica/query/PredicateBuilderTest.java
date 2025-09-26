/*-
 * #%L
 * athena-vertica
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
package com.amazonaws.athena.connectors.vertica.query;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PredicateBuilderTest {
    private Field col1Int;
    private Field col2Varchar;
    private Field col3Double;
    private Field col4Bool;
    private Schema schema;
    private HashMap<String, PredicateBuilder.TypeAndValue> accumulator;
    private BlockAllocator allocator;

    @Before
    public void setUp() {
        allocator = new BlockAllocatorImpl();
        col1Int = Field.nullable("col1", Types.MinorType.INT.getType());
        col2Varchar = Field.nullable("col2", Types.MinorType.VARCHAR.getType());
        col3Double = Field.nullable("col3", Types.MinorType.FLOAT8.getType());
        col4Bool = Field.nullable("col4", Types.MinorType.BIT.getType());
        schema = new Schema(ImmutableList.of(col1Int, col2Varchar, col3Double, col4Bool));
        accumulator = new HashMap<>();
    }

    @After
    public void tearDown() {
        allocator.close();
    }

    @Test
    public void quote_SimpleName_ReturnsQuotedString() {
        assertEquals("\"simpleName\"", PredicateBuilder.quote("simpleName"));
    }

    @Test
    public void getFromClauseWithSplit_SchemaAndTable_ReturnsQualifiedTableName() {
        assertEquals("\"my_schema\".\"my_table\"", PredicateBuilder.getFromClauseWithSplit("my_schema", "my_table"));
    }

    @Test
    public void toPredicate_EqualsOperator_ReturnsEqualityPredicate() {
        String expected = "\"col1\" = <col1> ";
        String actual = PredicateBuilder.toPredicate("col1", "=", 100, col1Int.getType(), accumulator);
        assertEquals(expected, actual);
        assertTrue(accumulator.containsKey("col1"));
        assertEquals(100, accumulator.get("col1").getValue());
        assertEquals(col1Int.getType(), accumulator.get("col1").getType());
    }

    @Test
    public void toPredicate_GreaterThanOperator_ReturnsGreaterThanPredicate() {
        String expected = "\"col3\" > <col3> ";
        String actual = PredicateBuilder.toPredicate("col3", ">", 99.5, col3Double.getType(), accumulator);
        assertEquals(expected, actual);
        assertTrue(accumulator.containsKey("col3"));
        assertEquals(99.5, accumulator.get("col3").getValue());
        assertEquals(col3Double.getType(), accumulator.get("col3").getType());
    }

    @Test
    public void toPredicate_LessThanOperator_ReturnsLessThanPredicate() {
        String expected = "\"col2\" \\< <col2> ";
        String actual = PredicateBuilder.toPredicate("col2", "<", "abc", col2Varchar.getType(), accumulator);
        assertEquals(expected, actual);
        assertTrue(accumulator.containsKey("col2"));
        assertEquals("abc", accumulator.get("col2").getValue());
        assertEquals(col2Varchar.getType(), accumulator.get("col2").getType());
    }

    @Test
    public void toPredicate_LessThanOrEqualOperator_ReturnsLessThanOrEqualPredicate() {
        String expected = "\"col1\" \\<= <col1> ";
        String actual = PredicateBuilder.toPredicate("col1", "<=", 5, col1Int.getType(), accumulator);
        assertEquals(expected, actual);
        assertTrue(accumulator.containsKey("col1"));
        assertEquals(5, accumulator.get("col1").getValue());
        assertEquals(col1Int.getType(), accumulator.get("col1").getType());
    }


    @Test
    public void toConjuncts_EmptyConstraints_ReturnsEmptyList() {
        Constraints constraints = createConstraints(Collections.emptyMap());
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertTrue(conjuncts.isEmpty());
        assertTrue(accumulator.isEmpty());
    }

    @Test
    public void toConjuncts_NonExistentColumn_ReturnsEmptyList() {
        Constraints constraints = createConstraints(ImmutableMap.of("non_existent_col", singleValue(col1Int.getType(), 10)));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertTrue(conjuncts.isEmpty());
        assertTrue(accumulator.isEmpty());
    }

    @Test
    public void toConjuncts_SingleEqualValue_ReturnsEqualityPredicate() {
        Constraints constraints = createConstraints(ImmutableMap.of("col1", singleValue(col1Int.getType(), 123)));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(1, conjuncts.size());
        assertEquals("(\"col1\" = <col1> )", conjuncts.get(0));
        assertEquals(1, accumulator.size());
        assertEquals(123, accumulator.get("col1").getValue());
    }

    @Test
    public void toConjuncts_MultipleValues_ReturnsInPredicate() {
        Constraints constraints = createConstraints(ImmutableMap.of("col1", inValues(col1Int.getType(), false, 10, 20, 30)));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(1, conjuncts.size());
        assertEquals("(\"col1\" IN (<col1>,<col1>,<col1>))", conjuncts.get(0));
        assertEquals(1, accumulator.size());
        assertEquals(30, accumulator.get("col1").getValue());
        assertEquals(col1Int.getType(), accumulator.get("col1").getType());
    }

    @Test
    public void toConjuncts_MultipleValuesWithNull_ReturnsInOrNullPredicate() {
        Constraints constraints = createConstraints(ImmutableMap.of("col1", inValues(col1Int.getType(), true, 10, 20)));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(1, conjuncts.size());
        assertEquals("((col1 IS NULL) OR \"col1\" IN (<col1>,<col1>))", conjuncts.get(0));
        assertEquals(1, accumulator.size());
        assertEquals(20, accumulator.get("col1").getValue());
    }

    @Test
    public void toConjuncts_GreaterThanValue_ReturnsGreaterThanPredicate() {
        Constraints constraints = createConstraints(ImmutableMap.of("col3", greaterThan(col3Double.getType(), 10.5)));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(1, conjuncts.size());
        assertEquals("((\"col3\" > <col3> ))", conjuncts.get(0));
        assertEquals(1, accumulator.size());
        assertEquals(10.5, accumulator.get("col3").getValue());
    }

    @Test
    public void toConjuncts_InclusiveRange_ReturnsRangePredicate() {
        Constraints constraints = createConstraints(ImmutableMap.of("col1", betweenInclusive(col1Int.getType())));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(1, conjuncts.size());
        assertEquals("((\"col1\" >= <col1>  AND \"col1\" \\<= <col1> ))", conjuncts.get(0));
        assertEquals(1, accumulator.size());
        assertEquals(20, accumulator.get("col1").getValue());
    }

    @Test
    public void toConjuncts_NullOnlyValue_ReturnsIsNullPredicate() {
        Constraints constraints = createConstraints(ImmutableMap.of("col2", isNullOnly(col2Varchar.getType())));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(1, conjuncts.size());
        assertEquals("(col2 IS NULL)", conjuncts.get(0));
        assertTrue(accumulator.isEmpty());
    }

    @Test
    public void toConjuncts_NotNullValue_ReturnsIsNotNullPredicate() {
        Constraints constraints = createConstraints(ImmutableMap.of("col3", isNotNullOnly(col3Double.getType())));
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(1, conjuncts.size());
        assertEquals("(col3 IS NOT NULL)", conjuncts.get(0));
        assertTrue(accumulator.isEmpty());
    }

    @Test
    public void toConjuncts_MultipleColumns_ReturnsMultiplePredicates() {
        Map<String, ValueSet> summary = ImmutableMap.of(
                "col1", greaterThan(col1Int.getType(), 10),
                "col2", singleValue(col2Varchar.getType(), "ACTIVE")
        );
        Constraints constraints = createConstraints(summary);
        List<String> conjuncts = PredicateBuilder.toConjuncts(schema.getFields(), constraints, accumulator);
        assertEquals(2, conjuncts.size());
        assertTrue(conjuncts.contains("((\"col1\" > <col1> ))"));
        assertTrue(conjuncts.contains("(\"col2\" = <col2> )"));
        assertEquals(2, accumulator.size());
        assertEquals(10, accumulator.get("col1").getValue());
        assertEquals("ACTIVE", accumulator.get("col2").getValue().toString());
    }

    @Test
    public void TypeAndValue_ValidInput_ReturnsCorrectValues() {
        PredicateBuilder.TypeAndValue tv = new PredicateBuilder.TypeAndValue(col1Int.getType(), 99);
        assertEquals(col1Int.getType(), tv.getType());
        assertEquals(99, tv.getValue());
        assertTrue(tv.toString().contains("type=" + col1Int.getType().toString()));
        assertTrue(tv.toString().contains("value=99"));
    }


    private Constraints createConstraints(Map<String, ValueSet> summary) {
        return new Constraints(summary, Collections.emptyList(), Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(),null);
    }

    private ValueSet singleValue(ArrowType type, Object value) {
        Range range = Range.equal(allocator, type, value);
        return SortedRangeSet.newBuilder(type, false)
                .add(range)
                .build();
    }

    private ValueSet greaterThan(ArrowType type, Object value) {
        Range range = Range.greaterThan(allocator, type, value);
        return SortedRangeSet.newBuilder(type, false)
                .add(range)
                .build();
    }

    private ValueSet betweenInclusive(ArrowType type) {
        Range range = Range.range(allocator, type, 10, true, 20, true);
        return SortedRangeSet.newBuilder(type, false)
                .add(range)
                .build();
    }

    private ValueSet inValues(ArrowType type, boolean nullAllowed, Object... values) {
        SortedRangeSet.Builder builder = SortedRangeSet.newBuilder(type, nullAllowed);
        for (Object value : values) {
            builder.add(Range.equal(allocator, type, value));
        }
        return builder.build();
    }

    private ValueSet isNullOnly(ArrowType type) {
        return SortedRangeSet.onlyNull(type);
    }

    private ValueSet isNotNullOnly(ArrowType type) {
        return SortedRangeSet.notNull(allocator, type);
    }
}
