/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.BIT;
import static org.apache.arrow.vector.types.Types.MinorType.DATEDAY;
import static org.apache.arrow.vector.types.Types.MinorType.DATEMILLI;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT4;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT8;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.SMALLINT;
import static org.apache.arrow.vector.types.Types.MinorType.TINYINT;
import static org.apache.arrow.vector.types.Types.MinorType.VARBINARY;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcSplitQueryBuilderTest
{
    private static final String TEST_CATALOG = "catalog";
    private static final String TEST_SCHEMA = "schema";
    private static final String TEST_TABLE = "table";
    private static final String TEST_COL1 = "col1";
    private static final String TEST_COL_BIGINT = "col_bigint";
    private static final String TEST_COL_INT = "col_int";
    private static final String TEST_COL_SMALLINT = "col_smallint";
    private static final String TEST_COL_TINYINT = "col_tinyint";
    private static final String TEST_COL_FLOAT8 = "col_float8";
    private static final String TEST_COL_FLOAT4 = "col_float4";
    private static final String TEST_COL_BIT = "col_bit";
    private static final String TEST_COL_DATEDAY = "col_dateday";
    private static final String TEST_COL_DATEMILLI = "col_datemilli";
    private static final String TEST_COL_VARCHAR = "col_varchar";
    private static final String TEST_COL_VARBINARY = "col_varbinary";
    private static final String TEST_PARTITION_COL = "partition_col";
    private static final String TEST_PARTITION_VALUE = "2024";
    private static final String QUOTE_CHAR = "\"";
    private static final String TEST_UNSUPPORTED_COL = "unsupported";
    private static final String TEST_ORDER_BY_CLAUSE = "ORDER BY";
    private static final String TEST_LIMIT_CLAUSE = "LIMIT";
    private static final String TEST_SELECT_CLAUSE = "SELECT";
    private static final String TEST_FROM_CLAUSE = "FROM";
    private static final String TEST_COL2 = "col2";
    private static final String TEST_COL3 = "col3";
    private static final String TEST_HELLO_VALUE = "hello";
    private static final String TEST_ABC_VALUE = "abc";
    private static final String TEST_COLUMN_WITH_QUOTES = "column\"with\"quotes";
    private static final String TEST_QUOTED_COLUMN_WITH_QUOTES = "\"column\"\"with\"\"quotes\"";
    private static final String TEST_CANNOT_HANDLE_TYPE_MESSAGE = "Can't handle type";
    private static final String TEST_LOW_MARKER_BELOW_BOUND_MESSAGE = "Low marker should never use BELOW bound";
    private static final String TEST_HIGH_MARKER_ABOVE_BOUND_MESSAGE = "High marker should never use ABOVE bound";
    private static final String EXPECTED_STATEMENT_NOT_NULL = "Statement should not be null";
    private BlockAllocatorImpl allocator;
    private JdbcSplitQueryBuilder builder;
    private Connection mockConnection;
    private PreparedStatement mockStatement;
    private Constraints constraints;
    private Split split;
    private Schema schema;
    private FederationExpressionParser expressionParser;

    @Before
    public void setup() throws SQLException
    {
        allocator = new BlockAllocatorImpl();
        expressionParser = mock(FederationExpressionParser.class);
        when(expressionParser.parseComplexExpressions(any(), any(), any())).thenReturn(Collections.emptyList());

        builder = new JdbcSplitQueryBuilder(QUOTE_CHAR, expressionParser)
        {
            @Override
            protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
            {
                return " FROM \"" + schema + "\".\"" + table + "\"";
            }

            @Override
            protected List<String> getPartitionWhereClauses(Split split)
            {
                return Collections.singletonList("\"" + TEST_PARTITION_COL + "\" = '" + TEST_PARTITION_VALUE + "'");
            }
        };

        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());

        constraints = createEmptyConstraints();

        Field field = new Field(TEST_COL1, FieldType.nullable(new ArrowType.Int(32, true)), null);
        schema = new Schema(Collections.singletonList(field));
    }

    @After
    public void after() {
        allocator.close();
    }

    @Test
    public void testBuildSql_IntType() throws SQLException
    {
        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraints, split);
        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains("SELECT \"" + TEST_COL1 + "\""));
    }

    @Test
    public void testExtractOrderByClause_AscNullsFirst()
    {
        OrderByField orderByField = new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_FIRST);
        Constraints constraintsWithOrderBy = createConstraintsWithOrderBy(Collections.singletonList(orderByField));

        String clause = builder.extractOrderByClause(constraintsWithOrderBy);
        assertEquals(TEST_ORDER_BY_CLAUSE + " \"" + TEST_COL1 + "\" ASC NULLS FIRST", clause);
    }

    @Test
    public void testToPredicateWithSingleValueSet() throws SQLException
    {
        ArrowType type = new ArrowType.Int(32, true);
        SortedRangeSet valueSet = SortedRangeSet.of(false, Range.all(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType()));

        Constraints constraintsWithSummary = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        Field field = new Field(TEST_COL1, FieldType.nullable(type), null);
        Schema schema = new Schema(List.of(field));

        builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithSummary, split);
    }

    @Test
    public void testUnsupportedTypeThrowsException()
    {
        // Unsupported ArrowType
        ArrowType unsupportedType = new ArrowType.Duration(TimeUnit.MILLISECOND);

        // Define schema with one unsupported field
        Field unsupportedField = new Field(TEST_UNSUPPORTED_COL, FieldType.nullable(unsupportedType), null);
        Schema schema = new Schema(List.of(unsupportedField));

        // Mock ValueSet with unsupported type
        Range range = Range.equal(allocator, BIGINT.getType(), 123L);
        ValueSet valueSet = SortedRangeSet.of(range);

        // Make constraints map return this ValueSet for the unsupported column
        Constraints constraintsWithUnsupported = createConstraintsWithSummary(Map.of(TEST_UNSUPPORTED_COL, valueSet));

        // Execute and verify that the unsupported type triggers exception
        AthenaConnectorException ex = assertThrows(
                AthenaConnectorException.class,
                () -> builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithUnsupported, split)
        );

        assertTrue(ex.getMessage().contains(TEST_CANNOT_HANDLE_TYPE_MESSAGE));
    }

    @Test
    public void testSupportedType() throws SQLException
    {
        List<Field> fields = new ArrayList<>();
        Map<String, ValueSet> valueSetMap = new HashMap<>();

        fields.add(new Field(TEST_COL_BIGINT, FieldType.nullable(BIGINT.getType()), null));
        valueSetMap.put(TEST_COL_BIGINT, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 123L)));

        fields.add(new Field(TEST_COL_INT, FieldType.nullable(INT.getType()), null));
        valueSetMap.put(TEST_COL_INT, SortedRangeSet.of(Range.equal(allocator, INT.getType(), 456)));

        fields.add(new Field(TEST_COL_SMALLINT, FieldType.nullable(SMALLINT.getType()), null));
        valueSetMap.put(TEST_COL_SMALLINT, SortedRangeSet.of(Range.equal(allocator, SMALLINT.getType(), (short) 7)));

        fields.add(new Field(TEST_COL_TINYINT, FieldType.nullable(TINYINT.getType()), null));
        valueSetMap.put(TEST_COL_TINYINT, SortedRangeSet.of(Range.equal(allocator, TINYINT.getType(), (byte) 2)));

        fields.add(new Field(TEST_COL_FLOAT8, FieldType.nullable(FLOAT8.getType()), null));
        valueSetMap.put(TEST_COL_FLOAT8, SortedRangeSet.of(Range.equal(allocator, FLOAT8.getType(), 3.14159d)));

        fields.add(new Field(TEST_COL_FLOAT4, FieldType.nullable(FLOAT4.getType()), null));
        valueSetMap.put(TEST_COL_FLOAT4, SortedRangeSet.of(Range.equal(allocator, FLOAT4.getType(), 2.71f)));

        fields.add(new Field(TEST_COL_BIT, FieldType.nullable(BIT.getType()), null));
        valueSetMap.put(TEST_COL_BIT, SortedRangeSet.of(Range.equal(allocator, BIT.getType(), true)));

        fields.add(new Field(TEST_COL_DATEDAY, FieldType.nullable(DATEDAY.getType()), null));
        valueSetMap.put(TEST_COL_DATEDAY, SortedRangeSet.of(Range.equal(allocator, DATEDAY.getType(), 19000))); // ~2022-01-01

        fields.add(new Field(TEST_COL_DATEMILLI, FieldType.nullable(DATEMILLI.getType()), null));
        valueSetMap.put(TEST_COL_DATEMILLI, SortedRangeSet.of(Range.equal(allocator, DATEMILLI.getType(), LocalDateTime.of(2023, 1, 1, 10, 0))));

        fields.add(new Field(TEST_COL_VARCHAR, FieldType.nullable(VARCHAR.getType()), null));
        valueSetMap.put(TEST_COL_VARCHAR, SortedRangeSet.of(Range.equal(allocator, VARCHAR.getType(), TEST_HELLO_VALUE)));

        fields.add(new Field(TEST_COL_VARBINARY, FieldType.nullable(VARBINARY.getType()), null));
        valueSetMap.put(TEST_COL_VARBINARY, SortedRangeSet.of(Range.equal(allocator, VARBINARY.getType(), TEST_ABC_VALUE.getBytes(StandardCharsets.UTF_8))));

        Schema schema = new Schema(fields);
        Constraints constraintsWithValueSets = createConstraintsWithSummary(valueSetMap);

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithValueSets, split);
        assertEquals("Generated statement should not be null", mockStatement, stmt);
        // Verify SQL contains all columns
        for (Field field : fields) {
            verify(mockConnection).prepareStatement(contains("\"" + field.getName() + "\""));
        }

        verify(mockConnection).prepareStatement(contains(TEST_FROM_CLAUSE + " \"" + TEST_SCHEMA + "\".\"" + TEST_TABLE + "\""));
    }

    @Test
    public void testInClauseWithMultipleValues() throws SQLException
    {
        List<Range> ranges = new ArrayList<>();
        ranges.add(Range.equal(allocator, INT.getType(), 10));
        ranges.add(Range.equal(allocator, INT.getType(), 20));
        ranges.add(Range.equal(allocator, INT.getType(), 30));
        
        SortedRangeSet valueSet = SortedRangeSet.copyOf(INT.getType(), ranges, false);
        Constraints constraintsWithMultipleValues = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithMultipleValues, split);
        
        // Verify IN clause is generated with multiple placeholders
        verify(mockConnection).prepareStatement(contains("\"" + TEST_COL1 + "\" IN (?,?,?)"));
    }

    @Test
    public void testRangeBoundAboveForLowMarker() throws SQLException
    {
        Range range = Range.greaterThan(allocator, INT.getType(), 100);
        SortedRangeSet valueSet = SortedRangeSet.of(range);
        Constraints constraintsWithGreaterThan = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithGreaterThan, split);
        
        // Verify greater than clause is generated
        verify(mockConnection).prepareStatement(contains("\"" + TEST_COL1 + "\" > ?"));
    }

    @Test
    public void testRangeBoundExactlyForLowMarker() throws SQLException
    {
        Range range = Range.greaterThanOrEqual(allocator, INT.getType(), 100);
        SortedRangeSet valueSet = SortedRangeSet.of(range);
        Constraints constraintsWithGreaterThanOrEqual = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithGreaterThanOrEqual, split);
        
        // Verify greater than or equal clause is generated
        verify(mockConnection).prepareStatement(contains("\"" + TEST_COL1 + "\" >= ?"));
    }

    @Test
    public void testRangeBoundExactlyForHighMarker() throws SQLException
    {
        Range range = Range.lessThanOrEqual(allocator, INT.getType(), 200);
        SortedRangeSet valueSet = SortedRangeSet.of(range);
        Constraints constraintsWithLessThanOrEqual = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithLessThanOrEqual, split);
        
        // Verify less than or equal clause is generated
        verify(mockConnection).prepareStatement(contains("\"" + TEST_COL1 + "\" <= ?"));
    }

    @Test
    public void testRangeBoundBelowForHighMarker() throws SQLException
    {
        Range range = Range.lessThan(allocator, INT.getType(), 200);
        SortedRangeSet valueSet = SortedRangeSet.of(range);
        Constraints constraintsWithLessThan = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithLessThan, split);
        
        // Verify less than clause is generated
        verify(mockConnection).prepareStatement(contains("\"" + TEST_COL1 + "\" < ?"));
    }

    @Test
    public void testInvalidBoundBelowForLowMarker()
    {
        Range mockRange = createRangeWithBounds(Marker.Bound.BELOW, Marker.Bound.EXACTLY);
        SortedRangeSet valueSet = createValueSetWithRange(mockRange);
        Constraints constraintsWithInvalidBound = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        AthenaConnectorException ex = assertThrows(
                AthenaConnectorException.class,
                () -> builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithInvalidBound, split)
        );
        
        assertTrue(ex.getMessage().contains(TEST_LOW_MARKER_BELOW_BOUND_MESSAGE));
    }

    @Test
    public void testInvalidBoundAboveForHighMarker()
    {
        Range mockRange = createRangeWithBounds(Marker.Bound.EXACTLY, Marker.Bound.ABOVE);
        SortedRangeSet valueSet = createValueSetWithRange(mockRange);
        Constraints constraintsWithInvalidHighBound = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        AthenaConnectorException ex = assertThrows(
                AthenaConnectorException.class,
                () -> builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithInvalidHighBound, split)
        );
        
        assertTrue(ex.getMessage().contains(TEST_HIGH_MARKER_ABOVE_BOUND_MESSAGE));
    }

    @Test
    public void testQuoteMethodWithSpecialCharacters()
    {
        String result = builder.quote(TEST_COLUMN_WITH_QUOTES);
        assertEquals(TEST_QUOTED_COLUMN_WITH_QUOTES, result);
    }

    @Test
    public void testAppendLimitOffsetWithSplit()
    {
        String result = builder.appendLimitOffset(split);
        assertEquals("", result); // Should return empty string by default
    }

    @Test
    public void testAppendLimitOffsetWithConstraints()
    {
        Constraints constraintsWithLimit = createConstraintsWithLimit();
        String result = builder.appendLimitOffset(split, constraintsWithLimit);
        assertEquals(" LIMIT 100", result);
    }

    @Test
    public void testExtractOrderByClauseMultipleFields()
    {
        OrderByField field1 = new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_LAST);
        OrderByField field2 = new OrderByField(TEST_COL2, OrderByField.Direction.DESC_NULLS_FIRST);
        
        Constraints constraintsWithMultipleOrderBy = createConstraintsWithOrderBy(List.of(field1, field2));

        String clause = builder.extractOrderByClause(constraintsWithMultipleOrderBy);
        assertEquals(TEST_ORDER_BY_CLAUSE + " \"" + TEST_COL1 + "\" ASC NULLS LAST, \"" + TEST_COL2 + "\" DESC NULLS FIRST", clause);
    }

    @Test
    public void testExtractOrderByClauseEmpty()
    {
        Constraints constraintsWithEmptyOrderBy = createConstraintsWithOrderBy(Collections.emptyList());

        String clause = builder.extractOrderByClause(constraintsWithEmptyOrderBy);
        assertEquals("", clause);
    }

    @Test
    public void testBuildSqlWithEmptyConstraints() throws SQLException
    {
        Constraints emptyConstraints = createEmptyConstraints();
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, emptyConstraints, split);
        
        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains(TEST_SELECT_CLAUSE));
        verify(mockConnection).prepareStatement(contains(TEST_COL1));
    }

    @Test
    public void testBuildSqlWithOrderByAndLimit() throws SQLException
    {
        // Test ORDER BY with LIMIT
        OrderByField orderByField = new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_FIRST);
        Constraints constraintsWithOrderByAndLimit = createConstraintsWithOrderByAndLimit(Collections.singletonList(orderByField), 50L);
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithOrderByAndLimit, split);
        
        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains(TEST_ORDER_BY_CLAUSE));
        verify(mockConnection).prepareStatement(contains(TEST_LIMIT_CLAUSE));
    }

    @Test
    public void testBuildSqlWithComplexExpressionsAndOrderBy() throws SQLException
    {
        // Test complex expressions with ORDER BY
        List<String> complexExpressions = List.of("(col1 + col2)", "CASE WHEN col3 > 0 THEN col4 ELSE 0 END");
        when(expressionParser.parseComplexExpressions(any(), any(), any())).thenReturn(complexExpressions);
        
        OrderByField orderByField = new OrderByField(TEST_COL1, OrderByField.Direction.DESC_NULLS_LAST);
        Constraints constraintsWithComplexAndOrderBy = createConstraintsWithOrderBy(Collections.singletonList(orderByField));
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithComplexAndOrderBy, split);
        
        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains(TEST_ORDER_BY_CLAUSE));
        verify(expressionParser).parseComplexExpressions(any(), any(), any());
    }

    @Test
    public void testBuildSqlWithOrderByLimitAndComplexExpressions() throws SQLException
    {
        // Test the complete scenario: ORDER BY + LIMIT + complex expressions
        List<String> complexExpressions = List.of(
            "(col1 * col2 + col3)", 
            "CASE WHEN col4 > 100 THEN col5 ELSE col6 END",
            "COALESCE(col7, col8, 'default')"
        );
        when(expressionParser.parseComplexExpressions(any(), any(), any())).thenReturn(complexExpressions);
        
        OrderByField orderByField1 = new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_FIRST);
        OrderByField orderByField2 = new OrderByField(TEST_COL2, OrderByField.Direction.DESC_NULLS_LAST);
        Constraints constraintsWithAll = createConstraintsWithOrderByAndLimit(List.of(orderByField1, orderByField2), 25L);
        
        List<Field> fields = List.of(
            new Field(TEST_COL1, FieldType.nullable(INT.getType()), null),
            new Field(TEST_COL2, FieldType.nullable(INT.getType()), null),
            new Field(TEST_COL3, FieldType.nullable(INT.getType()), null)
        );
        Schema schema = new Schema(fields);

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithAll, split);
        
        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains(TEST_ORDER_BY_CLAUSE));
        verify(mockConnection).prepareStatement(contains(TEST_LIMIT_CLAUSE));
        verify(expressionParser).parseComplexExpressions(any(), any(), any());
    }

    @Test
    public void testBuildSqlWithComplexNestedExpressions() throws SQLException
    {
        // Test deeply nested complex expressions
        List<String> nestedExpressions = List.of(
            "((col1 + col2) * (col3 - col4))",
            "CASE WHEN (col5 > col6) AND (col7 < col8) THEN col9 ELSE col10 END",
            "NULLIF(COALESCE(col11, col12), col13)"
        );
        when(expressionParser.parseComplexExpressions(any(), any(), any())).thenReturn(nestedExpressions);
        
        OrderByField orderByField = new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_FIRST);
        Constraints constraintsWithNested = createConstraintsWithOrderByAndLimit(Collections.singletonList(orderByField), 10L);
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithNested, split);
        
        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains(TEST_ORDER_BY_CLAUSE));
        verify(mockConnection).prepareStatement(contains(TEST_LIMIT_CLAUSE));
        verify(expressionParser).parseComplexExpressions(any(), any(), any());
    }

    @Test
    public void testBuildSqlWithAggregateFunctionsInOrderBy() throws SQLException
    {
        // Test ORDER BY with aggregate functions and complex expressions
        List<String> aggregateExpressions = List.of(
            "SUM(col1 + col2)",
            "AVG(CASE WHEN col3 > 0 THEN col4 ELSE 0 END)",
            "COUNT(DISTINCT col5)"
        );
        when(expressionParser.parseComplexExpressions(any(), any(), any())).thenReturn(aggregateExpressions);
        
        OrderByField orderByField = new OrderByField("SUM(col1)", OrderByField.Direction.DESC_NULLS_LAST);
        Constraints constraintsWithAggregates = createConstraintsWithOrderByAndLimit(Collections.singletonList(orderByField), 100L);
        
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithAggregates, split);
        
        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains(TEST_ORDER_BY_CLAUSE));
        verify(mockConnection).prepareStatement(contains(TEST_LIMIT_CLAUSE));
        verify(expressionParser).parseComplexExpressions(any(), any(), any());
    }

    /**
     * Creates a Constraints instance with empty collections and default values.
     * Uses the pattern provided by the user.
     */
    private Constraints createEmptyConstraints() {
        Map<String, String> queryArgs = Collections.emptyMap();
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryArgs, null);
    }

    /**
     * Creates a Constraints instance with the specified summary map.
     */
    private Constraints createConstraintsWithSummary(Map<String, ValueSet> summary) {
        Map<String, String> queryArgs = Collections.emptyMap();
        return new Constraints(summary, Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryArgs, null);
    }

    /**
     * Creates a Constraints instance with the specified order by clause.
     */
    private Constraints createConstraintsWithOrderBy(List<OrderByField> orderByClause) {
        Map<String, String> queryArgs = Collections.emptyMap();
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), orderByClause,
                Constraints.DEFAULT_NO_LIMIT, queryArgs, null);
    }

    /**
     * Creates a Constraints instance with the specified limit.
     */
    private Constraints createConstraintsWithLimit() {
        Map<String, String> queryArgs = Collections.emptyMap();
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                100L, queryArgs, null);
    }

    /**
     * Creates a Constraints instance with both ORDER BY and LIMIT.
     */
    private Constraints createConstraintsWithOrderByAndLimit(List<OrderByField> orderByClause, Long limit) {
        Map<String, String> queryArgs = Collections.emptyMap();
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), orderByClause,
                limit, queryArgs, null);
    }

    private Range createRangeWithBounds(Marker.Bound lowerBound, Marker.Bound upperBound)
    {
        Range range = mock(Range.class, RETURNS_DEEP_STUBS);
        when(range.isSingleValue()).thenReturn(false);
        when(range.getLow().isLowerUnbounded()).thenReturn(false);
        when(range.getLow().getBound()).thenReturn(lowerBound);
        when(range.getLow().getValue()).thenReturn(100);
        when(range.getHigh().isUpperUnbounded()).thenReturn(false);
        when(range.getHigh().getBound()).thenReturn(upperBound);
        when(range.getHigh().getValue()).thenReturn(200);
        return range;
    }

    private SortedRangeSet createValueSetWithRange(Range range)
    {
        SortedRangeSet valueSet = mock(SortedRangeSet.class, RETURNS_DEEP_STUBS);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
}
