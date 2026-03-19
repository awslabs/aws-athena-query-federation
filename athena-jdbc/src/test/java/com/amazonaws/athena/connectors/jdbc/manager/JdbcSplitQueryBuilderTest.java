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
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connector.substrait.SubstraitTypeAndValue;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

    @BeforeEach
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

    @AfterEach
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

    @Test
    public void testBuildSqlWithDecimalType() throws SQLException
    {
        // Test DECIMAL type handling
        ArrowType decimalType = new ArrowType.Decimal(10, 2, 128);
        Field decimalField = new Field("decimal_col", FieldType.nullable(decimalType), null);
        Schema schema = new Schema(List.of(decimalField));

        java.math.BigDecimal decimalValue = new java.math.BigDecimal("123.45");
        SortedRangeSet valueSet = SortedRangeSet.of(Range.equal(allocator, decimalType, decimalValue));
        Constraints constraintsWithDecimal = createConstraintsWithSummary(Map.of("decimal_col", valueSet));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithDecimal, split);

        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains("\"decimal_col\""));
    }

    @Test
    public void testBuildSqlWithNullOnlyValueSet() throws SQLException
    {
        // Test ValueSet that is none but allows nulls (IS NULL case)
        SortedRangeSet nullOnlyValueSet = mock(SortedRangeSet.class, RETURNS_DEEP_STUBS);
        when(nullOnlyValueSet.isNone()).thenReturn(true);
        when(nullOnlyValueSet.isNullAllowed()).thenReturn(true);
        when(nullOnlyValueSet.getRanges().getOrderedRanges()).thenReturn(Collections.emptyList());

        Constraints constraintsWithNullOnly = createConstraintsWithSummary(Map.of(TEST_COL1, nullOnlyValueSet));
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithNullOnly, split);

        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains("IS NULL"));
    }

    @Test
    public void testBuildSqlWithNotNullValueSet() throws SQLException
    {
        // Test ValueSet that represents NOT NULL (all values except null)
        SortedRangeSet notNullValueSet = SortedRangeSet.of(false, Range.all(allocator, INT.getType()));

        Constraints constraintsWithNotNull = createConstraintsWithSummary(Map.of(TEST_COL1, notNullValueSet));
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithNotNull, split);

        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains("IS NOT NULL"));
    }

    @Test
    public void testBuildSqlWithNullAllowedAndRanges() throws SQLException
    {
        // Test ValueSet that allows nulls AND has ranges (IS NULL OR range conditions)
        Range range = Range.equal(allocator, INT.getType(), 100);
        SortedRangeSet nullAndRangeValueSet = mock(SortedRangeSet.class, RETURNS_DEEP_STUBS);
        when(nullAndRangeValueSet.isNone()).thenReturn(false);
        when(nullAndRangeValueSet.isNullAllowed()).thenReturn(true);
        when(nullAndRangeValueSet.getRanges().getOrderedRanges()).thenReturn(List.of(range));

        Constraints constraintsWithNullAndRange = createConstraintsWithSummary(Map.of(TEST_COL1, nullAndRangeValueSet));
        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithNullAndRange, split);

        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains("IS NULL"));
    }

    @Test
    public void testBuildSqlWithUnhandledBoundException()
    {
        // Test unhandled bound exception for low marker - use NullPointerException since that's what actually gets thrown
        Range mockRange = mock(Range.class, RETURNS_DEEP_STUBS);
        when(mockRange.isSingleValue()).thenReturn(false);
        when(mockRange.getLow().isLowerUnbounded()).thenReturn(false);
        when(mockRange.getLow().getBound()).thenReturn(null); // This will trigger NPE
        when(mockRange.getLow().getValue()).thenReturn(100);
        when(mockRange.getHigh().isUpperUnbounded()).thenReturn(true);

        SortedRangeSet valueSet = createValueSetWithRange(mockRange);
        Constraints constraintsWithUnhandledBound = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));

        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        NullPointerException ex = assertThrows(
                NullPointerException.class,
                () -> builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithUnhandledBound, split)
        );

        assertNotNull(ex);
    }

    @Test
    public void testBuildSqlWithUnhandledHighBoundException()
    {
        // Test unhandled bound exception for high marker - use NullPointerException since that's what actually gets thrown
        Range mockRange = mock(Range.class, RETURNS_DEEP_STUBS);
        when(mockRange.isSingleValue()).thenReturn(false);
        when(mockRange.getLow().isLowerUnbounded()).thenReturn(true);
        when(mockRange.getHigh().isUpperUnbounded()).thenReturn(false);
        when(mockRange.getHigh().getBound()).thenReturn(null); // This will trigger NPE
        when(mockRange.getHigh().getValue()).thenReturn(200);

        SortedRangeSet valueSet = createValueSetWithRange(mockRange);
        Constraints constraintsWithUnhandledHighBound = createConstraintsWithSummary(Map.of(TEST_COL1, valueSet));

        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        NullPointerException ex = assertThrows(
                NullPointerException.class,
                () -> builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraintsWithUnhandledHighBound, split)
        );

        assertNotNull(ex);
    }

    @Test
    public void testBuildSqlWithEmptyColumnNames() throws SQLException
    {
        // Test case where all columns are filtered out (empty column names)
        Map<String, String> splitProperties = Map.of(TEST_COL1, "filtered_value");
        when(split.getProperties()).thenReturn(splitProperties);

        Field field = new Field(TEST_COL1, FieldType.nullable(INT.getType()), null);
        Schema schema = new Schema(List.of(field));

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraints, split);

        assertEquals(EXPECTED_STATEMENT_NOT_NULL, mockStatement, stmt);
        verify(mockConnection).prepareStatement(contains("null")); // Should select null when no columns
    }

    @Test
    public void testGetSqlDialect()
    {
        // Test getSqlDialect method returns AnsiSqlDialect
        SqlDialect dialect = builder.getSqlDialect();
        assertEquals(AnsiSqlDialect.DEFAULT, dialect);
    }

    @Test
    public void testConstructorWithoutExpressionParser()
    {
        // Test constructor that doesn't take expression parser
        JdbcSplitQueryBuilder builderWithoutParser = new JdbcSplitQueryBuilder(QUOTE_CHAR)
        {
            @Override
            protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
            {
                return " FROM \"" + schema + "\".\"" + table + "\"";
            }

            @Override
            protected List<String> getPartitionWhereClauses(Split split)
            {
                return Collections.emptyList();
            }
        };

        // Verify it was created successfully
        assertEquals("\"test\"", builderWithoutParser.quote("test"));
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

    @Test
    public void testHandleTimestampWithTimestampString() throws Exception
    {
        // Test TIMESTAMP type handling with TimestampString value
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        TimestampString timestampString = new TimestampString("2025-01-10 10:10:10");

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.TIMESTAMP,
            timestampString,
            "EVENT_TIMESTAMP"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setTimestamp was called with the correct timestamp
        verify(mockPreparedStatement).setTimestamp(eq(1), any(java.sql.Timestamp.class));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleTimestampWithNumericValue() throws Exception
    {
        // Test TIMESTAMP type handling with numeric millisecond value
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        long expectedMillis = 1704880210000L; // 2025-01-10 10:10:10 in milliseconds

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.TIMESTAMP,
            expectedMillis,
            "EVENT_TIMESTAMP"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setTimestamp was called with correct timestamp
        verify(mockPreparedStatement).setTimestamp(eq(1), eq(new java.sql.Timestamp(expectedMillis)));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleTimestampWithUnsupportedFormat() throws Exception
    {
        // Test TIMESTAMP type handling with unsupported value format
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);

        // Use an unsupported type like String directly (not TimestampString)
        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.TIMESTAMP,
            "2025-01-10",  // Plain string instead of TimestampString or Number
            "EVENT_TIMESTAMP"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method and expect AthenaConnectorException
        java.lang.reflect.InvocationTargetException ex = assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> method.invoke(builder, mockPreparedStatement, accumulator)
        );

        // Verify the cause is AthenaConnectorException
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof AthenaConnectorException);
        assertTrue(cause.getMessage().contains("Can't handle timestamp format"));
        assertTrue(cause.getMessage().contains("String"));
    }

    @Test
    public void testHandleMultipleTimestamps() throws Exception
    {
        // Test handling multiple TIMESTAMP values in a single prepared statement
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);

        TimestampString ts1 = new TimestampString("2025-01-10 10:10:10");
        long ts2Millis = 1704880210000L;

        SubstraitTypeAndValue typeAndValue1 = new SubstraitTypeAndValue(
            SqlTypeName.TIMESTAMP,
            ts1,
            "EVENT_TIMESTAMP1"
        );

        SubstraitTypeAndValue typeAndValue2 = new SubstraitTypeAndValue(
            SqlTypeName.TIMESTAMP,
            ts2Millis,
            "EVENT_TIMESTAMP2"
        );

        List<SubstraitTypeAndValue> accumulator = List.of(typeAndValue1, typeAndValue2);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setTimestamp was called twice with correct indexes
        verify(mockPreparedStatement).setTimestamp(eq(1), any(java.sql.Timestamp.class));
        verify(mockPreparedStatement).setTimestamp(eq(2), eq(new java.sql.Timestamp(ts2Millis)));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleTimestampWithMixedTypes() throws Exception
    {
        // Test TIMESTAMP handling mixed with other types (VARCHAR, INTEGER, etc.)
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);

        SubstraitTypeAndValue varchar = new SubstraitTypeAndValue(
            SqlTypeName.VARCHAR,
            "test_value",
            "COL_VARCHAR"
        );

        SubstraitTypeAndValue timestamp = new SubstraitTypeAndValue(
            SqlTypeName.TIMESTAMP,
            new TimestampString("2025-01-10 10:10:10"),
            "EVENT_TIMESTAMP"
        );

        SubstraitTypeAndValue integer = new SubstraitTypeAndValue(
            SqlTypeName.INTEGER,
            12345,
            "COL_INT"
        );

        List<SubstraitTypeAndValue> accumulator = List.of(varchar, timestamp, integer);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify all types were set correctly
        verify(mockPreparedStatement).setString(eq(1), eq("test_value"));
        verify(mockPreparedStatement).setTimestamp(eq(2), any(java.sql.Timestamp.class));
        verify(mockPreparedStatement).setInt(eq(3), eq(12345));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithDateStringValue() throws Exception
    {
        // Test DATE type handling with DateString value
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        DateString dateString = new DateString("2025-01-10");

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            dateString,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setDate was called with correct date
        verify(mockPreparedStatement).setDate(eq(1), any(java.sql.Date.class));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithSqlDateValue() throws Exception
    {
        // Test DATE type handling with java.sql.Date value
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        java.sql.Date sqlDate = java.sql.Date.valueOf("2025-01-10");

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            sqlDate,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setDate was called with the same date object
        verify(mockPreparedStatement).setDate(eq(1), eq(sqlDate));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithUtilDateValue() throws Exception
    {
        // Test DATE type handling with java.util.Date value
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        java.util.Date utilDate = new java.util.Date(1704844800000L); // 2025-01-10

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            utilDate,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);
            
        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setDate was called with converted sql.Date
        verify(mockPreparedStatement).setDate(eq(1), any(java.sql.Date.class));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithStringValue() throws Exception
    {
        // Test DATE type handling with string value (fallback parsing)
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        String dateString = "2025-01-10";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            dateString,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setDate was called with parsed date
        verify(mockPreparedStatement).setDate(eq(1), eq(java.sql.Date.valueOf(dateString)));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithNumericDayValue() throws Exception
    {
        // Test DATE type handling with numeric value (days since epoch)
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        long daysValue = 19000; // Days since epoch (approximately 2022-01-01)

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            daysValue,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setDate was called
        verify(mockPreparedStatement).setDate(eq(1), any(java.sql.Date.class));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithNumericMillisecondValue() throws Exception
    {
        // Test DATE type handling with numeric millisecond value
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        long millisValue = 1704844800000L; // Milliseconds since epoch

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            millisValue,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setDate was called
        verify(mockPreparedStatement).setDate(eq(1), any(java.sql.Date.class));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithTimestampStringValue() throws Exception
    {
        // Test DATE type handling with TimestampString value (should convert to timestamp)
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        TimestampString timestampString = new TimestampString("2025-01-10 10:10:10");

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            timestampString,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify setTimestamp was called (DATE with TimestampString converts to timestamp)
        verify(mockPreparedStatement).setTimestamp(eq(1), any(java.sql.Timestamp.class));
        assertEquals(mockPreparedStatement, result);
    }

    @Test
    public void testHandleDateWithInvalidStringValue() throws Exception
    {
        // Test DATE type handling with invalid string format
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        String invalidDateString = "invalid-date-format";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            invalidDateString,
            "EVENT_DATE"
        );

        List<SubstraitTypeAndValue> accumulator = Collections.singletonList(typeAndValue);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method and expect AthenaConnectorException
        java.lang.reflect.InvocationTargetException ex = assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> method.invoke(builder, mockPreparedStatement, accumulator)
        );

        // Verify the cause is AthenaConnectorException with appropriate error message
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof AthenaConnectorException);
        assertTrue(cause.getMessage().contains("Can't handle date format"));
        assertTrue(cause.getMessage().contains("value type"));
        assertTrue(cause.getMessage().contains(invalidDateString));
    }

    @Test
    public void testHandleDateWithMixedTypes() throws Exception
    {
        // Test DATE handling mixed with other types
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);

        SubstraitTypeAndValue varchar = new SubstraitTypeAndValue(
            SqlTypeName.VARCHAR,
            "test_value",
            "COL_VARCHAR"
        );

        SubstraitTypeAndValue date = new SubstraitTypeAndValue(
            SqlTypeName.DATE,
            new DateString("2025-01-10"),
            "EVENT_DATE"
        );

        SubstraitTypeAndValue integer = new SubstraitTypeAndValue(
            SqlTypeName.INTEGER,
            12345,
            "COL_INT"
        );

        List<SubstraitTypeAndValue> accumulator = List.of(varchar, date, integer);

        // Use reflection to access the private method
        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
            "handleDataTypesForPreparedStatement",
            PreparedStatement.class,
            List.class
        );
        method.setAccessible(true);

        // Execute the method
        PreparedStatement result = (PreparedStatement) method.invoke(builder, mockPreparedStatement, accumulator);

        // Verify all types were set correctly
        verify(mockPreparedStatement).setString(eq(1), eq("test_value"));
        verify(mockPreparedStatement).setDate(eq(2), any(java.sql.Date.class));
        verify(mockPreparedStatement).setInt(eq(3), eq(12345));
        assertEquals(mockPreparedStatement, result);
    }

    @ParameterizedTest
    @MethodSource("provideSqlTestCases")
    public void testPrepareStatementWithCalciteSql_Success(final String base64EncodedPlan) throws Exception {
        QueryPlan queryPlan = mock(QueryPlan.class);
        Constraints constraintsWithQueryPlan = mock(Constraints.class);

        when(queryPlan.getSubstraitPlan()).thenReturn(base64EncodedPlan);
        when(constraintsWithQueryPlan.getQueryPlan()).thenReturn(queryPlan);

        PreparedStatement result = builder.prepareStatementWithCalciteSql(mockConnection, constraintsWithQueryPlan, AnsiSqlDialect.DEFAULT, split);
        assertNotNull(result);
    }

    private static Stream<Arguments> provideSqlTestCases() {
        return Stream.of(
                // SELECT employee_id, employee_name, job_title, salary FROM "ADMIN"."ORACLE_BASIC_DBTABLE_GAMMA_EU_WEST_1_INTEG";
                Arguments.of("GrgEErUECoMEOoAECggSBgoEFBUWFxLFAwrCAwoCCgAS2gIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlCg5wYXJ0aXRpb25fbmFtZRKYAQoHugEECAEYAQoEOgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgnCAQYIBBATIAEKCcIBBggEEBMgAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAMQEyABCgQ6AhABCgnCAQYIAxATIAEKBDoCEAEKCcIBBggDEBMgAQoEOgIQAQoEYgIQARgCOl8KMU9SQUNMRV9CQVNJQ19EQlRBQkxFX1NDSEVNQV9HQU1NQV9FVV9XRVNUXzFfSU5URUcKKk9SQUNMRV9CQVNJQ19EQlRBQkxFX0dBTU1BX0VVX1dFU1RfMV9JTlRFRxoIEgYKAhIAIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggIIgASC2VtcGxveWVlX2lkEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSBnNhbGFyeTILEEoqB2lzdGhtdXM="),
                
                // SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
                Arguments.of("Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRrsAxLpAwrZAzrWAwoFEgMKARYSwAMSvQMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaJhokCAEaBAoCEAEiDBoKEggKBBICCAMiACIMGgoKCGIGRU1QMDAxGgoSCAoEEgIIAyIAEgtFTVBMT1lFRV9JRDILEEoqB2lzdGhtdXM="),
                
                // SELECT employee_id FROM basic_write_nonexist WHERE hash1 > 1000
                Arguments.of("Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAEQARoKZ3Q6YW55X2FueRrnAxLkAwrUAzrRAwoFEgMKARYSuwMSuAMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaIRofCAEaBAoCEAEiDBoKEggKBBICCA0iACIHGgUKAzjoBxoKEggKBBICCAMiABILRU1QTE9ZRUVfSUQyCxBKKgdpc3RobXVz"),
                
                Arguments.of("ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSFRoTCAIQARoNZXF1YWw6YW55X2FueRISGhAIAhACGgpndDphbnlfYW55Eg8aDQgBEAMaB29yOmJvb2wa8w4S8A4K7Q466g4KCRIHCgUIBAECAxKgDhKdDgoCCgAS2QEK1gEKAgoAEqQBCgJpZAoEbmFtZQoDYWdlCgZzYWxhcnkKCmRlcGFydG1lbnQKCWhpcmVfZGF0ZQoJaXNfYWN0aXZlCgVzY29yZQoEY2l0eQoHY291bnRyeQoHcGFydF9pZBJKCgQqAhABCgRiAhABCgQqAhABCgnCAQYIAhAKIAEKBGICEAEKBYIBAhABCgQKAhABCgRaAhABCgRiAhABCgRiAhABCgRiAhABGAI6KQoKZ2x1ZV90ZWFtMgobdGVzdF90YWJsZV9xdWVyeV9mZWRlcmF0aW9uGroMGrcMGgQKAhABIqMMGqAMGp0MGgQKAhABIusJGugJGuUJGgQKAhABIm8abRprGgQKAhABIiUaIxohCAEaBAoCEAEiDBoKEggKBBICCAYiACIJGgcKBQgBkAMBIjwaOho4CAIaBAoCEAEiDBoKEggKBBICCAMiACIgGh4KHMIBFgoQwM9qAAAAAAAAAAAAAAAAABAHGAKQAwEi6wga6Aga5QgIAxoECgIQASKsCBqpCBqmCAgDGgQKAhABIucHGuQHGuEHCAMaBAoCEAEipgcaowcaoAcIAxoECgIQASLmBhrjBhrgBggDGgQKAhABIqcGGqQGGqEGCAMaBAoCEAEi5wUa5AUa4QUIAxoECgIQASKmBRqjBRqgBQgDGgQKAhABIuMEGuAEGt0ECAMaBAoCEAEiogQanwQanAQIAxoECgIQASLfAxrcAxrZAwgDGgQKAhABIqADGp0DGpoDCAMaBAoCEAEi4QIa3gIa2wIIAxoECgIQASKjAhqgAhqdAggDGgQKAhABIuUBGuIBGt8BCAMaBAoCEAEipgEaowEaoAEIAxoECgIQASJmGmQaYggDGgQKAhABIisaKRonCAEaBAoCEAEiDBoKEggKBBICCAgiACIPGg0KC2IGQXVzdGlukAMBIisaKRonCAEaBAoCEAEiDBoKEggKBBICCAgiACIPGg0KC2IGQm9zdG9ukAMBIi4aLBoqCAEaBAoCEAEiDBoKEggKBBICCAgiACISGhAKDmIJQ2hhcmxvdHRlkAMBIiwaKhooCAEaBAoCEAEiDBoKEggKBBICCAgiACIQGg4KDGIHQ2hpY2Fnb5ADASIrGikaJwgBGgQKAhABIgwaChIICgQSAggIIgAiDxoNCgtiBkRhbGxhc5ADASIrGikaJwgBGgQKAhABIgwaChIICgQSAggIIgAiDxoNCgtiBkRlbnZlcpADASIsGioaKAgBGgQKAhABIgwaChIICgQSAggIIgAiEBoOCgxiB0RldHJvaXSQAwEiLBoqGigIARoECgIQASIMGgoSCAoEEgIICCIAIhAaDgoMYgdIb3VzdG9ukAMBIjAaLhosCAEaBAoCEAEiDBoKEggKBBICCAgiACIUGhIKEGILS2Fuc2FzIENpdHmQAwEiLhosGioIARoECgIQASIMGgoSCAoEEgIICCIAIhIaEAoOYglMYXMgVmVnYXOQAwEiMBouGiwIARoECgIQASIMGgoSCAoEEgIICCIAIhQaEgoQYgtMb3MgQW5nZWxlc5ADASIuGiwaKggBGgQKAhABIgwaChIICgQSAggIIgAiEhoQCg5iCU5hc2h2aWxsZZADASItGisaKQgBGgQKAhABIgwaChIICgQSAggIIgAiERoPCg1iCE5ldyBZb3JrkAMBIiwaKhooCAEaBAoCEAEiDBoKEggKBBICCAgiACIQGg4KDGIHUGhvZW5peJADASItGisaKQgBGgQKAhABIgwaChIICgQSAggIIgAiERoPCg1iCFBvcnRsYW5kkAMBIi4aLBoqCAEaBAoCEAEiDBoKEggKBBICCAgiACISGhAKDmIJU2FuIERpZWdvkAMBIjIaMBouCAEaBAoCEAEiDBoKEggKBBICCAgiACIWGhQKEmINU2FuIEZyYW5jaXNjb5ADASIsGioaKAgBGgQKAhABIgwaChIICgQSAggIIgAiEBoOCgxiB1NlYXR0bGWQAwEipgIaowIaoAIIAxoECgIQASLpARrmARrjAQgDGgQKAhABIqgBGqUBGqIBCAMaBAoCEAEiZxplGmMIAxoECgIQASIwGi4aLAgBGgQKAhABIgwaChIICgQSAggEIgAiFBoSChBiC0VuZ2luZWVyaW5nkAMBIicaJRojCAEaBAoCEAEiDBoKEggKBBICCAQiACILGgkKB2ICSFKQAwEiLxotGisIARoECgIQASIMGgoSCAoEEgIIBCIAIhMaEQoPYgpNYW5hZ2VtZW50kAMBIi4aLBoqCAEaBAoCEAEiDBoKEggKBBICCAQiACISGhAKDmIJTWFya2V0aW5nkAMBIioaKBomCAEaBAoCEAEiDBoKEggKBBICCAQiACIOGgwKCmIFU2FsZXOQAwEiCRoHCgUIAZADARoKEggKBBICCAgiABoKEggKBBICCAQiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiAA=="),
                Arguments.of("ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSFRoTCAIQARoNZXF1YWw6YW55X2FueRISGhAIAhACGgpndDphbnlfYW55GuYCEuMCCuACGt0CCgIKABLSAhLPAgoCCgAS2QEK1gEKAgoAEqQBCgJpZAoEbmFtZQoDYWdlCgZzYWxhcnkKCmRlcGFydG1lbnQKCWhpcmVfZGF0ZQoJaXNfYWN0aXZlCgVzY29yZQoEY2l0eQoHY291bnRyeQoHcGFydF9pZBJKCgQqAhABCgRiAhABCgQqAhABCgnCAQYIAhAKIAEKBGICEAEKBYIBAhABCgQKAhABCgRaAhABCgRiAhABCgRiAhABCgRiAhABGAI6KQoKZ2x1ZV90ZWFtMgobdGVzdF90YWJsZV9xdWVyeV9mZWRlcmF0aW9uGm0aaxoECgIQASJYGlYaVBoECgIQASIlGiMaIQgBGgQKAhABIgwaChIICgQSAggGIgAiCRoHCgUIAJADASIlGiMaIQgCGgQKAhABIgwaChIICgQSAggCIgAiCRoHCgUoHpADASIJGgcKBQgBkAMBGAAgZA=="),
                Arguments.of("GucFEuQFCuEFGt4FCgIKABLTBSrQBQoCCgASuQUKtgUKAgoAEpUFChFjY19jYWxsX2NlbnRlcl9zawoRY2NfY2FsbF9jZW50ZXJfaWQKEWNjX3JlY19zdGFydF9kYXRlCg9jY19yZWNfZW5kX2RhdGUKEWNjX2Nsb3NlZF9kYXRlX3NrCg9jY19vcGVuX2RhdGVfc2sKB2NjX25hbWUKCGNjX2NsYXNzCgxjY19lbXBsb3llZXMKCGNjX3NxX2Z0CghjY19ob3VycwoKY2NfbWFuYWdlcgoJY2NfbWt0X2lkCgxjY19ta3RfY2xhc3MKC2NjX21rdF9kZXNjChFjY19tYXJrZXRfbWFuYWdlcgoLY2NfZGl2aXNpb24KEGNjX2RpdmlzaW9uX25hbWUKCmNjX2NvbXBhbnkKD2NjX2NvbXBhbnlfbmFtZQoQY2Nfc3RyZWV0X251bWJlcgoOY2Nfc3RyZWV0X25hbWUKDmNjX3N0cmVldF90eXBlCg9jY19zdWl0ZV9udW1iZXIKB2NjX2NpdHkKCWNjX2NvdW50eQoIY2Nfc3RhdGUKBmNjX3ppcAoKY2NfY291bnRyeQoNY2NfZ210X29mZnNldAoRY2NfdGF4X3BlcmNlbnRhZ2UKB3BhcnRfaWQSzgEKBCoCEAEKBGICEAEKBYIBAhABCgWCAQIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoJwgEGCAIQBSABCgnCAQYIAhAFIAEKBGICEAEYAjoYCgl3YXJlaG91c2UKC2NhbGxfY2VudGVyGg4KChIICgQSAggGIgAQAhgAIGQ="),
                Arguments.of("ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSFRoTCAIQARoNZXF1YWw6YW55X2FueRqzAhKwAgqtAhqqAgoCCgASnwISnAIKAgoAEtkBCtYBCgIKABKkAQoCaWQKBG5hbWUKA2FnZQoGc2FsYXJ5CgpkZXBhcnRtZW50CgloaXJlX2RhdGUKCWlzX2FjdGl2ZQoFc2NvcmUKBGNpdHkKB2NvdW50cnkKB3BhcnRfaWQSSgoEKgIQAQoEYgIQAQoEKgIQAQoJwgEGCAIQCiABCgRiAhABCgWCAQIQAQoECgIQAQoEWgIQAQoEYgIQAQoEYgIQAQoEYgIQARgCOikKCmdsdWVfdGVhbTIKG3Rlc3RfdGFibGVfcXVlcnlfZmVkZXJhdGlvbho6GjgaBAoCEAEiJRojGiEIARoECgIQASIMGgoSCAoEEgIIBiIAIgkaBwoFCAGQAwEiCRoHCgUIAZADARgAIAU="),
                Arguments.of("Go8BEowBCokBCoYBCgIKABJbCglkZXB0X25hbWUKDG1hbmFnZXJfbmFtZQoGYnVkZ2V0Cghsb2NhdGlvbgoHcGFydF9pZBIlCgRiAhABCgRiAhABCgnCAQYIAhAMIAEKBGICEAEKBGICEAEYAjojCgpnbHVlX3RlYW0yChV0ZXN0X3RhYmxlX2RlcGFydG1lbnQ="),
                
                // SELECT * FROM "warehouse"."call_center" ORDER BY "cc_rec_start_date" LIMIT 100
                Arguments.of("GucFEuQFCuEFGt4FCgIKABLTBSrQBQoCCgASuQUKtgUKAgoAEpUFChFjY19jYWxsX2NlbnRlcl9zawoRY2NfY2FsbF9jZW50ZXJfaWQKEWNjX3JlY19zdGFydF9kYXRlCg9jY19yZWNfZW5kX2RhdGUKEWNjX2Nsb3NlZF9kYXRlX3NrCg9jY19vcGVuX2RhdGVfc2sKB2NjX25hbWUKCGNjX2NsYXNzCgxjY19lbXBsb3llZXMKCGNjX3NxX2Z0CghjY19ob3VycwoKY2NfbWFuYWdlcgoJY2NfbWt0X2lkCgxjY19ta3RfY2xhc3MKC2NjX21rdF9kZXNjChFjY19tYXJrZXRfbWFuYWdlcgoLY2NfZGl2aXNpb24KEGNjX2RpdmlzaW9uX25hbWUKCmNjX2NvbXBhbnkKD2NjX2NvbXBhbnlfbmFtZQoQY2Nfc3RyZWV0X251bWJlcgoOY2Nfc3RyZWV0X25hbWUKDmNjX3N0cmVldF90eXBlCg9jY19zdWl0ZV9udW1iZXIKB2NjX2NpdHkKCWNjX2NvdW50eQoIY2Nfc3RhdGUKBmNjX3ppcAoKY2NfY291bnRyeQoNY2NfZ210X29mZnNldAoRY2NfdGF4X3BlcmNlbnRhZ2UKB3BhcnRfaWQSzgEKBCoCEAEKBGICEAEKBYIBAhABCgWCAQIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoJwgEGCAIQBSABCgnCAQYIAhAFIAEKBGICEAEYAjoYCgl3YXJlaG91c2UKC2NhbGxfY2VudGVyGg4KChIICgQSAggCIgAQAhgAIGQ="),
                
                Arguments.of("ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSEhoQCAIQARoKZ3Q6YW55X2FueRIVGhMIAhACGg1lcXVhbDphbnlfYW55GuYCEuMCCuACGt0CCgIKABLSAhLPAgoCCgAS2QEK1gEKAgoAEqQBCgJpZAoEbmFtZQoDYWdlCgZzYWxhcnkKCmRlcGFydG1lbnQKCWhpcmVfZGF0ZQoJaXNfYWN0aXZlCgVzY29yZQoEY2l0eQoHY291bnRyeQoHcGFydF9pZBJKCgQqAhABCgRiAhABCgQqAhABCgnCAQYIAhAKIAEKBGICEAEKBYIBAhABCgQKAhABCgRaAhABCgRiAhABCgRiAhABCgRiAhABGAI6KQoKZ2x1ZV90ZWFtMgobdGVzdF90YWJsZV9xdWVyeV9mZWRlcmF0aW9uGm0aaxoECgIQASJYGlYaVBoECgIQASIlGiMaIQgBGgQKAhABIgwaChIICgQSAggCIgAiCRoHCgUoIJADASIlGiMaIQgCGgQKAhABIgwaChIICgQSAggGIgAiCRoHCgUIAZADASIJGgcKBQgBkAMBGAAgBQ=="),
                Arguments.of("GucFEuQFCuEFGt4FCgIKABLTBSrQBQoCCgASuQUKtgUKAgoAEpUFChFjY19jYWxsX2NlbnRlcl9zawoRY2NfY2FsbF9jZW50ZXJfaWQKEWNjX3JlY19zdGFydF9kYXRlCg9jY19yZWNfZW5kX2RhdGUKEWNjX2Nsb3NlZF9kYXRlX3NrCg9jY19vcGVuX2RhdGVfc2sKB2NjX25hbWUKCGNjX2NsYXNzCgxjY19lbXBsb3llZXMKCGNjX3NxX2Z0CghjY19ob3VycwoKY2NfbWFuYWdlcgoJY2NfbWt0X2lkCgxjY19ta3RfY2xhc3MKC2NjX21rdF9kZXNjChFjY19tYXJrZXRfbWFuYWdlcgoLY2NfZGl2aXNpb24KEGNjX2RpdmlzaW9uX25hbWUKCmNjX2NvbXBhbnkKD2NjX2NvbXBhbnlfbmFtZQoQY2Nfc3RyZWV0X251bWJlcgoOY2Nfc3RyZWV0X25hbWUKDmNjX3N0cmVldF90eXBlCg9jY19zdWl0ZV9udW1iZXIKB2NjX2NpdHkKCWNjX2NvdW50eQoIY2Nfc3RhdGUKBmNjX3ppcAoKY2NfY291bnRyeQoNY2NfZ210X29mZnNldAoRY2NfdGF4X3BlcmNlbnRhZ2UKB3BhcnRfaWQSzgEKBCoCEAEKBGICEAEKBYIBAhABCgWCAQIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoJwgEGCAIQBSABCgnCAQYIAhAFIAEKBGICEAEYAjoYCgl3YXJlaG91c2UKC2NhbGxfY2VudGVyGg4KChIICgQSAggCIgAQAhgAIGQ="),
                Arguments.of("GpMCEpACCo0CGooCCgIKABL/ASr8AQoCCgAS5QEK4gEKAgoAEscBCgpvX29yZGVya2V5CglvX2N1c3RrZXkKDW9fb3JkZXJzdGF0dXMKDG9fdG90YWxwcmljZQoLb19vcmRlcmRhdGUKD29fb3JkZXJwcmlvcml0eQoHb19jbGVyawoOb19zaGlwcHJpb3JpdHkKCW9fY29tbWVudAoJcGFydGl0aW9uEkQKBDoCEAEKBDoCEAEKBGICEAEKCcIBBggCEAwgAQoFggECEAEKBGICEAEKBGICEAEKBDoCEAEKBGICEAEKBGICEAEYAjoSCgh0cGNoX3NmMQoGb3JkZXJzGg4KChIICgQSAggEIgAQAhgAIGQ=")
        );
    }

    @Test
    public void testPrepareStatementWithCalciteSql_ParameterCountMismatch() throws Exception {
        // SELECT * FROM testdb.users WHERE age > 60
        String base64EncodedPlanWhere = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSEhoQCAIQARoKZ3Q6YW55X2FueRq8ARK5AQq2ARKzAQoCCgAScQpvCgIKABJYCgJpZAoEbmFtZQoFZW1haWwKA2FnZQoIbG9jYXRpb24KDnBhcnRpdGlvbl9uYW1lEiYKBCoCEAEKBGICEAEKBGICEAEKBCoCEAEKBGICEAEKBGICEAEYAjoPCgZ0ZXN0ZGIKBXVzZXJzGjoaOBoECgIQASIlGiMaIQgBGgQKAhABIgwaChIICgQSAggDIgAiCRoHCgUoPJADASIJGgcKBQgBkAMB";
        QueryPlan queryPlan = mock(QueryPlan.class);
        Constraints constraintsWithQueryPlan = mock(Constraints.class);

        ParameterMetaData mockParameterMetaData = mock(ParameterMetaData.class);
        when(mockParameterMetaData.getParameterCount()).thenReturn(0);
        when(mockStatement.getParameterMetaData()).thenReturn(mockParameterMetaData);

        when(queryPlan.getSubstraitPlan()).thenReturn(base64EncodedPlanWhere);
        when(constraintsWithQueryPlan.getQueryPlan()).thenReturn(queryPlan);

        PreparedStatement result = builder.prepareStatementWithCalciteSql(mockConnection, constraintsWithQueryPlan, AnsiSqlDialect.DEFAULT, split);

        assertNotNull(result);
        verify(mockConnection).prepareStatement(contains("SELECT"));
        verify(mockConnection).prepareStatement(contains("`testdb`.`users`"));
        verify(mockConnection).prepareStatement(contains("`age` > ?"));
    }
}
