/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnowflakeQueryStringBuilderTest
{
    private static final String QUOTE_CHAR_DOUBLE = "\"";
    private static final String SCHEMA_PUBLIC = "public";
    private static final String TABLE_USERS = "users";
    private static final String COLUMN_ID = "id";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_AGE = "age";
    private static final String COLUMN_PRICE = "price";
    private static final String COLUMN_ACTIVE = "active";
    private static final String COLUMN_DATE = "date";
    private static final String COLUMN_CREATED_AT = "created_at";
    private static final String COLUMN_TIME = "time";
    private static final String COLUMN_INTERVAL = "interval";
    private static final String COLUMN_BINARY = "binary";
    private static final String COLUMN_FIXED = "fixed";
    private static final String COLUMN_NULL = "null";
    private static final String COLUMN_STRUCT = "struct";
    private static final String COLUMN_LIST = "list";
    private static final String COLUMN_FIXED_LIST = "fixedList";
    private static final String COLUMN_UNION = "union";
    private static final String COLUMN_UNKNOWN = "unknown";
    private static final String COLUMN_UNSUPPORTED = "unsupported";

    private static final String VALUE_JOHN = "John";
    private static final String VALUE_JANE = "Jane";
    private static final String VALUE_O_REILLY = "O'Reilly";
    private static final String VALUE_O_REILLYS = "O'Reilly's";
    private static final String VALUE_USER_QUOTE = "user\"name";
    private static final String VALUE_DATE_STRING = "2023-03-15T00:00";
    private static final String VALUE_DATE_DAYS = "18706";
    private static final String VALUE_TIME_STRING = "12:00:00";
    private static final String VALUE_INTERVAL_STRING = "1 day";
    private static final String VALUE_BINARY_DATA = "data";

    private static final int VALUE_AGE_30 = 30;
    private static final int VALUE_AGE_42 = 42;
    private static final int VALUE_ID_10 = 10;
    private static final int VALUE_ID_20 = 20;
    private static final double VALUE_PRICE_99_99 = 99.99;
    private static final long VALUE_TIMESTAMP_1711929600000L = 1711929600000L;

    private static final String EXPECTED_FROM_CLAUSE_WITH_SCHEMA = " FROM \"public\".\"users\" ";
    private static final String EXPECTED_FROM_CLAUSE_NO_SCHEMA = " FROM \"users\" ";
    private static final String EXPECTED_QUOTED_USERS = "\"users\"";
    private static final String EXPECTED_QUOTED_USER_QUOTE = "\"user\"\"name\"";
    private static final String EXPECTED_SINGLE_QUOTED_O_REILLY = "'O''Reilly'";
    private static final String EXPECTED_SINGLE_QUOTED_O_REILLYS = "'O''Reilly''s'";
    private static final String EXPECTED_PREDICATE_AGE_EQUALS = "age = 30";
    private static final String EXPECTED_PREDICATE_NAME_EQUALS = "name = 'John'";
    private static final String EXPECTED_PREDICATE_DATE_EQUALS = "date = '2023-03-15 00:00:00'";

    private SnowflakeQueryStringBuilder queryBuilder;
    private BlockAllocator allocator;

    @Mock
    private Connection mockConnection;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        queryBuilder = new SnowflakeQueryStringBuilder(QUOTE_CHAR_DOUBLE, null);
        allocator = new BlockAllocatorImpl();
    }

    private Schema createSimpleSchema(String fieldName, ArrowType fieldType) {
        return new Schema(Collections.singletonList(
            new Field(fieldName, new FieldType(true, fieldType, null), null)
        ));
    }

    private Schema createIdSchema() {
        return createSimpleSchema(COLUMN_ID, new ArrowType.Int(32, true));
    }

    private Schema createNameSchema() {
        return createSimpleSchema(COLUMN_NAME, new ArrowType.Utf8());
    }

    private Constraints createConstraintsWithLimit(long limit) {
        Constraints constraints = mock(Constraints.class);
        when(constraints.getLimit()).thenReturn(limit);
        return constraints;
    }

    private Map<String, ValueSet> createValueSetSummary(String columnName, ArrowType columnType, List<Range> ranges) {
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put(columnName, SortedRangeSet.copyOf(columnType, ranges, false));
        return summary;
    }

    private Constraints createConstraintsWithSummary(Map<String, ValueSet> summary) {
        Constraints constraints = mock(Constraints.class);
        when(constraints.getLimit()).thenReturn(0L);
        when(constraints.getSummary()).thenReturn(summary);
        return constraints;
    }

    @Test
    public void testGetFromClauseWithSplit() {
        String result = queryBuilder.getFromClauseWithSplit(null, SCHEMA_PUBLIC, TABLE_USERS, null);
        assertEquals(EXPECTED_FROM_CLAUSE_WITH_SCHEMA, result);
    }

    @Test
    public void testGetFromClauseWithSplit_NoSchema() {
        String result = queryBuilder.getFromClauseWithSplit(null, null, TABLE_USERS, null);
        assertEquals(EXPECTED_FROM_CLAUSE_NO_SCHEMA, result);
    }

    @Test
    public void testGetPartitionWhereClauses() {
        List<String> result = queryBuilder.getPartitionWhereClauses(null);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildSqlString_NoConstraints() throws SQLException {
        Schema tableSchema = createIdSchema();
        Constraints constraints = createConstraintsWithLimit(0L);

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("SELECT \"" + COLUMN_ID + "\" FROM \"" + SCHEMA_PUBLIC + "\".\"" + TABLE_USERS + "\" "));
    }

    @Test
    public void testBuildSqlString_WithConstraints() throws SQLException {
        Schema tableSchema = createIdSchema();
        Constraints constraints = createConstraintsWithLimit(10L);

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("LIMIT 10"));
    }

    @Test
    public void testBuildSqlString_EmptyColumns() throws SQLException {
        Schema tableSchema = createSimpleSchema("partition", new ArrowType.Int(32, true));
        Constraints constraints = createConstraintsWithLimit(0L);

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("SELECT null FROM \"" + SCHEMA_PUBLIC + "\".\"" + TABLE_USERS + "\" "));
    }

    @Test
    public void testBuildSqlString_WithWhereClause() throws SQLException {
        Schema tableSchema = createIdSchema();

        Map<String, ValueSet> summary = createValueSetSummary(
            COLUMN_ID,
            Types.MinorType.INT.getType(),
            Collections.singletonList(Range.equal(allocator, Types.MinorType.INT.getType(), VALUE_AGE_42))
        );

        Constraints constraints = createConstraintsWithSummary(summary);

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains(COLUMN_ID));
    }

    @Test
    public void testBuildSqlString_WithOrderBy() throws SQLException {
        Schema tableSchema = createIdSchema();
        Constraints constraints = createConstraintsWithLimit(0L);
        when(constraints.getOrderByClause()).thenReturn(
            Collections.singletonList(new OrderByField(COLUMN_ID, OrderByField.Direction.DESC_NULLS_LAST))
        );

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("ORDER BY"));
    }

    @Test
    public void testQuote() {
        String result = queryBuilder.quote(TABLE_USERS);
        assertEquals(EXPECTED_QUOTED_USERS, result);
    }

    @Test
    public void testQuote_WithQuotes() {
        String result = queryBuilder.quote(VALUE_USER_QUOTE);
        assertEquals(EXPECTED_QUOTED_USER_QUOTE, result);
    }

    @Test
    public void testSingleQuote() {
        String result = queryBuilder.singleQuote(VALUE_O_REILLY);
        assertEquals(EXPECTED_SINGLE_QUOTED_O_REILLY, result);
    }

    @Test
    public void testSingleQuote_WithSingleQuotes() {
        String result = queryBuilder.singleQuote(VALUE_O_REILLYS);
        assertEquals(EXPECTED_SINGLE_QUOTED_O_REILLYS, result);
    }

    @Test
    public void testToPredicate_SingleValue() {
        String predicate = queryBuilder.toPredicate(COLUMN_AGE, "=", VALUE_AGE_30, new ArrowType.Int(32, true));
        assertEquals(EXPECTED_PREDICATE_AGE_EQUALS, predicate);
    }

    @Test
    public void testToPredicate_StringValue() {
        String predicate = queryBuilder.toPredicate(COLUMN_NAME, "=", VALUE_JOHN, new ArrowType.Utf8());
        assertEquals(EXPECTED_PREDICATE_NAME_EQUALS, predicate);
    }

    @Test
    public void testToPredicate_DateValue() {
        String predicate = queryBuilder.toPredicate(COLUMN_DATE, "=", VALUE_DATE_STRING, new ArrowType.Date(DateUnit.DAY));
        assertEquals(EXPECTED_PREDICATE_DATE_EQUALS, predicate);
    }

    @Test
    public void testGetObjectForWhereClause_Int() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_AGE, VALUE_AGE_42, new ArrowType.Int(32, true));
        assertEquals(42L, result);
    }

    @Test
    public void testGetObjectForWhereClause_Decimal() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_PRICE, new BigDecimal("99.99"), new ArrowType.Decimal(10, 2));
        assertEquals(new BigDecimal("99.99"), result);
    }

    @Test
    public void testGetObjectForWhereClause_DecimalFromNumber() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_PRICE, VALUE_PRICE_99_99, new ArrowType.Decimal(10, 2));
        assertTrue(result instanceof BigDecimal);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetObjectForWhereClause_DecimalInvalidType() {
        queryBuilder.getObjectForWhereClause(COLUMN_PRICE, "invalid", new ArrowType.Decimal(10, 2));
    }

    @Test
    public void testGetObjectForWhereClause_FloatingPoint() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_PRICE, VALUE_PRICE_99_99, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE));
        assertEquals(VALUE_PRICE_99_99, result);
    }

    @Test
    public void testGetObjectForWhereClause_Bool() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_ACTIVE, true, new ArrowType.Bool());
        assertEquals(true, result);
    }

    @Test
    public void testGetObjectForWhereClause_Utf8() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_NAME, VALUE_JOHN, new ArrowType.Utf8());
        assertEquals(VALUE_JOHN, result);
    }

    @Test
    public void testGetObjectForWhereClause_Date_DateTimeString() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_DATE, VALUE_DATE_STRING, new ArrowType.Date(DateUnit.DAY));
        assertEquals("2023-03-15 00:00:00", result);
    }

    @Test
    public void testGetObjectForWhereClause_Date_DaysNumber() {
        Object result = queryBuilder.getObjectForWhereClause(COLUMN_DATE, VALUE_DATE_DAYS, new ArrowType.Date(DateUnit.DAY));
        assertTrue(result.toString().contains("2021-03-20"));
    }

    @Test
    public void testGetObjectForWhereClause_Timestamp() {
        // Set timezone to UTC for this test to ensure consistent results
        TimeZone originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            Object result = queryBuilder.getObjectForWhereClause(COLUMN_CREATED_AT, VALUE_TIMESTAMP_1711929600000L, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
            assertEquals("2024-04-01 00:00:00", result);
        } finally {
            // Restore original timezone
            TimeZone.setDefault(originalTimeZone);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_Time() {
        queryBuilder.getObjectForWhereClause(COLUMN_TIME, VALUE_TIME_STRING, new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_Interval() {
        queryBuilder.getObjectForWhereClause(COLUMN_INTERVAL, VALUE_INTERVAL_STRING, new ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.DAY_TIME));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_Binary() {
        queryBuilder.getObjectForWhereClause(COLUMN_BINARY, VALUE_BINARY_DATA, new ArrowType.Binary());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_FixedSizeBinary() {
        queryBuilder.getObjectForWhereClause(COLUMN_FIXED, VALUE_BINARY_DATA, new ArrowType.FixedSizeBinary(10));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_Null() {
        queryBuilder.getObjectForWhereClause(COLUMN_NULL, "value", new ArrowType.Null());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_Struct() {
        queryBuilder.getObjectForWhereClause(COLUMN_STRUCT, "value", new ArrowType.Struct());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_List() {
        queryBuilder.getObjectForWhereClause(COLUMN_LIST, "value", new ArrowType.List());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_FixedSizeList() {
        queryBuilder.getObjectForWhereClause(COLUMN_FIXED_LIST, "value", new ArrowType.FixedSizeList(5));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_Union() {
        queryBuilder.getObjectForWhereClause(COLUMN_UNION, "value", new ArrowType.Union(org.apache.arrow.vector.types.UnionMode.Sparse, new int[]{0, 1}));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetObjectForWhereClause_UnknownType() {
        // Test with a type that doesn't exist in the switch statement
        // This will trigger the default case which throws UnsupportedOperationException
        ArrowType unknownType = new ArrowType.Utf8() {
            @Override
            public ArrowTypeID getTypeID() {
                return ArrowTypeID.NONE;
            }
        };

        queryBuilder.getObjectForWhereClause(COLUMN_UNKNOWN, "value", unknownType);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToPredicateWithUnsupportedType() {
        queryBuilder.getObjectForWhereClause(COLUMN_UNSUPPORTED, "value", new ArrowType.Struct());
    }

    @Test
    public void testToPredicate_NoneValueSet() throws SQLException {
        Map<String, ValueSet> summary = new HashMap<>();
        // Create a "none" ValueSet by setting the third parameter to true
        summary.put(COLUMN_ID, SortedRangeSet.copyOf(Types.MinorType.INT.getType(), Collections.emptyList(), true));

        Constraints constraints = createConstraintsWithSummary(summary);
        Schema tableSchema = createIdSchema();

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("IS NULL"));
    }

    @Test
    public void testToPredicate_AllValueSet() throws SQLException {
        Map<String, ValueSet> summary = createValueSetSummary(
            COLUMN_ID,
            Types.MinorType.INT.getType(),
            Collections.singletonList(Range.all(allocator, Types.MinorType.INT.getType()))
        );

        Constraints constraints = createConstraintsWithSummary(summary);
        Schema tableSchema = createIdSchema();

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("IS NOT NULL"));
    }

    @Test
    public void testToPredicate_SingleRange() throws SQLException {
        Map<String, ValueSet> summary = createValueSetSummary(
            COLUMN_ID,
            Types.MinorType.INT.getType(),
            Collections.singletonList(Range.range(allocator, Types.MinorType.INT.getType(), VALUE_ID_10, true, VALUE_ID_20, false))
        );

        Constraints constraints = createConstraintsWithSummary(summary);
        Schema tableSchema = createIdSchema();

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains(">="));
        assertTrue(sql.contains("<"));
    }

    @Test
    public void testToPredicate_MultipleValues() throws SQLException {
        Map<String, ValueSet> summary = createValueSetSummary(
            COLUMN_ID,
            Types.MinorType.INT.getType(),
            List.of(Range.equal(allocator, Types.MinorType.INT.getType(), VALUE_ID_10),
                   Range.equal(allocator, Types.MinorType.INT.getType(), VALUE_ID_20))
        );

        Constraints constraints = createConstraintsWithSummary(summary);
        Schema tableSchema = createIdSchema();

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("IN ("));
    }

    @Test
    public void testToPredicate_MultipleStringValues() throws SQLException {
        Map<String, ValueSet> summary = createValueSetSummary(
            COLUMN_NAME,
            Types.MinorType.VARCHAR.getType(),
            List.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), VALUE_JOHN),
                   Range.equal(allocator, Types.MinorType.VARCHAR.getType(), VALUE_JANE))
        );

        Constraints constraints = createConstraintsWithSummary(summary);
        Schema tableSchema = createNameSchema();

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("IN ("));
        assertTrue(sql.contains("'" + VALUE_JOHN + "'"));
        assertTrue(sql.contains("'" + VALUE_JANE + "'"));
    }

    @Test
    public void testToPredicate_RangeWithAboveBound() throws SQLException {
        Map<String, ValueSet> summary = createValueSetSummary(
            COLUMN_ID,
            Types.MinorType.INT.getType(),
            Collections.singletonList(Range.greaterThan(allocator, Types.MinorType.INT.getType(), VALUE_ID_10))
        );

        Constraints constraints = createConstraintsWithSummary(summary);
        Schema tableSchema = createIdSchema();

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains(">"));
    }

    @Test
    public void testToPredicate_RangeWithBelowBound() throws SQLException {
        Map<String, ValueSet> summary = createValueSetSummary(
            COLUMN_ID,
            Types.MinorType.INT.getType(),
            Collections.singletonList(Range.lessThan(allocator, Types.MinorType.INT.getType(), VALUE_ID_20))
        );

        Constraints constraints = createConstraintsWithSummary(summary);
        Schema tableSchema = createIdSchema();

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("<"));
    }

    @Test
    public void testBuildSqlString_WithPartitionWhereClauses() throws SQLException {
        Schema tableSchema = createIdSchema();
        Constraints constraints = createConstraintsWithLimit(0L);
        Split split = mock(Split.class);

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, split);
        assertTrue(sql.contains("SELECT \"" + COLUMN_ID + "\" FROM \"" + SCHEMA_PUBLIC + "\".\"" + TABLE_USERS + "\" "));
    }

    @Test
    public void testBuildSqlString_WithLimitAndOffset() throws SQLException {
        Schema tableSchema = createIdSchema();
        Constraints constraints = createConstraintsWithLimit(10L);

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        assertTrue(sql.contains("LIMIT 10"));
    }

    @Test
    public void testBuildSqlString_WithLimitOnly() throws SQLException {
        Schema tableSchema = createIdSchema();
        Constraints constraints = createConstraintsWithLimit(0L);

        String sql = queryBuilder.buildSqlString(mockConnection, null, SCHEMA_PUBLIC, TABLE_USERS, tableSchema, constraints, null);
        // When limit is 0, it should not append LIMIT clause
        assertTrue(!sql.contains("LIMIT"));
    }
}
