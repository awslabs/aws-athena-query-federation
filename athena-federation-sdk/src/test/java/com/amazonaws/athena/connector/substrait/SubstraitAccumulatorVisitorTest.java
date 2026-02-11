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
package com.amazonaws.athena.connector.substrait;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubstraitAccumulatorVisitorTest
{
    private List<SubstraitTypeAndValue> accumulator;
    private RelDataType schema;
    private SubstraitAccumulatorVisitor visitor;
    private RelDataTypeFactory typeFactory;

    @Before
    public void setUp()
    {
        accumulator = new ArrayList<>();
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        // Create a test schema with various field types
        schema = createTestSchema();
        visitor = new SubstraitAccumulatorVisitor(accumulator, schema);
    }

    private RelDataType createTestSchema()
    {
        return typeFactory.createStructType(Arrays.asList(
            Pair.of("int_col", typeFactory.createSqlType(SqlTypeName.INTEGER)),
            Pair.of("bigint_col", typeFactory.createSqlType(SqlTypeName.BIGINT)),
            Pair.of("float_col", typeFactory.createSqlType(SqlTypeName.FLOAT)),
            Pair.of("double_col", typeFactory.createSqlType(SqlTypeName.DOUBLE)),
            Pair.of("varchar_col", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("bool_col", typeFactory.createSqlType(SqlTypeName.BOOLEAN)),
            Pair.of("decimal_col", typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2)),
            Pair.of("date_col", typeFactory.createSqlType(SqlTypeName.DATE)),
            Pair.of("time_col", typeFactory.createSqlType(SqlTypeName.TIME)),
            Pair.of("timestamp_col", typeFactory.createSqlType(SqlTypeName.TIMESTAMP)),
            Pair.of("binary_col", typeFactory.createSqlType(SqlTypeName.VARBINARY))
        ));
    }

    @Test
    public void testVisitSqlIdentifier()
    {
        SqlIdentifier identifier = new SqlIdentifier("int_col", SqlParserPos.ZERO);

        SqlNode result = visitor.visit(identifier);

        assertEquals(identifier, result);
    }

    @Test
    public void testVisitSqlLiteralWithIntegerColumn()
    {
        // First visit identifier to set current column
        SqlIdentifier identifier = new SqlIdentifier("int_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        // Then visit literal
        SqlLiteral literal = SqlLiteral.createExactNumeric("123", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.INTEGER, accumulator.get(0).getType());
        assertEquals("123", accumulator.get(0).getValue().toString());
        assertEquals("int_col", accumulator.get(0).getColumnName());
    }

    @Test
    public void testVisitSqlLiteralWithBigIntColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("bigint_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createExactNumeric("9223372036854775807", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.BIGINT, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithFloatColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("float_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createApproxNumeric("3.14", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.FLOAT, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithDoubleColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("double_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createApproxNumeric("3.141592653589793", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.DOUBLE, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithVarcharColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("varchar_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createCharString("test_string", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.VARCHAR, accumulator.get(0).getType());
        assertEquals("test_string", accumulator.get(0).getValue());
    }

    @Test
    public void testVisitSqlLiteralWithNlsString()
    {
        SqlIdentifier identifier = new SqlIdentifier("varchar_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        NlsString nlsString = new NlsString("nls_test", "UTF-8", null);
        SqlLiteral literal = mock(SqlLiteral.class);
        when(literal.getValue()).thenReturn(nlsString);
        when(literal.getParserPosition()).thenReturn(SqlParserPos.ZERO);

        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.VARCHAR, accumulator.get(0).getType());
        assertEquals("nls_test", accumulator.get(0).getValue());
    }

    @Test
    public void testVisitSqlLiteralWithBooleanColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("bool_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.BOOLEAN, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithDecimalColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("decimal_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createExactNumeric("123.45", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.DECIMAL, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithDateColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("date_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createDate(org.apache.calcite.util.DateString.fromCalendarFields(java.util.Calendar.getInstance()), SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.DATE, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithTimeColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("time_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createTime(org.apache.calcite.util.TimeString.fromCalendarFields(java.util.Calendar.getInstance()), 0, SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.TIME, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithTimestampColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("timestamp_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createTimestamp(org.apache.calcite.util.TimestampString.fromCalendarFields(java.util.Calendar.getInstance()), 0, SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.TIMESTAMP, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithBinaryColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("binary_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createBinaryString("ABCD", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.VARBINARY, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithNullColumn()
    {
        // Create schema with null type field
        List<Pair<String, RelDataType>> fields = new ArrayList<>();
        for (String fieldName : schema.getFieldNames()) {
            fields.add(Pair.of(fieldName, schema.getField(fieldName, false, false).getType()));
        }
        fields.add(Pair.of("null_col", typeFactory.createSqlType(SqlTypeName.NULL)));
        schema = typeFactory.createStructType(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, schema);

        SqlIdentifier identifier = new SqlIdentifier("null_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createNull(SqlParserPos.ZERO);

        // SubstraitTypeAndValue requires non-null values, so this should throw a NullPointerException
        NullPointerException exception = assertThrows(NullPointerException.class, () -> {
            visitor.visit(literal);
        });

        assertTrue(exception.getMessage().contains("value is null"));
    }

    @Test
    public void testVisitSqlLiteralWithCharColumn()
    {
        // Create schema with char type field
        List<Pair<String, RelDataType>> fields = new ArrayList<>();
        for (String fieldName : schema.getFieldNames()) {
            fields.add(Pair.of(fieldName, schema.getField(fieldName, false, false).getType()));
        }
        fields.add(Pair.of("char_col", typeFactory.createSqlType(SqlTypeName.CHAR)));
        schema = typeFactory.createStructType(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, schema);

        SqlIdentifier identifier = new SqlIdentifier("char_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createCharString("test_char", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.CHAR, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithArrayColumn()
    {
        // Create schema with array type field
        List<Pair<String, RelDataType>> fields = new ArrayList<>();
        for (String fieldName : schema.getFieldNames()) {
            fields.add(Pair.of(fieldName, schema.getField(fieldName, false, false).getType()));
        }
        fields.add(Pair.of("array_col", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1)));
        schema = typeFactory.createStructType(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, schema);

        SqlIdentifier identifier = new SqlIdentifier("array_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.ARRAY, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithMapColumn()
    {
        // Create schema with map type field
        List<Pair<String, RelDataType>> fields = new ArrayList<>();
        for (String fieldName : schema.getFieldNames()) {
            fields.add(Pair.of(fieldName, schema.getField(fieldName, false, false).getType()));
        }
        fields.add(Pair.of("map_col", typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.INTEGER))));
        schema = typeFactory.createStructType(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, schema);

        SqlIdentifier identifier = new SqlIdentifier("map_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.MAP, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithoutCurrentColumnShouldIgnore()
    {
        SqlLiteral literal = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        visitor.visit(literal);
    }

    @Test
    public void testVisitSqlLiteralWithNonExistentColumn()
    {
        SqlIdentifier identifier = new SqlIdentifier("non_existent_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createCharString("test", SqlParserPos.ZERO);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            visitor.visit(literal);
        });

        assertTrue(exception.getMessage().contains("field non_existent_col not found"));
    }

    @Test
    public void testMultipleLiteralsAccumulation()
    {
        // Visit multiple identifiers and literals
        SqlIdentifier identifier1 = new SqlIdentifier("int_col", SqlParserPos.ZERO);
        visitor.visit(identifier1);
        SqlLiteral literal1 = SqlLiteral.createExactNumeric("123", SqlParserPos.ZERO);
        visitor.visit(literal1);

        SqlIdentifier identifier2 = new SqlIdentifier("varchar_col", SqlParserPos.ZERO);
        visitor.visit(identifier2);
        SqlLiteral literal2 = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        visitor.visit(literal2);

        assertEquals(2, accumulator.size());
        assertEquals(SqlTypeName.INTEGER, accumulator.get(0).getType());
        assertEquals("int_col", accumulator.get(0).getColumnName());
        assertEquals(SqlTypeName.VARCHAR, accumulator.get(1).getType());
        assertEquals("varchar_col", accumulator.get(1).getColumnName());
    }
}
