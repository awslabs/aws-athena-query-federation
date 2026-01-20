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

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubstraitAccumulatorVisitorTest
{
    private List<SubstraitTypeAndValue> accumulator;
    private Map<String, String> splitProperties;
    private Schema schema;
    private SubstraitAccumulatorVisitor visitor;

    @Before
    public void setUp()
    {
        accumulator = new ArrayList<>();
        splitProperties = new HashMap<>();

        // Create a test schema with various field types
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("int_col", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field("bigint_col", FieldType.nullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field("float_col", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
        fields.add(new Field("double_col", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        fields.add(new Field("varchar_col", FieldType.nullable(new ArrowType.Utf8()), null));
        fields.add(new Field("bool_col", FieldType.nullable(new ArrowType.Bool()), null));
        fields.add(new Field("decimal_col", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null));
        fields.add(new Field("date_col", FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)), null));
        fields.add(new Field("time_col", FieldType.nullable(new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32)), null));
        fields.add(new Field("timestamp_col", FieldType.nullable(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")), null));
        fields.add(new Field("binary_col", FieldType.nullable(new ArrowType.Binary()), null));

        schema = new Schema(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, splitProperties, schema);
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
        // Add a null type field to schema
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.add(new Field("null_col", FieldType.nullable(new ArrowType.Null()), null));
        schema = new Schema(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, splitProperties, schema);

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
    public void testVisitSqlLiteralWithLargeUtf8Column()
    {
        // Add a large utf8 field to schema
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.add(new Field("large_utf8_col", FieldType.nullable(new ArrowType.LargeUtf8()), null));
        schema = new Schema(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, splitProperties, schema);

        SqlIdentifier identifier = new SqlIdentifier("large_utf8_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createCharString("large_string", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.VARCHAR, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithLargeBinaryColumn()
    {
        // Add a large binary field to schema
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.add(new Field("large_binary_col", FieldType.nullable(new ArrowType.LargeBinary()), null));
        schema = new Schema(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, splitProperties, schema);

        SqlIdentifier identifier = new SqlIdentifier("large_binary_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createBinaryString("ABCD", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.VARBINARY, accumulator.get(0).getType());
    }

    @Test
    public void testVisitSqlLiteralWithUnsupportedArrowType()
    {
        // Add an unsupported field type to schema
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.add(new Field("unsupported_col", FieldType.nullable(new ArrowType.List()), null));
        schema = new Schema(fields);
        visitor = new SubstraitAccumulatorVisitor(accumulator, splitProperties, schema);

        SqlIdentifier identifier = new SqlIdentifier("unsupported_col", SqlParserPos.ZERO);
        visitor.visit(identifier);

        SqlLiteral literal = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        SqlNode result = visitor.visit(literal);

        assertTrue(result instanceof SqlDynamicParam);
        assertEquals(1, accumulator.size());
        assertEquals(SqlTypeName.VARCHAR, accumulator.get(0).getType()); // Falls back to VARCHAR
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
