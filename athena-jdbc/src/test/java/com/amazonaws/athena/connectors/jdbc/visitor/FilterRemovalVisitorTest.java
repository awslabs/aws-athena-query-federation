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
package com.amazonaws.athena.connectors.jdbc.visitor;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterRemovalVisitorTest
{
    private Set<String> targetColumns;
    private FilterRemovalVisitor visitor;

    @Before
    public void setUp()
    {
        targetColumns = new HashSet<>();
        targetColumns.add("partition_col");
        targetColumns.add("year");
        targetColumns.add("month");

        visitor = new FilterRemovalVisitor(targetColumns);
    }

    @Test
    public void testVisitDirectConditionOnTargetColumn()
    {
        // Create a mock SqlCall representing "partition_col = 'value'"
        SqlCall call = createMockSqlCall(SqlKind.EQUALS, "partition_col", "value");

        SqlNode result = visitor.visit(call);

        assertTrue(result instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) result;
        assertTrue(literal.booleanValue()); // Should return TRUE for AND context
    }

    @Test
    public void testVisitDirectConditionOnTargetColumnCaseInsensitive()
    {
        // Test case insensitive matching
        SqlCall call = createMockSqlCall(SqlKind.EQUALS, "PARTITION_COL", "value");

        SqlNode result = visitor.visit(call);

        assertTrue(result instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) result;
        assertTrue(literal.booleanValue()); // Should return TRUE for AND context
    }

    @Test
    public void testVisitDirectConditionOnNonTargetColumn()
    {
        // Create a mock SqlCall representing "regular_col = 'value'"
        SqlCall call = createMockSqlCall(SqlKind.EQUALS, "regular_col", "value");

        SqlNode result = visitor.visit(call);

        // Should return the original call since it's not on a target column
        assertEquals(call, result);
    }

    @Test
    public void testVisitAndConditionWithTargetColumns()
    {
        // Create mock SqlCalls for operands
        SqlCall leftOperand = createMockSqlCall(SqlKind.EQUALS, "partition_col", "2024");
        SqlCall rightOperand = createMockSqlCall(SqlKind.EQUALS, "regular_col", "value");

        // Create AND call
        SqlCall andCall = createMockAndOrCall(SqlKind.AND, leftOperand, rightOperand);

        SqlNode result = visitor.visit(andCall);

        assertTrue(result instanceof SqlCall);
        SqlCall resultCall = (SqlCall) result;
        assertEquals(SqlKind.AND, resultCall.getKind());
    }

    @Test
    public void testVisitOrConditionWithTargetColumns()
    {
        // Create mock SqlCalls for operands
        SqlCall leftOperand = createMockSqlCall(SqlKind.EQUALS, "partition_col", "2024");
        SqlCall rightOperand = createMockSqlCall(SqlKind.EQUALS, "regular_col", "value");

        // Create OR call
        SqlCall orCall = createMockAndOrCall(SqlKind.OR, leftOperand, rightOperand);

        SqlNode result = visitor.visit(orCall);

        assertTrue(result instanceof SqlCall);
        SqlCall resultCall = (SqlCall) result;
        assertEquals(SqlKind.OR, resultCall.getKind());
    }

    @Test
    public void testVisitCallWithNoOperands()
    {
        SqlCall call = mock(SqlCall.class);
        when(call.getKind()).thenReturn(SqlKind.EQUALS);
        when(call.operandCount()).thenReturn(0);
        when(call.getOperandList()).thenReturn(Arrays.asList());

        // Mock the operator
        SqlOperator operator = mock(SqlOperator.class);
        when(call.getOperator()).thenReturn(operator);

        SqlNode result = visitor.visit(call);

        // Should return the original call since there are no operands
        assertEquals(call, result);
    }

    @Test
    public void testVisitCallWithNonIdentifierFirstOperand()
    {
        SqlCall call = mock(SqlCall.class);
        when(call.getKind()).thenReturn(SqlKind.EQUALS);
        when(call.operandCount()).thenReturn(2);

        // First operand is not an SqlIdentifier
        SqlLiteral literal = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        SqlLiteral secondLiteral = SqlLiteral.createCharString("value", SqlParserPos.ZERO);
        when(call.operand(0)).thenReturn(literal);
        when(call.operand(1)).thenReturn(secondLiteral);
        when(call.getOperandList()).thenReturn(Arrays.asList(literal, secondLiteral));

        // Mock the operator
        SqlOperator operator = mock(SqlOperator.class);
        when(call.getOperator()).thenReturn(operator);

        SqlNode result = visitor.visit(call);

        // Should return the original call since first operand is not an identifier
        assertEquals(call, result);
    }

    @Test
    public void testVisitGreaterThanConditionOnTargetColumn()
    {
        SqlCall call = createMockSqlCall(SqlKind.GREATER_THAN, "year", "2023");

        SqlNode result = visitor.visit(call);

        assertTrue(result instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) result;
        assertTrue(literal.booleanValue()); // Should return TRUE for AND context
    }

    @Test
    public void testVisitLessThanConditionOnTargetColumn()
    {
        SqlCall call = createMockSqlCall(SqlKind.LESS_THAN, "month", "12");

        SqlNode result = visitor.visit(call);

        assertTrue(result instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) result;
        assertTrue(literal.booleanValue()); // Should return TRUE for AND context
    }

    @Test
    public void testVisitInConditionOnTargetColumn()
    {
        SqlCall call = createMockSqlCall(SqlKind.IN, "partition_col", "value1", "value2");

        SqlNode result = visitor.visit(call);

        assertTrue(result instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) result;
        assertTrue(literal.booleanValue()); // Should return TRUE for AND context
    }

    @Test
    public void testVisitLikeConditionOnTargetColumn()
    {
        SqlCall call = createMockSqlCall(SqlKind.LIKE, "partition_col", "2024%");

        SqlNode result = visitor.visit(call);

        assertTrue(result instanceof SqlLiteral);
        SqlLiteral literal = (SqlLiteral) result;
        assertTrue(literal.booleanValue()); // Should return TRUE for AND context
    }

    @Test
    public void testVisitComplexAndCondition()
    {
        // Create a complex AND condition: (partition_col = '2024') AND (regular_col = 'value') AND (year > 2020)
        SqlCall partitionCondition = createMockSqlCall(SqlKind.EQUALS, "partition_col", "2024");
        SqlCall regularCondition = createMockSqlCall(SqlKind.EQUALS, "regular_col", "value");
        SqlCall yearCondition = createMockSqlCall(SqlKind.GREATER_THAN, "year", "2020");

        SqlCall andCall = createMockAndOrCall(SqlKind.AND, partitionCondition, regularCondition, yearCondition);

        SqlNode result = visitor.visit(andCall);

        assertTrue(result instanceof SqlCall);
        SqlCall resultCall = (SqlCall) result;
        assertEquals(SqlKind.AND, resultCall.getKind());
    }

    @Test
    public void testVisitComplexOrCondition()
    {
        // Create a complex OR condition: (partition_col = '2024') OR (regular_col = 'value') OR (year > 2020)
        SqlCall partitionCondition = createMockSqlCall(SqlKind.EQUALS, "partition_col", "2024");
        SqlCall regularCondition = createMockSqlCall(SqlKind.EQUALS, "regular_col", "value");
        SqlCall yearCondition = createMockSqlCall(SqlKind.GREATER_THAN, "year", "2020");

        SqlCall orCall = createMockAndOrCall(SqlKind.OR, partitionCondition, regularCondition, yearCondition);

        SqlNode result = visitor.visit(orCall);

        assertTrue(result instanceof SqlCall);
        SqlCall resultCall = (SqlCall) result;
        assertEquals(SqlKind.OR, resultCall.getKind());
    }

    @Test
    public void testVisitNestedAndOrConditions()
    {
        // Create nested conditions: ((partition_col = '2024') AND (regular_col = 'value')) OR (year > 2020)
        SqlCall partitionCondition = createMockSqlCall(SqlKind.EQUALS, "partition_col", "2024");
        SqlCall regularCondition = createMockSqlCall(SqlKind.EQUALS, "regular_col", "value");
        SqlCall yearCondition = createMockSqlCall(SqlKind.GREATER_THAN, "year", "2020");

        SqlCall innerAnd = createMockAndOrCall(SqlKind.AND, partitionCondition, regularCondition);
        SqlCall outerOr = createMockAndOrCall(SqlKind.OR, innerAnd, yearCondition);

        SqlNode result = visitor.visit(outerOr);

        assertTrue(result instanceof SqlCall);
        SqlCall resultCall = (SqlCall) result;
        assertEquals(SqlKind.OR, resultCall.getKind());
    }

    @Test
    public void testVisitEmptyTargetColumns()
    {
        FilterRemovalVisitor emptyVisitor = new FilterRemovalVisitor(new HashSet<>());

        SqlCall call = createMockSqlCall(SqlKind.EQUALS, "any_column", "value");

        SqlNode result = emptyVisitor.visit(call);

        // Should return the original call since no target columns are specified
        assertEquals(call, result);
    }

    private SqlCall createMockSqlCall(SqlKind kind, String columnName, String... values)
    {
        SqlCall call = mock(SqlCall.class);
        when(call.getKind()).thenReturn(kind);
        when(call.operandCount()).thenReturn(1 + values.length);

        // First operand is the column identifier
        SqlIdentifier identifier = new SqlIdentifier(columnName, SqlParserPos.ZERO);
        when(call.operand(0)).thenReturn(identifier);

        // Additional operands are the values
        for (int i = 0; i < values.length; i++) {
            SqlLiteral literal = SqlLiteral.createCharString(values[i], SqlParserPos.ZERO);
            when(call.operand(i + 1)).thenReturn(literal);
        }

        // Create operand list for SqlShuttle compatibility
        List<SqlNode> operandList = new ArrayList<>();
        operandList.add(identifier);
        for (String value : values) {
            operandList.add(SqlLiteral.createCharString(value, SqlParserPos.ZERO));
        }
        when(call.getOperandList()).thenReturn(operandList);

        // Mock the operator
        SqlOperator operator = mock(SqlOperator.class);
        when(call.getOperator()).thenReturn(operator);

        return call;
    }

    private SqlCall createMockAndOrCall(SqlKind kind, SqlNode... operands)
    {
        SqlCall call = mock(SqlCall.class);
        when(call.getKind()).thenReturn(kind);
        when(call.operandCount()).thenReturn(operands.length);

        List<SqlNode> operandList = Arrays.asList(operands);
        when(call.getOperandList()).thenReturn(operandList);

        for (int i = 0; i < operands.length; i++) {
            when(call.operand(i)).thenReturn(operands[i]);
        }

        // Mock the operator to create new calls
        SqlOperator operator = mock(SqlOperator.class);
        when(call.getOperator()).thenReturn(operator);

        // Create a new mock call to return from createCall
        SqlCall newCall = mock(SqlCall.class);
        when(newCall.getKind()).thenReturn(kind);
        when(newCall.operandCount()).thenReturn(operands.length);
        when(newCall.getOperandList()).thenReturn(operandList);
        for (int i = 0; i < operands.length; i++) {
            when(newCall.operand(i)).thenReturn(operands[i]);
        }
        when(newCall.getOperator()).thenReturn(operator);

        // Properly stub the createCall method with specific signature
        when(operator.createCall(eq(SqlParserPos.ZERO), any(List.class))).thenReturn(newCall);

        return call;
    }
}
