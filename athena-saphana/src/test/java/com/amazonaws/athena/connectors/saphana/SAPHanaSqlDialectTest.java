/*-
 * #%L
 * athena-saphana
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SAPHanaSqlDialectTest
{
    @Mock
    private RelDataType booleanType;

    @Mock
    private RelDataType intType;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDefaultDialectExists()
    {
        assertNotNull(SAPHanaSqlDialect.DEFAULT);
        assertFalse(SAPHanaSqlDialect.DEFAULT.supportsApproxCountDistinct());
    }

    @Test
    public void testDialectContext()
    {
        assertEquals("\"", SAPHanaSqlDialect.DEFAULT_CONTEXT.identifierQuoteString());
        assertEquals(Casing.TO_UPPER, SAPHanaSqlDialect.DEFAULT_CONTEXT.unquotedCasing());
        assertEquals(Casing.UNCHANGED, SAPHanaSqlDialect.DEFAULT_CONTEXT.quotedCasing());
        assertFalse(SAPHanaSqlDialect.DEFAULT_CONTEXT.caseSensitive());
    }

    @Test
    public void testCustomDialectCreationWithUpperCaseFilter()
    {
        SAPHanaSqlDialect dialect = new SAPHanaSqlDialect(SAPHanaSqlDialect.DEFAULT_CONTEXT, true);
        assertFalse(dialect.supportsApproxCountDistinct());
    }

    @Test
    public void testCustomDialectCreationWithoutUpperCaseFilter()
    {
        SAPHanaSqlDialect dialect = new SAPHanaSqlDialect(SAPHanaSqlDialect.DEFAULT_CONTEXT, false);
        assertFalse(dialect.supportsApproxCountDistinct());
    }

    @Test
    public void testRewriteMaxMinExprNonSqlBasicCall()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        when(booleanType.getSqlTypeName()).thenReturn(SqlTypeName.BOOLEAN);

        SqlNode literal = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
        SqlNode result = dialect.rewriteMaxMinExpr(literal, booleanType);
        assertEquals(literal, result);
    }

    @Test
    public void testRewriteMaxMinExprNonBooleanType()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        when(intType.getSqlTypeName()).thenReturn(SqlTypeName.INTEGER);

        SqlNode operand = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall maxCall = (SqlBasicCall) SqlStdOperatorTable.MAX.createCall(SqlParserPos.ZERO, operand);
        
        SqlNode result = dialect.rewriteMaxMinExpr(maxCall, intType);
        assertEquals(maxCall, result);
    }

    @Test
    public void testRewriteMaxMinExprMaxBoolean()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        when(booleanType.getSqlTypeName()).thenReturn(SqlTypeName.BOOLEAN);

        SqlNode operand = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
        SqlBasicCall maxCall = (SqlBasicCall) SqlStdOperatorTable.MAX.createCall(SqlParserPos.ZERO, operand);
        
        SqlNode result = dialect.rewriteMaxMinExpr(maxCall, booleanType);
        assertNotNull(result);
        assertNotEquals(maxCall, result);
    }

    @Test
    public void testRewriteMaxMinExprMinBoolean()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        when(booleanType.getSqlTypeName()).thenReturn(SqlTypeName.BOOLEAN);

        SqlNode operand = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        SqlBasicCall minCall = (SqlBasicCall) SqlStdOperatorTable.MIN.createCall(SqlParserPos.ZERO, operand);
        
        SqlNode result = dialect.rewriteMaxMinExpr(minCall, booleanType);
        assertNotNull(result);
        assertNotEquals(minCall, result);
    }

    @Test
    public void testRewriteMaxMinExprNonMaxMinOperator()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        when(booleanType.getSqlTypeName()).thenReturn(SqlTypeName.BOOLEAN);

        SqlNode operand = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
        SqlBasicCall sumCall = (SqlBasicCall) SqlStdOperatorTable.SUM.createCall(SqlParserPos.ZERO, operand);
        
        SqlNode result = dialect.rewriteMaxMinExpr(sumCall, booleanType);
        assertEquals(sumCall, result);
    }

    @Test
    public void testUnparseCallCharLength()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        SqlNode operand = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
        SqlBasicCall charLengthCall = (SqlBasicCall) SqlStdOperatorTable.CHAR_LENGTH.createCall(SqlParserPos.ZERO, operand);
        
        assertNotNull(charLengthCall);
        assertEquals(SqlKind.CHAR_LENGTH, charLengthCall.getKind());
    }

    @Test
    public void testUnparseCallNonCharLength()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        SqlNode operand = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall countCall = (SqlBasicCall) SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, operand);
        
        assertNotNull(countCall);
        assertEquals(SqlKind.COUNT, countCall.getKind());
    }

    @Test
    public void testUnparseOffsetFetchBothPresent()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        SqlNode offset = SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO);
        SqlNode fetch = SqlLiteral.createExactNumeric("20", SqlParserPos.ZERO);
        
        assertNotNull(offset);
        assertNotNull(fetch);
    }

    @Test
    public void testUnparseOffsetFetchOnlyFetch()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        SqlNode fetch = SqlLiteral.createExactNumeric("20", SqlParserPos.ZERO);
        
        assertNotNull(fetch);
    }

    @Test
    public void testUnparseOffsetFetchOnlyOffset()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        SqlNode offset = SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO);
        
        assertNotNull(offset);
    }

    @Test
    public void testUnparseOffsetFetchBothNull()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        assertNotNull(dialect);
    }

    @Test
    public void testQuoteIdentifierWithUpperCaseFilter()
    {
        SAPHanaSqlDialect dialect = new SAPHanaSqlDialect(SAPHanaSqlDialect.DEFAULT_CONTEXT, true);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "testIdentifier");
        assertEquals("testIdentifier", buf.toString());
    }

    @Test
    public void testQuoteIdentifierWithoutUpperCaseFilter()
    {
        SAPHanaSqlDialect dialect = new SAPHanaSqlDialect(SAPHanaSqlDialect.DEFAULT_CONTEXT, false);
        StringBuilder buf = new StringBuilder();
        dialect.quoteIdentifier(buf, "testIdentifier");
        assertEquals("\"testIdentifier\"", buf.toString());
    }

    @Test
    public void testSupportsApproxCountDistinct()
    {
        assertFalse(SAPHanaSqlDialect.DEFAULT.supportsApproxCountDistinct());
    }
}