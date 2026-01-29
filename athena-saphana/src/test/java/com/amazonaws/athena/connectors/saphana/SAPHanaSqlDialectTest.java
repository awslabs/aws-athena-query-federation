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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class SAPHanaSqlDialectTest
{
    @Mock
    private RelDataTypeFactory typeFactory;
    
    @Mock
    private RelDataType booleanType;
    
    @Mock
    private SqlWriter writer;

    public SAPHanaSqlDialectTest()
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
        SqlDialect.Context context = SAPHanaSqlDialect.DEFAULT_CONTEXT;
        assertEquals("\"", context.identifierQuoteString());
        assertEquals(Casing.TO_UPPER, context.unquotedCasing());
        assertEquals(Casing.UNCHANGED, context.quotedCasing());
        assertFalse(context.caseSensitive());
    }

    @Test
    public void testCustomDialectCreation()
    {
        SAPHanaSqlDialect dialect = new SAPHanaSqlDialect(SAPHanaSqlDialect.DEFAULT_CONTEXT);
        assertFalse(dialect.supportsApproxCountDistinct());
    }

    @Test
    public void testRewriteMaxMinExpr()
    {
        SAPHanaSqlDialect dialect = SAPHanaSqlDialect.DEFAULT;
        when(booleanType.getSqlTypeName()).thenReturn(SqlTypeName.BOOLEAN);
        
        // Test with a simple literal node (not a SqlBasicCall)
        SqlNode literal = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
        SqlNode result = dialect.rewriteMaxMinExpr(literal, booleanType);
        assertEquals(literal, result); // Should return unchanged for non-SqlBasicCall
    }
}
