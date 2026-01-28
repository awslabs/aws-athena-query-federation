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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Calcite SQL dialect for SAP HANA (Calcite 1.40.0).
 *
 * Behavior:
 *  - Unquoted identifiers are folded to UPPERCASE
 *  - Quoted identifiers preserve case
 *  - BOOLEAN MAX/MIN rewritten to BOOL aggregations
 *  - OFFSET/FETCH rendered as LIMIT/OFFSET
 */
public class SAPHanaSqlDialect extends SqlDialect
{
    public static final SqlDialect.Context DEFAULT_CONTEXT =
            SqlDialect.EMPTY_CONTEXT
                    .withDatabaseProduct(DatabaseProduct.UNKNOWN)
                    .withIdentifierQuoteString("\"")
                    .withUnquotedCasing(Casing.TO_UPPER)
                    .withQuotedCasing(Casing.UNCHANGED)
                    .withCaseSensitive(false);

    public static final SAPHanaSqlDialect DEFAULT =
            new SAPHanaSqlDialect(DEFAULT_CONTEXT);

    public SAPHanaSqlDialect(Context context)
    {
        super(context);
    }

    // ----------------------------------------------------------------------
    // Function rewriting
    // ----------------------------------------------------------------------

    @Override
    public void unparseCall(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec)
    {
        switch (call.getKind()) {
            case CHAR_LENGTH:
                // Normalize CHAR_LENGTH(x) -> LENGTH(x)
                SqlCall lengthCall =
                        SqlLibraryOperators.LENGTH.createCall(
                                SqlParserPos.ZERO,
                                call.getOperandList());
                super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
                return;

            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    // ----------------------------------------------------------------------
    // Aggregate rewrites
    // ----------------------------------------------------------------------

    /**
     * Rewrite MAX/MIN(boolean) since SAP HANA does not support
     * MAX/MIN on BOOLEAN types.
     *
     * MAX(boolean) -> BOOLOR_AGG
     * MIN(boolean) -> BOOLAND_AGG
     */
    public SqlNode rewriteMaxMinExpr(SqlNode aggCall, RelDataType relDataType)
    {
        if (!(aggCall instanceof SqlBasicCall)) {
            return aggCall;
        }

        SqlTypeName type = relDataType.getSqlTypeName();
        if (type != SqlTypeName.BOOLEAN) {
            return aggCall;
        }

        SqlKind kind = aggCall.getKind();
        if (kind != SqlKind.MAX && kind != SqlKind.MIN) {
            return aggCall;
        }

        boolean isMax = kind == SqlKind.MAX;
        SqlOperator operator =
                isMax
                        ? SqlLibraryOperators.BOOLOR_AGG
                        : SqlLibraryOperators.BOOLAND_AGG;

        SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
        return operator.createCall(SqlParserPos.ZERO, operand);
    }

    // ----------------------------------------------------------------------
    // LIMIT / OFFSET
    // ----------------------------------------------------------------------

    @Override
    public void unparseOffsetFetch(
            SqlWriter writer,
            @Nullable SqlNode offset,
            @Nullable SqlNode fetch)
    {
        // SAP HANA uses: LIMIT <fetch> OFFSET <offset>
        unparseFetchUsingLimit(writer, offset, fetch);
    }

    // ----------------------------------------------------------------------
    // Feature support
    // ----------------------------------------------------------------------

    @Override
    public boolean supportsApproxCountDistinct()
    {
        // SAP HANA does not support approximate distinct aggregates
        return false;
    }
}
