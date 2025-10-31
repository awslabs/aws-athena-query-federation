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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FilterRemovalVisitor extends SqlShuttle
{
    private final Set<String> targetColumns;
    private final boolean insideOr;

    public FilterRemovalVisitor(Set<String> targetColumns)
    {
        this(targetColumns, false);
    }

    private FilterRemovalVisitor(Set<String> targetColumns, boolean insideOr)
    {
        this.targetColumns = targetColumns;
        this.insideOr = insideOr;
    }

    @Override
    public SqlNode visit(SqlCall call)
    {
        SqlKind kind = call.getKind();

        if (isDirectConditionOnTarget(call)) {
            return SqlLiteral.createBoolean(!insideOr, SqlParserPos.ZERO);
        }

        if (kind == SqlKind.AND || kind == SqlKind.OR) {
            List<SqlNode> newOperands = new ArrayList<>();
            for (SqlNode operand : call.getOperandList()) {
                SqlNode newOperand = operand.accept(new FilterRemovalVisitor(
                        targetColumns, kind == SqlKind.OR
                ));

                newOperands.add(newOperand);
            }
            return call.getOperator().createCall(SqlParserPos.ZERO, newOperands);
        }

        return super.visit(call);
    }

    private boolean isDirectConditionOnTarget(SqlCall call)
    {
        if (call.operandCount() < 1) {
            return false;
        }
        SqlNode first = call.operand(0);
        return first instanceof SqlIdentifier &&
                targetColumns.stream()
                        .anyMatch(targetColumn ->
                                targetColumn.equalsIgnoreCase(((SqlIdentifier) first).getSimple()));
    }
}
