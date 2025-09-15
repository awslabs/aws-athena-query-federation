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

import com.amazonaws.athena.connectors.jdbc.manager.SubstraitTypeAndValue;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;

import java.util.List;
import java.util.Map;

public class SubstraitAccumulatorVisitor extends SqlShuttle
{
    private List<SubstraitTypeAndValue> accumulator;
    private Map<String, String> splitProperties;

    public SubstraitAccumulatorVisitor(List<SubstraitTypeAndValue> accumulator, Map<String, String> splitProperties)
    {
        this.accumulator = accumulator;
        this.splitProperties = splitProperties;
    }

    @Override
    public SqlNode visit(SqlLiteral literal)
    {
        if (literal.getValue() instanceof NlsString) {
            accumulator.add(new SubstraitTypeAndValue(literal.getTypeName(), ((NlsString) literal.getValue()).getValue()));
        }
        else {
            accumulator.add(new SubstraitTypeAndValue(literal.getTypeName(), literal.getValue()));
        }
        return new SqlDynamicParam(0, literal.getParserPosition());
    }
}
