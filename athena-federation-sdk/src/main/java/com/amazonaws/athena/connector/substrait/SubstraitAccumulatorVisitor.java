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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SubstraitAccumulatorVisitor extends SqlShuttle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubstraitAccumulatorVisitor.class);

    private final List<SubstraitTypeAndValue> accumulator;
    private final RelDataType schema;
    private String currentColumn;

    public SubstraitAccumulatorVisitor(final List<SubstraitTypeAndValue> accumulator, final RelDataType schema)
    {
        this.accumulator = accumulator;
        this.schema = schema;
    }

    @Override
    public SqlNode visit(final SqlIdentifier id)
    {
        if (id.isSimple()) {
            currentColumn = id.getSimple();
        }
        return super.visit(id);
    }

    @Override
    public SqlNode visit(final SqlLiteral literal)
    {
        if (currentColumn == null) {
            // such as LIMIT
            LOGGER.info("literal value {} doesn't have an associated column. skipping", literal.toValue());
            return literal;
        }

        RelDataTypeField field = schema.getField(currentColumn, true, true);

        if (field == null) {
            throw new IllegalArgumentException("field " + currentColumn + " not found in schema with fields: " + schema.getFieldNames());
        }

        final SqlTypeName typeName = field.getType().getSqlTypeName();
        if (literal.getValue() instanceof NlsString) {
            accumulator.add(new SubstraitTypeAndValue(typeName, ((NlsString) literal.getValue()).getValue(), currentColumn));
        }
        else {
            accumulator.add(new SubstraitTypeAndValue(typeName, literal.getValue(), currentColumn));
        }
        return new SqlDynamicParam(0, literal.getParserPosition());
    }
}
