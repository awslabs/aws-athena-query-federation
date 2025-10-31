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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;

import java.util.List;
import java.util.Map;

public class SubstraitAccumulatorVisitor extends SqlShuttle
{
    private List<SubstraitTypeAndValue> accumulator;
    private Map<String, String> splitProperties;
    private final Schema schema;
    private String currentColumn;

    public SubstraitAccumulatorVisitor(List<SubstraitTypeAndValue> accumulator, Map<String, String> splitProperties, Schema schema)
    {
        this.accumulator = accumulator;
        this.splitProperties = splitProperties;
        this.schema = schema;
    }

    @Override
    public SqlNode visit(SqlIdentifier id)
    {
        if (id.isSimple()) {
            currentColumn = id.getSimple();
        }
        return super.visit(id);
    }

    @Override
    public SqlNode visit(SqlLiteral literal)
    {
        if (currentColumn == null) {
            throw new RuntimeException("Cannot determine column for literal: " + literal);
        }
        Field arrowField = schema.findField(currentColumn);
        if (arrowField == null) {
            throw new RuntimeException("Column not found in schema: " + currentColumn);
        }
        SqlTypeName typeName = mapArrowTypeToSqlTypeName(arrowField.getType());
        if (literal.getValue() instanceof NlsString) {
            accumulator.add(new SubstraitTypeAndValue(typeName, ((NlsString) literal.getValue()).getValue(), currentColumn));
        }
        else {
            accumulator.add(new SubstraitTypeAndValue(typeName, literal.getValue(), currentColumn));
        }
        return new SqlDynamicParam(0, literal.getParserPosition());
    }

    private SqlTypeName mapArrowTypeToSqlTypeName(ArrowType arrowType)
    {
        if (arrowType instanceof ArrowType.Int) {
            int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
            if (bitWidth <= 32) {
                return SqlTypeName.INTEGER;
            }
            return SqlTypeName.BIGINT;
        }
        else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) arrowType;
            if (fp.getPrecision() == FloatingPointPrecision.SINGLE) {
                return SqlTypeName.FLOAT;
            }
            return SqlTypeName.DOUBLE;
        }
        else if (arrowType instanceof ArrowType.Null) {
            return SqlTypeName.NULL;
        }
        else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
            return SqlTypeName.VARCHAR;
        }
        else if (arrowType instanceof ArrowType.Bool) {
            return SqlTypeName.BOOLEAN;
        }
        else if (arrowType instanceof ArrowType.Decimal) {
            return SqlTypeName.DECIMAL;
        }
        else if (arrowType instanceof ArrowType.Date) {
            return SqlTypeName.DATE;
        }
        else if (arrowType instanceof ArrowType.Time) {
            return SqlTypeName.TIME;
        }
        else if (arrowType instanceof ArrowType.Timestamp) {
            return SqlTypeName.TIMESTAMP;
        }
        else if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.LargeBinary) {
            return SqlTypeName.VARBINARY;
        }
        else {
            return SqlTypeName.VARCHAR;
        }
    }
}
