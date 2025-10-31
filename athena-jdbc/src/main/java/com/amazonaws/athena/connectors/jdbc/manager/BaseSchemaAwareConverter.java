/*-
 * #%L
 * Amazon Athena JDBC Connector
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
package com.amazonaws.athena.connectors.jdbc.manager;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
public class BaseSchemaAwareConverter
{
    private BaseSchemaAwareConverter() {}
    public static AbstractTable makeCalciteTableFromBaseSchema(final NamedStruct schema)
    {
        return new AbstractTable()
        {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                List<String> colNames = schema.getNamesList();
                List<RelDataType> colTypes = new ArrayList<>();

                for (Type t : schema.getStruct().getTypesList()) {
                    colTypes.add(substraitTypeToCalcite(typeFactory, t));
                }

                return typeFactory.createStructType(colTypes, colNames);
            }
        };
    }

    private static RelDataType substraitTypeToCalcite(RelDataTypeFactory factory, Type t) 
    {
        switch (t.getKindCase()) {
            case I32:
                return factory.createSqlType(SqlTypeName.INTEGER);
            case I64:
                return factory.createSqlType(SqlTypeName.BIGINT);
            case VARCHAR:
                return factory.createSqlType(SqlTypeName.VARCHAR, t.getVarchar().getLength());
            default:
                throw new UnsupportedOperationException("Unsupported type: " + t.getKindCase());
        }
    }
}
