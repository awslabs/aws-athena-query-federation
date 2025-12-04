/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.Rel;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class CustomSubstraitToCalcite extends SubstraitToCalcite {
    
    private final String tableName;
    private final Schema tableSchema;
    
    public CustomSubstraitToCalcite(SimpleExtension.ExtensionCollection extensions,
                                    RelDataTypeFactory typeFactory,
                                    TypeConverter typeConverter,
                                    String tableName,
                                    Schema tableSchema) {
        super(extensions, typeFactory, typeConverter);
        this.tableName = tableName;
        this.tableSchema = tableSchema;
    }
    
    @Override
    protected CalciteSchema toSchema(Rel rel) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        
        rootSchema.add(tableName, new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                RelDataTypeFactory.Builder builder = factory.builder();
                
                for (Field field : tableSchema.getFields()) {
                    SqlTypeName sqlType = mapArrowTypeToSqlType(field.getType());
                    builder.add(field.getName(), factory.createSqlType(sqlType));
                }
                
                return builder.build();
            }
        });
        
        return rootSchema;
    }
    
    private SqlTypeName mapArrowTypeToSqlType(org.apache.arrow.vector.types.pojo.ArrowType arrowType) {
        if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int) {
            return SqlTypeName.INTEGER;
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
            return SqlTypeName.DOUBLE;
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
            return SqlTypeName.BOOLEAN;
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Date) {
            return SqlTypeName.DATE;
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
            return SqlTypeName.TIMESTAMP;
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Decimal) {
            return SqlTypeName.DECIMAL;
        } else {
            // Default to VARCHAR for string types and unknown types
            return SqlTypeName.VARCHAR;
        }
    }
}
