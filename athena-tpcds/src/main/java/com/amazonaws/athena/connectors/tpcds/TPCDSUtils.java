/*-
 * #%L
 * athena-tpcds
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.tpcds;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Optional;

/**
 * Utility class that centralizes a few commonly used tools for working with
 * the TPC-DS Tables,Columns,Schemas.
 */
public class TPCDSUtils
{
    private TPCDSUtils() {}

    /**
     * Converts from TPCDS columns to Apache Arrow fields.
     *
     * @param column The TPCDS column to conver.
     * @return The Apache Arrow field that corresponds to the TPCDS column.
     */
    public static Field convertColumn(Column column)
    {
        ColumnType type = column.getType();
        switch (type.getBase()) {
            case TIME:
            case IDENTIFIER:
                return FieldBuilder.newBuilder(column.getName(), Types.MinorType.BIGINT.getType()).build();
            case INTEGER:
                return FieldBuilder.newBuilder(column.getName(), Types.MinorType.INT.getType()).build();
            case DATE:
                return FieldBuilder.newBuilder(column.getName(), Types.MinorType.DATEDAY.getType()).build();
            case DECIMAL:
                ArrowType arrowType = new ArrowType.Decimal(type.getPrecision().get(), type.getScale().get());
                return FieldBuilder.newBuilder(column.getName(), arrowType).build();
            case CHAR:
            case VARCHAR:
                return FieldBuilder.newBuilder(column.getName(), Types.MinorType.VARCHAR.getType()).build();
        }
        throw new IllegalArgumentException("Unsupported TPC-DS type " + column.getName() + ":" + column.getType().getBase());
    }

    /**
     * Extracts the scale factor of the schema from its name.
     *
     * @param schemaName The schema name from which to extract a scale factor.
     * @return The scale factor associated with the schema name. Method throws is the scale factor can not be determined.
     */
    public static int extractScaleFactor(String schemaName)
    {
        if (!schemaName.startsWith("tpcds")) {
            throw new RuntimeException("Unknown schema format " + schemaName + ", can not extract scale factor.");
        }

        try {
            return Integer.parseInt(schemaName.substring(5));
        }
        catch (RuntimeException ex) {
            throw new RuntimeException("Unknown schema format " + schemaName + ", can not extract scale factor.", ex);
        }
    }

    /**
     * Required that the requested Table be present in the TPCDS generated schema.
     *
     * @param tableName The fully qualified name of the requested table.
     * @return The TPCDS table, if present, otherwise the method throws.
     */
    public static Table validateTable(TableName tableName)
    {
        Optional<Table> table = Table.getBaseTables().stream()
                .filter(next -> next.getName().equals(tableName.getTableName()))
                .findFirst();

        if (!table.isPresent()) {
            throw new RuntimeException("Unknown table " + tableName);
        }

        return table.get();
    }
}
