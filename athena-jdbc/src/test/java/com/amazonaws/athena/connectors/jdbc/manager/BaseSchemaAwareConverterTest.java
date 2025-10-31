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
package com.amazonaws.athena.connectors.jdbc.manager;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseSchemaAwareConverterTest
{
    @Test
    public void testMakeCalciteTableFromBaseSchemaWithIntegerColumn()
    {
        // Create mock NamedStruct with integer column
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("id"),
            Arrays.asList(createMockI32Type())
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test the row type
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType intType = mock(RelDataType.class);
        RelDataType structType = mock(RelDataType.class);

        when(typeFactory.createSqlType(SqlTypeName.INTEGER)).thenReturn(intType);
        when(typeFactory.createStructType(Arrays.asList(intType), Arrays.asList("id"))).thenReturn(structType);

        RelDataType rowType = table.getRowType(typeFactory);
        assertEquals(structType, rowType);
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithBigIntColumn()
    {
        // Create mock NamedStruct with bigint column
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("big_id"),
            Arrays.asList(createMockI64Type())
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test the row type
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType bigintType = mock(RelDataType.class);
        RelDataType structType = mock(RelDataType.class);

        when(typeFactory.createSqlType(SqlTypeName.BIGINT)).thenReturn(bigintType);
        when(typeFactory.createStructType(Arrays.asList(bigintType), Arrays.asList("big_id"))).thenReturn(structType);

        RelDataType rowType = table.getRowType(typeFactory);
        assertEquals(structType, rowType);
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithVarcharColumn()
    {
        // Create mock NamedStruct with varchar column
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("name"),
            Arrays.asList(createMockVarcharType(255))
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test the row type
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType varcharType = mock(RelDataType.class);
        RelDataType structType = mock(RelDataType.class);

        when(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255)).thenReturn(varcharType);
        when(typeFactory.createStructType(Arrays.asList(varcharType), Arrays.asList("name"))).thenReturn(structType);

        RelDataType rowType = table.getRowType(typeFactory);
        assertEquals(structType, rowType);
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithMultipleColumns()
    {
        // Create mock NamedStruct with multiple columns
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("id", "name", "age"),
            Arrays.asList(
                createMockI32Type(),
                createMockVarcharType(100),
                createMockI64Type()
            )
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test the row type
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType intType = mock(RelDataType.class);
        RelDataType varcharType = mock(RelDataType.class);
        RelDataType bigintType = mock(RelDataType.class);
        RelDataType structType = mock(RelDataType.class);

        when(typeFactory.createSqlType(SqlTypeName.INTEGER)).thenReturn(intType);
        when(typeFactory.createSqlType(SqlTypeName.VARCHAR, 100)).thenReturn(varcharType);
        when(typeFactory.createSqlType(SqlTypeName.BIGINT)).thenReturn(bigintType);
        when(typeFactory.createStructType(
            Arrays.asList(intType, varcharType, bigintType),
            Arrays.asList("id", "name", "age")
        )).thenReturn(structType);

        RelDataType rowType = table.getRowType(typeFactory);
        assertEquals(structType, rowType);
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithEmptySchema()
    {
        // Create mock NamedStruct with no columns
        NamedStruct namedStruct = createMockNamedStruct(
            Collections.emptyList(),
            Collections.emptyList()
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test the row type
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType structType = mock(RelDataType.class);

        when(typeFactory.createStructType(
            Collections.emptyList(),
            Collections.emptyList()
        )).thenReturn(structType);

        RelDataType rowType = table.getRowType(typeFactory);
        assertEquals(structType, rowType);
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithUnsupportedType()
    {
        // Create mock NamedStruct with unsupported type
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("unsupported_col"),
            Arrays.asList(createMockUnsupportedType())
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test that unsupported type throws exception
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);

        assertThrows(UnsupportedOperationException.class, () -> {
            table.getRowType(typeFactory);
        });
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithVarcharZeroLength()
    {
        // Create mock NamedStruct with varchar of length 0
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("empty_varchar"),
            Arrays.asList(createMockVarcharType(0))
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test the row type
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType varcharType = mock(RelDataType.class);
        RelDataType structType = mock(RelDataType.class);

        when(typeFactory.createSqlType(SqlTypeName.VARCHAR, 0)).thenReturn(varcharType);
        when(typeFactory.createStructType(Arrays.asList(varcharType), Arrays.asList("empty_varchar"))).thenReturn(structType);

        RelDataType rowType = table.getRowType(typeFactory);
        assertEquals(structType, rowType);
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithVarcharMaxLength()
    {
        // Create mock NamedStruct with varchar of maximum length
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("max_varchar"),
            Arrays.asList(createMockVarcharType(Integer.MAX_VALUE))
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test the row type
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);
        RelDataType varcharType = mock(RelDataType.class);
        RelDataType structType = mock(RelDataType.class);

        when(typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE)).thenReturn(varcharType);
        when(typeFactory.createStructType(Arrays.asList(varcharType), Arrays.asList("max_varchar"))).thenReturn(structType);

        RelDataType rowType = table.getRowType(typeFactory);
        assertEquals(structType, rowType);
    }

    @Test
    public void testMakeCalciteTableFromBaseSchemaWithMixedTypes()
    {
        // Create mock NamedStruct with mixed supported and unsupported types
        NamedStruct namedStruct = createMockNamedStruct(
            Arrays.asList("id", "unsupported", "name"),
            Arrays.asList(
                createMockI32Type(),
                createMockUnsupportedType(),
                createMockVarcharType(50)
            )
        );

        AbstractTable table = BaseSchemaAwareConverter.makeCalciteTableFromBaseSchema(namedStruct);

        assertNotNull(table);

        // Test that unsupported type in the middle throws exception
        RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);

        assertThrows(UnsupportedOperationException.class, () -> {
            table.getRowType(typeFactory);
        });
    }

    private NamedStruct createMockNamedStruct(List<String> names, List<Type> types)
    {
        NamedStruct.Builder builder = NamedStruct.newBuilder();
        builder.addAllNames(names);

        Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
        structBuilder.addAllTypes(types);

        builder.setStruct(structBuilder.build());

        return builder.build();
    }

    private Type createMockI32Type()
    {
        return Type.newBuilder()
            .setI32(Type.I32.newBuilder().build())
            .build();
    }

    private Type createMockI64Type()
    {
        return Type.newBuilder()
            .setI64(Type.I64.newBuilder().build())
            .build();
    }

    private Type createMockVarcharType(int length)
    {
        return Type.newBuilder()
            .setVarchar(Type.VarChar.newBuilder().setLength(length).build())
            .build();
    }

    private Type createMockUnsupportedType()
    {
        // Create a type that's not supported (e.g., BOOLEAN which is not handled in the converter)
        return Type.newBuilder()
            .setBool(Type.Boolean.newBuilder().build())
            .build();
    }
}
