/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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

import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SubstraitMetadataParserTest
{
    @Test
    void testGetTableColumnsWithSimpleTypes()
    {
        SubstraitRelModel model = createRelModel(
            Arrays.asList("id", "name", "age"),
            Arrays.asList(
                createIntType(),
                createStringType(),
                createIntType()
            )
        );
        
        List<String> columns = SubstraitMetadataParser.getTableColumns(model);
        
        assertEquals(3, columns.size());
        assertEquals("id", columns.get(0));
        assertEquals("name", columns.get(1));
        assertEquals("age", columns.get(2));
    }

    @Test
    void testGetTableColumnsWithStructType()
    {
        SubstraitRelModel model = createRelModel(
            Arrays.asList("id", "address", "street", "city", "name"),
            Arrays.asList(
                createIntType(),
                createStructType(Arrays.asList(createStringType(), createStringType())),
                createStringType()
            )
        );
        
        List<String> columns = SubstraitMetadataParser.getTableColumns(model);
        
        assertEquals(3, columns.size());
        assertEquals("id", columns.get(0));
        assertEquals("address", columns.get(1));
        assertEquals("name", columns.get(2));
    }

    @Test
    void testGetTableColumnsWithEmptySchema()
    {
        SubstraitRelModel model = createRelModel(
            Arrays.asList(),
            Arrays.asList()
        );
        
        List<String> columns = SubstraitMetadataParser.getTableColumns(model);
        
        assertTrue(columns.isEmpty());
    }

    @Test
    void testGetTableColumnsWithSingleColumn()
    {
        SubstraitRelModel model = createRelModel(
            Arrays.asList("single_column"),
            Arrays.asList(createStringType())
        );
        
        List<String> columns = SubstraitMetadataParser.getTableColumns(model);
        
        assertEquals(1, columns.size());
        assertEquals("single_column", columns.get(0));
    }

    private SubstraitRelModel createRelModel(List<String> names, List<Type> types)
    {
        NamedStruct namedStruct = NamedStruct.newBuilder()
                .addAllNames(names)
                .setStruct(Type.Struct.newBuilder()
                        .addAllTypes(types)
                        .build())
                .build();
        
        ReadRel readRel = ReadRel.newBuilder()
                .setBaseSchema(namedStruct)
                .build();
        
        return new SubstraitRelModel(readRel, null, null, null, null);
    }

    private Type createIntType()
    {
        return Type.newBuilder()
                .setI32(Type.I32.newBuilder().build())
                .build();
    }

    private Type createStringType()
    {
        return Type.newBuilder()
                .setString(Type.String.newBuilder().build())
                .build();
    }

    private Type createStructType(List<Type> childTypes)
    {
        return Type.newBuilder()
                .setStruct(Type.Struct.newBuilder()
                        .addAllTypes(childTypes)
                        .build())
                .build();
    }
}