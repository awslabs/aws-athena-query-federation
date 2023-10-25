/*-
 * #%L
 * athena-opensearch
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
package com.amazonaws.athena.connectors.opensearch;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.Types;

public class OpensearchNestedStructBuilderTest {
    
    @Test
    public void createNestedStructTest()
    {
        // Simple 1 layer struct
        String attribute1 = "Struct1.attributeString";
        String attribute2 = "Struct1.attributeInt";

        FieldBuilder simpleStruct = OpensearchNestedStructBuilder.createNestedStruct(attribute1, null, Types.MinorType.VARCHAR.getType());
        simpleStruct = OpensearchNestedStructBuilder.createNestedStruct(attribute2, simpleStruct, Types.MinorType.INT.getType());

        FieldBuilder expectedSimpleStruct = 
            FieldBuilder.newBuilder("Struct1", Types.MinorType.STRUCT.getType());
        expectedSimpleStruct.addStringField("attributeString");
        expectedSimpleStruct.addIntField("attributeInt");

        assertEquals(expectedSimpleStruct.build(), simpleStruct.build());

        // Nested struct
        String nested_attribute1 = "Struct1.attributeDateDay";
        String nested_attribute2 = "Struct1.Struct2.attributeString1";
        String nested_attribute3 = "Struct1.Struct3.attributeInt1";
        String nested_attribute4 = "Struct1.Struct2.attributeString2";
        String nested_attribute5 = "Struct1.Struct3.attributeFloat1";

        FieldBuilder nestedStruct = OpensearchNestedStructBuilder.createNestedStruct(nested_attribute1, null, Types.MinorType.DATEDAY.getType());
        nestedStruct = OpensearchNestedStructBuilder.createNestedStruct(nested_attribute2, nestedStruct, Types.MinorType.VARCHAR.getType());
        nestedStruct = OpensearchNestedStructBuilder.createNestedStruct(nested_attribute3, nestedStruct, Types.MinorType.INT.getType());
        nestedStruct = OpensearchNestedStructBuilder.createNestedStruct(nested_attribute4, nestedStruct, Types.MinorType.VARCHAR.getType());
        nestedStruct = OpensearchNestedStructBuilder.createNestedStruct(nested_attribute5, nestedStruct, Types.MinorType.FLOAT8.getType());

        FieldBuilder struct2 = 
            FieldBuilder.newBuilder("Struct2", Types.MinorType.STRUCT.getType());
            struct2.addStringField("attributeString1");
            struct2.addStringField("attributeString2");
        FieldBuilder struct3 = 
            FieldBuilder.newBuilder("Struct3", Types.MinorType.STRUCT.getType());
            struct3.addIntField("attributeInt1");
            struct3.addFloat8Field("attributeFloat1");
        Field attr5 = new Field("attributeDateDay", FieldType.nullable(Types.MinorType.DATEDAY.getType()), null);
        Field expectedNestedStruct = new Field("Struct1", FieldType.nullable(Types.MinorType.STRUCT.getType()), new ArrayList<>(Arrays.asList(attr5, struct2.build(), struct3.build())));
        
        assertEquals(expectedNestedStruct, nestedStruct.build());
    }
}
