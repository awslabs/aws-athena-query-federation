/*-
 * #%L
 * athena-opensearch
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Arrays;
import java.util.List;

public class OpensearchNestedStructBuilder
{
    private OpensearchNestedStructBuilder()
    {
    }

    public static FieldBuilder createNestedStruct(String schemaString, FieldBuilder rootField, ArrowType type) 
    {
        List<String> nestedEntry = Arrays.asList(schemaString.split("\\."));
        FieldBuilder currentField;

        if (rootField == null) {
            rootField = FieldBuilder.newBuilder(nestedEntry.get(0), Types.MinorType.STRUCT.getType());
        }

        currentField = rootField;
        for (String layer : nestedEntry.subList(1, nestedEntry.size() - 1)) {
            FieldBuilder child = currentField.getNestedChild(layer);
            if (child == null) {
                FieldBuilder childField = FieldBuilder.newBuilder(layer, Types.MinorType.STRUCT.getType());
                currentField.addFieldBuilder(layer, childField);
                currentField = childField;
            }
            else {
                currentField = child;
            }
        }
        Field newField = new Field(nestedEntry.get(nestedEntry.size() - 1), FieldType.nullable(type), null);
        currentField.addField(newField);
        return rootField;
    }
}
