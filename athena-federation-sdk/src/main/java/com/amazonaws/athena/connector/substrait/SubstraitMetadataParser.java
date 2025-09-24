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

import com.amazonaws.athena.connector.substrait.model.SubstraitField;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for parsing Substrait metadata and Handles schema extraction from Substrait protocol buffers.
 */
public class SubstraitMetadataParser
{
    private SubstraitMetadataParser()
    {
    }

    public static List<String> getTableColumns(SubstraitRelModel substraitRelModel)
    {
        List<String> baseSchema = substraitRelModel.getReadRel().getBaseSchema().getNamesList();
        List<SubstraitField> connectorSchema = new ArrayList<>();
        parseSchemaFields(baseSchema, substraitRelModel.getReadRel().getBaseSchema().getStruct().getTypesList(),
                connectorSchema, 0);
        return connectorSchema.stream()
                .map(SubstraitField::getName)
                .collect(Collectors.toList());
    }

    private static int parseSchemaFields(List<String> names,
                                         List<Type> types,
                                         List<SubstraitField> columns,
                                         int nameIdx)
    {
        int typeIdx = 0;
        while (typeIdx < types.size()) {
            String name = names.get(nameIdx);
            Type type = types.get(typeIdx);

            if (type.hasStruct()) {
                List<Type> structTypes = type.getStruct().getTypesList();

                List<SubstraitField> children = new ArrayList<>();
                nameIdx = parseSchemaFields(names, structTypes, children, nameIdx + 1);
                columns.add(new SubstraitField(name, "struct", children));
            }
            else {
                String dataType = convertSubstraitTypeToAthena(type);
                columns.add(new SubstraitField(name, dataType, null));
                nameIdx += 1;
            }
            typeIdx += 1;
        }
        return nameIdx;
    }

    private static String convertSubstraitTypeToAthena(Type type)
    {
        return type.getKindCase().name();
    }
}
