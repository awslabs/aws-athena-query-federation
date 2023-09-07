/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility class for dealing with Arrow Schemas
 */
public class ArrowSchemaUtils
{
    public static Field remapArrowTypesWithinField(Field inputField, Function<ArrowType, ArrowType> arrowTypeMapper)
    {
        // Recursively transform any FieldTypes in the children Fields
        List<Field> updatedChildren = inputField.getChildren().stream()
            .map(f -> remapArrowTypesWithinField(f, arrowTypeMapper))
            .collect(Collectors.toList());
        ArrowType newArrowType = arrowTypeMapper.apply(inputField.getType());
        FieldType oldFieldType = inputField.getFieldType();
        FieldType newFieldType = new FieldType(oldFieldType.isNullable(), newArrowType, oldFieldType.getDictionary(), oldFieldType.getMetadata());
        return new Field(inputField.getName(), newFieldType, updatedChildren);
    }

    private ArrowSchemaUtils()
    {
    }
}
