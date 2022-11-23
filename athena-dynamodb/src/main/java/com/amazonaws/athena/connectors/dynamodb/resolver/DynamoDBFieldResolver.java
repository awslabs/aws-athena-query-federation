/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.dynamodb.resolver;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DynamoDBFieldResolver
        implements FieldResolver
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBFieldResolver.class);

    private final DDBRecordMetadata metadata;

    public DynamoDBFieldResolver(DDBRecordMetadata recordMetadata)
    {
        metadata = recordMetadata;
    }

    /**
     * Return the field value from a complex structure or list.
     * @param field is the field that we would like to extract from the provided value.
     * @param originalValue is the original value object.
     * @return the field's value as a List for a LIST field type, a Map for a STRUCT field type, or the actual
     * value if neither of the above.
     * @throws IllegalArgumentException if originalValue is not an instance of Map.
     * @throws RuntimeException if the fieldName does not exist in originalValue, or if the fieldType is a STRUCT and
     * the fieldValue is not instance of Map.
     */
    @Override
    public Object getFieldValue(Field field, Object originalValue)
            throws RuntimeException
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        String fieldName = field.getName();
        Object fieldValue = originalValue;

        if (originalValue instanceof Map) {
            if (((Map) originalValue).containsKey(fieldName)) {
                fieldValue = ((Map) originalValue).get(fieldName);
            }
            else {
                // Ignore columns that do not exist in the DB record.
                return null;
            }
        }

        if (fieldValue == null) {
            return null;
        }

        switch (fieldType) {
            case LIST:
                return DDBTypeUtils.coerceListToExpectedType(fieldValue, field, metadata);
            case STRUCT:
                if (fieldValue instanceof Map) {
                    // Both fieldType and fieldValue are nested structures => return as map.
                    return fieldValue;
                }
                break;
            default:
                return DDBTypeUtils.coerceValueToExpectedType(fieldValue, field, fieldType, metadata);
        }

        throw new RuntimeException("Invalid field value encountered in DB record for field: " + field +
                ",value: " + fieldValue);
    }

    // Return the field value of a map key
    // For DynamoDB, the key is always a string so we can return the originalValue
    @Override
    public Object getMapKey(Field field, Object originalValue)
    {
        return originalValue;
    }

    // Return the field value of a map value
    @Override
    public Object getMapValue(Field field, Object originalValue)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        return DDBTypeUtils.coerceValueToExpectedType(originalValue, field, fieldType, metadata);
    }
}
