/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;

/**
 * Used to resolve Elasticsearch complex structures to Apache Arrow Types.
 * @see com.amazonaws.athena.connector.lambda.data.FieldResolver
 */
public class ElasticsearchFieldResolver
        implements FieldResolver
{
    protected static final FieldResolver DEFAULT_FIELD_RESOLVER = new ElasticsearchFieldResolver();

    private ElasticsearchFieldResolver() {}

    /**
     * Return the field value from a complex structure or list.
     * @param field is the field that we would like to extract from the provided value.
     * @param originalValue is the original value object.
     * @return the field's value as an ArrayList for a LIST field type, a HashMap for a STRUCT field type, or the actual
     * value if neither of the above.
     * @throws RuntimeException if the fieldName does not exist in originalValue, if the fieldType is a STRUCT and
     * the fieldValue is not instance of HashMap, or if the fieldType is neither a LIST or a STRUCT but the fieldValue
     * is instance of HashMap (STRUCT).
     */
    @Override
    public Object getFieldValue(Field field, Object originalValue)
            throws RuntimeException
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        String fieldName = field.getName();
        Object fieldValue;

        if (((HashMap<String, Object>) originalValue).containsKey(fieldName)) {
            fieldValue = ((HashMap<String, Object>) originalValue).get(fieldName);
        }
        else {
            throw new RuntimeException("Field not found in Document: " + fieldName);
        }

        switch (fieldType) {
            case LIST:
                return ElasticsearchHelper.coerceListField(field, fieldValue);
            case STRUCT:
                if (fieldValue instanceof HashMap) {
                    // Both fieldType and fieldValue are nested structures => return as map.
                    return fieldValue;
                }
                break;
            default:
                if (!(fieldValue instanceof HashMap)) {
                    return ElasticsearchHelper.coerceField(field, fieldValue);
                }
                break;
        }

        throw new RuntimeException("Invalid field value encountered in Document for field: " + field.toString() +
                ",value: " + fieldValue.toString());
    }
}
