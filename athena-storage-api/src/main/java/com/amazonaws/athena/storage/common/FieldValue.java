/*-
 * #%L
 * athena-storage-api
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.storage.common;

import java.util.Optional;

public class FieldValue
{
    private final String field;
    private final String value;

    public FieldValue(String field, String value)
    {
        this.field = field;
        this.value = value;
    }

    public static Optional<FieldValue> from(String fieldValue)
    {
        String[] fieldValuePair = fieldValue.split("=");
        if (fieldValuePair.length == 2) {
            return Optional.of(new FieldValue(fieldValuePair[0], fieldValuePair[1]));
        }
        return Optional.empty();
    }

    public String getField()
    {
        return field;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return "FieldValue{" +
                "field=" + field +
                ", value=" + value +
                '}';
    }
}
