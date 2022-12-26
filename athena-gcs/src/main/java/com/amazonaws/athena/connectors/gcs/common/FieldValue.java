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
package com.amazonaws.athena.connectors.gcs.common;

import java.util.Objects;
import java.util.Optional;

/**
 * Used to evaluate expression to select a partition folder based on the constraints
 * These are built using another util
 */
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
            FieldValue value = new FieldValue(fieldValuePair[0].toLowerCase(), fieldValuePair[1].replace("'", "").replace("\"", ""));
            return Optional.of(value);
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldValue)) {
            return false;
        }
        FieldValue that = (FieldValue) o;
        return getField().equals(that.getField()) && getValue().equals(that.getValue());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getField(), getValue());
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
