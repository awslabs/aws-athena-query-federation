package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SchemaAware
{

    protected abstract Schema internalGetSchema();

    //TODO: This isn't very performant, perhaps we should keep a map
    public Field getField(String fieldName)
    {
        Schema schema = internalGetSchema();
        List<Field> results = schema.getFields()
                .stream().filter(next -> next.getName()
                        .equals(fieldName)).collect(Collectors.toList());

        if (results.size() != 1) {
            throw new IllegalArgumentException("fieldName is ambiguous, found:" + results.toString());
        }

        return results.get(0);
    }

    public List<Field> getFields()
    {
        return internalGetSchema().getFields();
    }

    public String getMetaData(String key)
    {
        return internalGetSchema().getCustomMetadata().get(key);
    }

    public Map<String, String> getMetaData()
    {
        return internalGetSchema().getCustomMetadata();
    }
}
