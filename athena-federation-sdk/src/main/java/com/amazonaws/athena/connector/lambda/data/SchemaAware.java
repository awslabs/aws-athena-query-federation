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

/**
 * Defines a component that is aware of Apache Arrow Schema.
 */
public abstract class SchemaAware
{
    /**
     * Provides access to the Schema object.
     *
     * @return The Schema currently being used by this object.
     */
    protected abstract Schema internalGetSchema();

    /**
     * Provides access to the Fields on the Schema currently being used by this Object.
     *
     * @return The list of fields.
     */
    public List<Field> getFields()
    {
        return internalGetSchema().getFields();
    }

    /**
     * Provides access to metadata stored on the Schema currently being used by this Object.
     *
     * @param key The metadata key to lookup.
     * @return The value associated with that key in the Schema's metadata, null if no such key exists.
     */
    public String getMetaData(String key)
    {
        return internalGetSchema().getCustomMetadata().get(key);
    }

    /**
     * Provides access to all avaialable metadata on the Schema.
     *
     * @return All metadata key-value pairs as a map.
     */
    public Map<String, String> getMetaData()
    {
        return internalGetSchema().getCustomMetadata();
    }
}
