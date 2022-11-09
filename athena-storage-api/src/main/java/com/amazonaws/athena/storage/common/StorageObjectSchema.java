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

import java.util.List;

public class StorageObjectSchema
{
    private List<StorageObjectField> fields;
    private Object baseSchema;

    public StorageObjectSchema(List<StorageObjectField> fields, Object baseSchema)
    {
        this.fields = fields;
        this.baseSchema = baseSchema;
    }

    public List<StorageObjectField> getFields()
    {
        return fields;
    }

    public void setFields(List<StorageObjectField> fields)
    {
        this.fields = fields;
    }

    public Object getBaseSchema()
    {
        return baseSchema;
    }

    public void setBaseSchema(Object baseSchema)
    {
        this.baseSchema = baseSchema;
    }

    @Override
    public String toString()
    {
        return "StorageObjectSchema{" +
                "fields=" + fields +
                ", baseSchema=" + baseSchema +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private List<StorageObjectField> fields;
        private Object baseSchema;

        public Builder fields(final List<StorageObjectField> fields)
        {
            this.fields = fields;
            return this;
        }

        public Builder baseSchema(Object baseSchema)
        {
            this.baseSchema = baseSchema;
            return this;
        }

        public StorageObjectSchema build()
        {
            return new StorageObjectSchema(this.fields, this.baseSchema);
        }
    }
}
