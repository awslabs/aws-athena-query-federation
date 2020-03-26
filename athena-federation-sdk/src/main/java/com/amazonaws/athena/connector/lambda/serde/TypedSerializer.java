/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public abstract class TypedSerializer<T> extends BaseSerializer<T>
{
    private Class<? extends T> subType;

    protected TypedSerializer(Class<T> superType, Class<? extends T> subType)
    {
        super(superType);
        this.subType = requireNonNull(subType, "subType is null");
    }

    public Class<? extends T> getSubType()
    {
        return subType;
    }

    @Override
    protected void doSerialize(T value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException
    {
        writeType(jgen, subType);
        doTypedSerialize(value, jgen, provider);
    }

    protected abstract void doTypedSerialize(T value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException;
}
