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
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class DelegatingSerializer<T> extends BaseSerializer<T>
{
    private final Map<String, TypedSerializer<T>> delegateSerDeMap;

    public DelegatingSerializer(Class<T> clazz, Set<TypedSerializer<T>> serDes)
    {
        super(clazz);
        ImmutableMap.Builder<String, TypedSerializer<T>> delegateSerDeMapBuilder = ImmutableMap.builder();
        for (TypedSerializer<T> serDe : serDes) {
            delegateSerDeMapBuilder.put(serDe.getSubType().getSimpleName(), serDe);
        }
        delegateSerDeMap = delegateSerDeMapBuilder.build();
    }

    @Override
    public void doSerialize(T value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException
    {
        String type = value.getClass().getSimpleName();
        TypedSerializer<T> delegateSerDe = delegateSerDeMap.get(type);
        if (delegateSerDe != null) {
            delegateSerDe.doSerialize(value, jgen, provider);
        }
        else {
            throw new IllegalStateException("No SerDe configured for " + type);
        }
    }

    public Map<String, TypedSerializer<T>> getDelegateSerDeMap()
    {
        return delegateSerDeMap;
    }
}
