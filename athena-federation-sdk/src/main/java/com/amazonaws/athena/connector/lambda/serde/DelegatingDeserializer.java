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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class DelegatingDeserializer<T> extends BaseDeserializer<T>
{
    private final Map<String, TypedDeserializer<T>> delegateSerDeMap;

    public DelegatingDeserializer(Class<T> clazz, Set<TypedDeserializer<T>> serDes)
    {
        super(clazz);
        ImmutableMap.Builder<String, TypedDeserializer<T>> delegateSerDeMapBuilder = ImmutableMap.builder();
        for (TypedDeserializer<T> serDe : serDes) {
            delegateSerDeMapBuilder.put(serDe.getSubType().getSimpleName(), serDe);
        }
        delegateSerDeMap = delegateSerDeMapBuilder.build();
    }

    @Override
    public Object deserializeWithType(JsonParser jp, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
            throws IOException
    {
        return doDeserialize(jp, ctxt);
    }

    @Override
    public T doDeserialize(JsonParser jparser, DeserializationContext ctxt)
            throws IOException
    {
        String type = getType(jparser);
        TypedDeserializer<T> delegateSerDe = delegateSerDeMap.get(type);
        if (delegateSerDe != null) {
            return delegateSerDe.doTypedDeserialize(jparser, ctxt);
        }
        else {
            throw new IllegalStateException("No SerDe configured for " + type);
        }
    }

    public Map<String, TypedDeserializer<T>> getDelegateSerDeMap()
    {
        return delegateSerDeMap;
    }
}
