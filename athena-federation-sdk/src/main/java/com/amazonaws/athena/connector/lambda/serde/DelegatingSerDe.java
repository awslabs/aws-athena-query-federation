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
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DelegatingSerDe<T> extends BaseSerDe<T>
{
    // TODO construct this map from a set of provided SerDes that self-describe the type (class) name
    private final Map<String, TypedSerDe<T>> delegateSerDeMap;

    public DelegatingSerDe(Map<String, TypedSerDe<T>> delegateSerDeMap)
    {
        this.delegateSerDeMap = requireNonNull(delegateSerDeMap, "delegateSerDeMap is null");
    }

    @Override
    public void serialize(JsonGenerator jgen, T object)
            throws IOException
    {
        // we delegate the wrapping/enclosing to the delegate
        doSerialize(jgen, object);
    }

    @Override
    public void doSerialize(JsonGenerator jgen, T object)
            throws IOException
    {
        String type = object.getClass().getSimpleName();
        BaseSerDe<T> delegateSerDe = delegateSerDeMap.get(type);
        if (delegateSerDe != null) {
            delegateSerDe.serialize(jgen, object);
        }
        else {
            throw new IllegalStateException("No SerDe configured for " + type);
        }
    }

    @Override
    public T doDeserialize(JsonParser jparser)
            throws IOException
    {
        String type = getType(jparser);
        BaseSerDe<T> delegateSerDe = delegateSerDeMap.get(type);
        if (delegateSerDe != null) {
            return delegateSerDe.doDeserialize(jparser);
        }
        else {
            throw new IllegalStateException("No SerDe configured for " + type);
        }
    }
}
