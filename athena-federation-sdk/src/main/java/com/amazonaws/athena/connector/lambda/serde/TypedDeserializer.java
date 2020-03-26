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

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public abstract class TypedDeserializer<T> extends BaseDeserializer<T>
{
    private Class<? extends T> subType;

    protected TypedDeserializer(Class<T> superType, Class<? extends T> subType)
    {
        super(superType);
        this.subType = requireNonNull(subType, "subType is null");
    }

    public Class<? extends T> getSubType()
    {
        return subType;
    }

    @Override
    public Object deserializeWithType(JsonParser jp, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
            throws IOException
    {
        return doDeserialize(jp, ctxt);
    }

    @Override
    protected T doDeserialize(JsonParser jparser, DeserializationContext ctxt)
            throws IOException
    {
        // parse the type before delegating to implementation
        getType(jparser);
        return doTypedDeserialize(jparser, ctxt);
    }

    protected abstract T doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
            throws IOException;
}
