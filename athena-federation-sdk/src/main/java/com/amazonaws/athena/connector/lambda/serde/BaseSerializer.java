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
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public abstract class BaseSerializer<T> extends StdSerializer<T> implements VersionedSerDe.Serializer<T>
{
    static final String TYPE_FIELD = "@type";

    protected BaseSerializer(Class<T> clazz)
    {
        super(clazz);
    }

    @Override
    public void serialize(T value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException
    {
        if (value != null) {
            jgen.writeStartObject();
            doSerialize(value, jgen, provider);
            jgen.writeEndObject();
        }
        else {
            jgen.writeNull();
        }
    }

    @Override
    public void serializeWithType(T value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
            throws IOException
    {
        // TODO leverage TypeSerializer if it simplifies things
        serialize(value, gen, serializers);
    }

    public abstract void doSerialize(T value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException;

    /**
     * Helper used to help serialize a list of strings.
     *
     * @param jgen The json generator to use.
     * @param fieldName The name to associated to the resulting json array.
     * @param values The values to populate the array with.
     * @throws IOException If an error occurs while writing to the generator.
     */
    protected void writeStringArray(JsonGenerator jgen, String fieldName, Collection<String> values)
            throws IOException
    {
        jgen.writeArrayFieldStart(fieldName);

        for (String nextElement : values) {
            jgen.writeString(nextElement);
        }

        jgen.writeEndArray();
    }

    /**
     * Helper used to help serialize a list of strings.
     *
     * @param jgen The json generator to use.
     * @param fieldName The name to associated to the resulting json array.
     * @param values The values to populate the array with.
     * @throws IOException If an error occurs while writing to the generator.
     */
    protected void writeStringMap(JsonGenerator jgen, String fieldName, Map<String, String> values) throws IOException
    {
        jgen.writeObjectFieldStart(fieldName);

        for (Map.Entry<String, String> entry : values.entrySet()) {
            jgen.writeStringField(entry.getKey(), entry.getValue());
        }

        jgen.writeEndObject();
    }

    /**
     * Helper used to write the type of the object (usually the class name).
     *
     * @param jgen The JSON generator.
     * @throws IOException If there is an error generating the JSON.
     */
    void writeType(JsonGenerator jgen, Class<?> type)
            throws IOException
    {
        jgen.writeStringField(TYPE_FIELD, type.getSimpleName());
    }
}
