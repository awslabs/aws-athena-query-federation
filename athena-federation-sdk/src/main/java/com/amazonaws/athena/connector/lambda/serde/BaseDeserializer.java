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
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.serde.BaseSerializer.TYPE_FIELD;

public abstract class BaseDeserializer<T> extends StdDeserializer<T> implements VersionedSerDe.Deserializer<T>
{
    protected BaseDeserializer(Class<T> clazz)
    {
        super(clazz);
    }

    @Override
    public T deserialize(JsonParser jparser, DeserializationContext ctxt)
            throws IOException
    {
        if (jparser.nextToken() != JsonToken.VALUE_NULL) {
            validateObjectStart(jparser.getCurrentToken());
            T result = doDeserialize(jparser, ctxt);
            validateObjectEnd(jparser);
            return result;
        }
        else {
            return null;
        }
    }

    @Override
    public Object deserializeWithType(JsonParser jp, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
            throws IOException
    {
        // TODO leverage TypeDeserializer if it simplifies things
        return deserialize(jp, ctxt);
    }

    public abstract T doDeserialize(JsonParser jparser, DeserializationContext ctxt)
            throws IOException;

    /**
     * Helper used to extract named String fields from the json parser in a streaming fashion.
     *
     * @param jparser The parser to use for extraction.
     * @param expectedFieldName The expected name of the next field in the stream.
     * @return The string representation of the requested field.
     * @throws IOException If there is an error parsing the field.
     */
    protected String getNextStringField(JsonParser jparser, String expectedFieldName)
            throws IOException
    {
        assertFieldName(jparser, expectedFieldName);

        //move to the value token
        jparser.nextToken();
        return jparser.getValueAsString();
    }

    /**
     * Helper used to extract named boolean fields from the json parser in a streaming fashion.
     *
     * @param jparser The parser to use for extraction.
     * @param expectedFieldName The expected name of the next field in the stream.
     * @return The boolean representation of the requested field.
     * @throws IOException If there is an error parsing the field.
     */
    protected boolean getNextBoolField(final JsonParser jparser, final String expectedFieldName)
            throws IOException
    {
        assertFieldName(jparser, expectedFieldName);

        //move to the value token
        jparser.nextToken();
        return jparser.getValueAsBoolean();
    }

    /**
     * Helper used to extract named byte array fields from the json parser in a streaming fashion.
     *
     * @param jparser The parser to use for extraction.
     * @param expectedFieldName The expected name of the next field in the stream.
     * @return The byte array representation of the requested field.
     * @throws IOException If there is an error parsing the field.
     */
    protected byte[] getNextBinaryField(final JsonParser jparser, final String expectedFieldName)
            throws IOException
    {
        assertFieldName(jparser, expectedFieldName);

        //move to the value token
        jparser.nextToken();
        return jparser.getBinaryValue();
    }

    /**
     * Helper used to extract named integer fields from the json parser in a streaming fashion.
     *
     * @param jparser The parser to use for extraction.
     * @param expectedFieldName The expected name of the next field in the stream.
     * @return The integer representation of the requested field.
     * @throws IOException If there is an error parsing the field.
     */
    protected int getNextIntField(JsonParser jparser, String expectedFieldName)
            throws IOException
    {
        assertFieldName(jparser, expectedFieldName);

        //move to the value token
        jparser.nextToken();
        return jparser.getValueAsInt();
    }

    /**
     * Helper used to help serialize a list of strings.
     *
     * @param jgen The json generator to use.
     * @param fieldname The name to associated to the resulting json array.
     * @param values The values to populate the array with.
     * @throws IOException If an error occurs while writing to the generator.
     */
    protected void writeStringArray(JsonGenerator jgen, String fieldname, Collection<String> values)
            throws IOException
    {
        jgen.writeArrayFieldStart(fieldname);

        for (String nextElement : values) {
            jgen.writeString(nextElement);
        }

        jgen.writeEndArray();
    }

    /**
     * Helper used to read a list of strings.
     *
     * @param jparser The parser to read from.
     * @param expectedFieldName The expected name of the field containing the string array.
     * @return The contents of the array as a list.
     * @throws IOException If an error occurs while reading from the parser.
     */
    protected List<String> getNextStringArray(JsonParser jparser, String expectedFieldName)
            throws IOException
    {
        assertFieldName(jparser, expectedFieldName);

        validateArrayStart(jparser);

        List<String> result = new ArrayList<>();
        while (jparser.nextToken() != JsonToken.END_ARRAY) {
            result.add(jparser.getValueAsString());
        }

        return result;
    }

    /**
     * Helper used to read a Map of strings to strings.
     *
     * @param jparser The parser to read from.
     * @param expectedFieldName The expected name of the field containing the string map.
     * @return The contents of the array as a map.
     * @throws IOException If an error occurs while reading from the parser.
     */
    protected Map<String, String> getNextStringMap(JsonParser jparser, String expectedFieldName)
            throws IOException
    {
        assertFieldName(jparser, expectedFieldName);

        validateObjectStart(jparser);

        Map<String, String> result = new HashMap<>();
        while (jparser.nextToken() != JsonToken.END_OBJECT) {
            result.put(jparser.getCurrentName(), jparser.getValueAsString());
        }

        return result;
    }

    /**
     * Helper used to validate an expected field name.
     *
     * @param jparser The parser to read from.
     * @param expectedFieldName The expected name of the next field.
     * @throws IOException If an error occurs while reading from the parser.
     */
    protected void assertFieldName(JsonParser jparser, String expectedFieldName)
            throws IOException
    {
        jparser.nextToken();
        if (jparser.getCurrentToken() != JsonToken.FIELD_NAME) {
            throw new IllegalStateException("Expected field name token but got " + jparser.getCurrentToken());
        }
        String fieldNameToken = jparser.getCurrentName();
        if (expectedFieldName != null && !expectedFieldName.equals(fieldNameToken)) {
            throw new IllegalStateException("Unexpected field[" + fieldNameToken + "] while expecting[" + expectedFieldName + "]");
        }
    }

    /**
     * Helper used to validate that an object is starting (open brace) at the provided {@link JsonToken}.
     *
     * @param token The token expected to be an object start.
     * @throws IOException If an error occurs while reading from the parser.
     */
    protected void validateObjectStart(JsonToken token)
            throws IOException
    {
        if (!JsonToken.START_OBJECT.equals(token)) {
            throw new IllegalStateException("Expected " + JsonToken.START_OBJECT + " found " + token.asString());
        }
    }

    /**
     * Helper used to validate that an object is ending (close brace) at the next token.
     *
     * @param jparser The parser to read from.
     * @throws IOException If an error occurs while reading from the parser.
     */
    protected void validateObjectEnd(JsonParser jparser)
            throws IOException
    {
        if (!JsonToken.END_OBJECT.equals(jparser.nextToken())) {
            throw new IllegalStateException("Expected " + JsonToken.END_OBJECT + " found " + jparser.getText());
        }
    }

    /**
     * Helper used to validate that an array is starting (open bracket) at the next token.
     *
     * @param jparser The parser to read from.
     * @throws IOException If an error occurs while reading from the parser.
     */
    protected void validateArrayStart(JsonParser jparser)
            throws IOException
    {
        if (!JsonToken.START_ARRAY.equals(jparser.nextToken())) {
            throw new IllegalStateException("Expected " + JsonToken.START_ARRAY + " found " + jparser.getText());
        }
    }

    /**
     * Helper used to validate that an object is starting (open brace) at the next token.
     *
     * @param jparser The parser to read from.
     * @throws IOException If an error occurs while reading from the parser.
     */
    protected void validateObjectStart(JsonParser jparser)
            throws IOException
    {
        if (!JsonToken.START_OBJECT.equals(jparser.nextToken())) {
            throw new IllegalStateException("Expected " + JsonToken.START_OBJECT + " found " + jparser.getText());
        }
    }

    /**
     * Helper used to parse the type of the object (usually the class name).
     *
     * @param jparser The parser to use for extraction.
     * @return The type name of the object being deserialized.
     * @throws IOException If there is an error parsing the field.
     */
    protected String getType(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, TYPE_FIELD);
        jparser.nextToken();
        return jparser.getValueAsString();
    }

    /**
     * Helper used to skip to the end of the current object.  Useful for forwards compatibility.
     *
     * @param jparser The parser to use for extraction.
     * @throws IOException If there is an error parsing.
     */
    protected void ignoreRestOfObject(JsonParser jparser)
            throws IOException
    {
        if (jparser.getCurrentToken() == JsonToken.END_OBJECT) {
            return;
        }

        int open = 1;
        /* Since proper matching of start/end markers is handled
         * by nextToken(), we'll just count nesting levels here
         */
        while (true) {
            JsonToken t = jparser.nextToken();
            if (t == null) {
                throw new IllegalStateException("Expected " + JsonToken.END_OBJECT + " found " + jparser.getText());
            }
            if (t.isStructStart()) {
                ++open;
            }
            else if (t == JsonToken.END_OBJECT) {
                if (--open == 0) {
                    return;
                }
            }
        }
    }
}
