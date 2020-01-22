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

public abstract class TypedSerDe<T> extends BaseSerDe<T>
{
    @Override
    public void serialize(JsonGenerator jgen, T object)
            throws IOException
    {
        jgen.writeStartObject();
        // write the type before delegating to implementation
        String type = object.getClass().getSimpleName();
        writeType(jgen, type);
        doSerialize(jgen, object);
        jgen.writeEndObject();
    }

    @Override
    public T deserialize(JsonParser jparser)
            throws IOException
    {
        validateObjectStart(jparser.nextToken());
        // parse the type before delegating to implementation
        getType(jparser);
        T result = doDeserialize(jparser);
        validateObjectEnd(jparser);
        return result;
    }
}
