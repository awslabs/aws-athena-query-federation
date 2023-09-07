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
package com.amazonaws.athena.connector.lambda.serde.v4;

import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

public final class SchemaSerDeV4 implements VersionedSerDe
{
    SchemaSerDeV4() {}

    public static final class Serializer extends BaseSerializer<Schema> implements VersionedSerDe.Serializer<Schema>
    {
        public Serializer()
        {
            super(Schema.class);
        }

        @Override
        public void serialize(Schema schema, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            // Schema is serialized inline and not wrapped
            doSerialize(schema, jgen, provider);
        }

        @Override
        public void doSerialize(Schema schema, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
            jgen.writeBinary(out.toByteArray());
        }
    }

    public static final class Deserializer extends BaseDeserializer<Schema>
    {
        public Deserializer()
        {
            super(Schema.class);
        }

        @Override
        public Schema deserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            // Schema should be deserialized inline and not unwrapped
            return doDeserialize(jparser, ctxt);
        }

        @Override
        public Schema doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            if (!JsonToken.VALUE_STRING.equals(jparser.nextToken())) {
                throw new IllegalStateException("Expected " + JsonToken.VALUE_STRING + " found " + jparser.getText());
            }
            byte[] schemaBytes = jparser.getBinaryValue();
            ByteArrayInputStream in = new ByteArrayInputStream(schemaBytes);
            return MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)));
        }
    }
}
