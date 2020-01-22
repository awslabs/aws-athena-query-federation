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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.serde.BaseSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Verify;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

public class SchemaSerDe extends BaseSerDe<Schema>
{
    @Override
    public void serialize(JsonGenerator jgen, Schema schema)
            throws IOException
    {
        // Schema is serialized inline and not wrapped
        doSerialize(jgen, schema);
    }

    @Override
    public Schema deserialize(JsonParser jparser)
            throws IOException
    {
        // Schema should be deserialized inline and not unwrapped
        return doDeserialize(jparser);
    }

    @Override
    public void doSerialize(JsonGenerator jgen, Schema schema)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
        jgen.writeBinary(out.toByteArray());
    }

    @Override
    public Schema doDeserialize(JsonParser jparser)
            throws IOException
    {
        Verify.verify(JsonToken.VALUE_STRING.equals(jparser.nextToken()), "Expected " + JsonToken.VALUE_STRING + " found " + jparser.getText());
        byte[] schemaBytes = jparser.getBinaryValue();
        ByteArrayInputStream in = new ByteArrayInputStream(schemaBytes);
        return MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)));
    }
}
