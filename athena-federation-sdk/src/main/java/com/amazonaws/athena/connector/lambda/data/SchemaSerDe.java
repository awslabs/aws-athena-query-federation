package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

/**
 * Used to serialize and deserialize Apache Arrow Schema objects.
 *
 * @deprecated {@link com.amazonaws.athena.connector.lambda.serde.v3.SchemaSerDeV3} should be used instead
 */
@Deprecated
public class SchemaSerDe
{
    /**
     * Serialized the provided Schema to the provided OutputStream.
     *
     * @param schema The Schema to serialize.
     * @param out The OutputStream to write to.
     * @throws IOException
     */
    public void serialize(Schema schema, OutputStream out)
            throws IOException
    {
        IpcOption option = new IpcOption(true, MetadataVersion.V4);
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema, option);
    }

    /**
     * Attempts to deserialize a Schema from the provided InputStream.
     *
     * @param in The InputStream that is expected to contain a serialized Schema.
     * @return The resulting Schema if the InputStream contains a valid Schema.
     * @throws IOException
     * @note This method does _not_ close the input stream and also reads the InputStream to the end.
     */
    public Schema deserialize(InputStream in)
            throws IOException
    {
        return MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)));
    }
}
