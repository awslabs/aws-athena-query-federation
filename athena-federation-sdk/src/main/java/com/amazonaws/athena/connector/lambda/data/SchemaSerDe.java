package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

public class SchemaSerDe
{
    public void serialize(Schema schema, OutputStream out)
            throws IOException
    {
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    }

    public Schema deserialize(InputStream in)
            throws IOException
    {
        return MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)));
    }
}
