package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.SchemaSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SchemaSerializer
        extends StdSerializer<Schema>
{
    public static final String SCHEMA_FIELD_NAME = "schema";
    private final SchemaSerDe serDe = new SchemaSerDe();

    public SchemaSerializer()
    {
        super(Schema.class);
    }

    @Override
    public void serialize(Schema schema, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        jsonGenerator.writeStartObject();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serDe.serialize(schema, out);
        jsonGenerator.writeBinaryField(SCHEMA_FIELD_NAME, out.toByteArray());
        jsonGenerator.writeEndObject();
    }
}
