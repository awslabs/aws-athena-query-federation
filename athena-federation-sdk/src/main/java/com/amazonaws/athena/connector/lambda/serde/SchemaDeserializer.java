package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.SchemaSerDe;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class SchemaDeserializer
        extends StdDeserializer<Schema>
{
    private final SchemaSerDe serDe = new SchemaSerDe();

    public SchemaDeserializer()
    {
        super(Schema.class);
    }

    @Override
    public Schema deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException
    {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        byte[] schemaBytes = node.get(SchemaSerializer.SCHEMA_FIELD_NAME).binaryValue();
        return serDe.deserialize(new ByteArrayInputStream(schemaBytes));
    }
}
