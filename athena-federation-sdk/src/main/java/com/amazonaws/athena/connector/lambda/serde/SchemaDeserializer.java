package com.amazonaws.athena.connector.lambda.serde;

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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Used to enhance Jackson's stock ObjectMapper with the ability to deserialize Apache Arrow Schema objects.
 *
 * @deprecated {@link com.amazonaws.athena.connector.lambda.serde.v3.SchemaSerDeV3} should be used instead
 */
@Deprecated
public class SchemaDeserializer
        extends StdDeserializer<Schema>
{
    private final com.amazonaws.athena.connector.lambda.data.SchemaSerDe serDe = new com.amazonaws.athena.connector.lambda.data.SchemaSerDe();

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
