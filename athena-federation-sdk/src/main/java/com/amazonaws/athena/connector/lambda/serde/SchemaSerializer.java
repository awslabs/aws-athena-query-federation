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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Used to enhance Jackson's stock ObjectMapper with the ability to serialize Apache Arrow Schema objects.
 *
 * @deprecated {@link com.amazonaws.athena.connector.lambda.serde.v3.SchemaSerDeV3} should be used instead
 */
@Deprecated
public class SchemaSerializer
        extends StdSerializer<Schema>
{
    public static final String SCHEMA_FIELD_NAME = "schema";
    private final com.amazonaws.athena.connector.lambda.data.SchemaSerDe serDe = new com.amazonaws.athena.connector.lambda.data.SchemaSerDe();

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
