/*-
 * #%L
 * Athena MSK Connector
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connectors.msk.dto.TopicSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

public abstract class MskAbstractDeserializerTest
{
    protected static final ObjectMapper objectMapper = new ObjectMapper();

    protected Schema createSchema(TopicSchema topicSchema) throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        topicSchema.getMessage().getFields().forEach(it -> {
            FieldType fieldType = new FieldType(
                    true,
                    AmazonMskUtils.toArrowType(it.getType()),
                    null,
                    Map.of(
                            "mapping", it.getMapping(),
                            "formatHint", it.getFormatHint(),
                            "type", it.getType()
                    )
            );
            Field field = new Field(it.getName(), fieldType, null);
            schemaBuilder.addField(field);
        });

        schemaBuilder.addMetadata("dataFormat", topicSchema.getMessage().getDataFormat());
        return schemaBuilder.build();
    }

    protected Schema createSchemaForException(TopicSchema topicSchema) throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        topicSchema.getMessage().getFields().forEach(it -> {
            FieldType fieldType = new FieldType(
                    true,
                    AmazonMskUtils.toArrowType(it.getType()),
                    null,
                    Map.of(
                            "formatHint", it.getFormatHint(),
                            "type", it.getType()
                    )
            );
            Field field = new Field(it.getName(), fieldType, null);
            schemaBuilder.addField(field);
        });

        schemaBuilder.addMetadata("dataFormat", topicSchema.getMessage().getDataFormat());
        return schemaBuilder.build();
    }
}
