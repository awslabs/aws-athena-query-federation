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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

final class TableNameSerDe
{
    private static final String SCHEMA_NAME_FIELD = "schemaName";
    private static final String TABLE_NAME_FIELD = "tableName";

    private TableNameSerDe(){}

    static final class Serializer extends BaseSerializer<TableName>
    {
        Serializer()
        {
            super(TableName.class);
        }

        @Override
        protected void doSerialize(TableName tableName, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeStringField(SCHEMA_NAME_FIELD, tableName.getSchemaName());
            jgen.writeStringField(TABLE_NAME_FIELD, tableName.getTableName());
        }
    }

    static final class Deserializer extends BaseDeserializer<TableName>
    {
        Deserializer()
        {
            super(TableName.class);
        }

        @Override
        protected TableName doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String schemaName = getNextStringField(jparser, SCHEMA_NAME_FIELD);
            String tableName = getNextStringField(jparser, TABLE_NAME_FIELD);
            return new TableName(schemaName, tableName);
        }
    }
}
