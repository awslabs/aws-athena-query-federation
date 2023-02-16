/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class OrderByFieldSerDeV4
{
    private static final String COLUMN_NAME_FIELD = "columnName";
    private static final String DIRECTION_FIELD = "direction";

    private OrderByFieldSerDeV4() {}

    public static final class Serializer extends BaseSerializer<OrderByField> implements VersionedSerDe.Serializer<OrderByField>
    {
        public Serializer()
        {
            super(OrderByField.class);
        }

        @Override
        public void doSerialize(OrderByField orderByField, JsonGenerator jgen, SerializerProvider provider)
                throws IOException 
        {
            jgen.writeStringField(COLUMN_NAME_FIELD, orderByField.getColumnName());
            jgen.writeStringField(DIRECTION_FIELD, orderByField.getDirection().name());
        }
    }

    public static final class Deserializer extends BaseDeserializer<OrderByField> implements VersionedSerDe.Deserializer<OrderByField>
    {
        public Deserializer()
        {
            super(OrderByField.class);
        }

        @Override
        public OrderByField doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String columnName = getNextStringField(jparser, COLUMN_NAME_FIELD);
            String direction = getNextStringField(jparser, DIRECTION_FIELD);
            OrderByField.Direction directionEnum;
            switch (direction) {
                case "ASC_NULLS_FIRST":
                    directionEnum = OrderByField.Direction.ASC_NULLS_FIRST;
                    break;
                case "ASC_NULLS_LAST":
                    directionEnum = OrderByField.Direction.ASC_NULLS_LAST;
                    break;
                case "DESC_NULLS_FIRST":
                    directionEnum = OrderByField.Direction.DESC_NULLS_FIRST;
                    break;
                case "DESC_NULLS_LAST":
                    directionEnum = OrderByField.Direction.DESC_NULLS_LAST;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + direction);
            }
            return new OrderByField(columnName, directionEnum);
        }
    }
    
}
