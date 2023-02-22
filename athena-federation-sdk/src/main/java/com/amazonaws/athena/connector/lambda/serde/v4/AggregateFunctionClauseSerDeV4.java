/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde.v4;

import com.amazonaws.athena.connector.lambda.domain.predicate.aggregation.AggregateFunctionClause;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AggregateFunctionClauseSerDeV4
{
    private static final String AGGREGATE_FUNCTIONS_FIELD = "aggregateFunctions";
    private static final String GROUPING_SETS_FIELD = "groupingSets";

    private AggregateFunctionClauseSerDeV4() {}

    public static final class Serializer extends BaseSerializer<AggregateFunctionClause> implements VersionedSerDe.Serializer<AggregateFunctionClause>
    {
        private final VersionedSerDe.Serializer<FederationExpression> federationExpressionSerializer;

        public Serializer(VersionedSerDe.Serializer<FederationExpression> federationExpressionSerializer)
        {
            super(AggregateFunctionClause.class);
            this.federationExpressionSerializer = requireNonNull(federationExpressionSerializer, "functionNameSerializer is null");
        }

        @Override
        public void doSerialize(AggregateFunctionClause aggregateFunctionClause, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeObjectFieldStart(AGGREGATE_FUNCTIONS_FIELD);
            for (Map.Entry<String, FederationExpression> expression : aggregateFunctionClause.getAggregateFunctions().entrySet()) {
                jgen.writeFieldName(expression.getKey());
                federationExpressionSerializer.serialize(expression.getValue(), jgen, provider);
            }
            jgen.writeEndObject();

            jgen.writeArrayFieldStart(GROUPING_SETS_FIELD);
            for (List<String> groups : aggregateFunctionClause.getGroupingSets()) {
                jgen.writeStartArray();
                for (String group : groups) {
                    jgen.writeString(group);
                }
                jgen.writeEndArray();
            }
            jgen.writeEndArray();
        }
    }

    public static final class Deserializer extends BaseDeserializer<AggregateFunctionClause> implements VersionedSerDe.Deserializer<AggregateFunctionClause>
    {
        private VersionedSerDe.Deserializer<FederationExpression> federationExpressionDeserializer;

        public Deserializer(VersionedSerDe.Deserializer<FederationExpression> federationExpressionDeserializer)
        {
            super(AggregateFunctionClause.class);
            this.federationExpressionDeserializer = requireNonNull(federationExpressionDeserializer, "functionNameSerializer is null");
        }

        @Override
        public AggregateFunctionClause deserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            // Schema should be deserialized inline and not unwrapped
            return doDeserialize(jparser, ctxt);
        }

        @Override
        public AggregateFunctionClause doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, AGGREGATE_FUNCTIONS_FIELD);
            validateObjectStart(jparser.nextToken());
            ImmutableMap.Builder<String, FederationExpression> federationExpressionBuilder = ImmutableMap.builder();
            while (jparser.nextToken() != JsonToken.END_OBJECT) {
                String column = jparser.getCurrentName();
                federationExpressionBuilder.put(column, federationExpressionDeserializer.deserialize(jparser, ctxt));
            }

            assertFieldName(jparser, GROUPING_SETS_FIELD);
            validateArrayStart(jparser);
            ImmutableList.Builder<List<String>> groupingSetsBuilder = ImmutableList.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                List<String> group = new ArrayList<>();
                while (jparser.nextToken() != JsonToken.END_ARRAY) {
                    group.add(jparser.getValueAsString());
                }
                groupingSetsBuilder.add(group);
            }

            return new AggregateFunctionClause(federationExpressionBuilder.build(), groupingSetsBuilder.build());
        }
    }
}
