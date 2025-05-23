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
package com.amazonaws.athena.connector.lambda.serde.v4;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ValueSetSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class ConstraintsSerDeV4
{
    private static final String SUMMARY_FIELD = "summary";
    private static final String EXPRESSION_FIELD = "expression";
    private static final String ORDER_BY_CLAUSE = "orderByClause";
    private static final String LIMIT_FIELD = "limit";

    private ConstraintsSerDeV4() {}

    public static final class Serializer extends BaseSerializer<Constraints> implements VersionedSerDe.Serializer<Constraints>
    {
        private final ValueSetSerDe.Serializer valueSetSerializer;
        private final VersionedSerDe.Serializer<FederationExpression> federationExpressionSerializer;
        private final VersionedSerDe.Serializer<OrderByField> orderByFieldSerializer;
        public Serializer(ValueSetSerDe.Serializer valueSetSerializer,
                          VersionedSerDe.Serializer<FederationExpression> federationExpressionSerializer,
                          VersionedSerDe.Serializer<OrderByField> orderByFieldSerializer)
        {
            super(Constraints.class);
            this.valueSetSerializer = requireNonNull(valueSetSerializer, "valueSetSerDe is null");
            this.federationExpressionSerializer = requireNonNull(federationExpressionSerializer, "federationExpressionSerDe is null");
            this.orderByFieldSerializer = requireNonNull(orderByFieldSerializer, "orderByFieldSerDe is null");
        }

        @Override
        public void doSerialize(Constraints constraints, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeObjectFieldStart(SUMMARY_FIELD);
            for (Map.Entry<String, ValueSet> entry : constraints.getSummary().entrySet()) {
                jgen.writeFieldName(entry.getKey());
                valueSetSerializer.serialize(entry.getValue(), jgen, provider);
            }
            jgen.writeEndObject();

            jgen.writeArrayFieldStart(EXPRESSION_FIELD);
            for (FederationExpression federationExpression : constraints.getExpression()) {
                federationExpressionSerializer.serialize(federationExpression, jgen, provider);
            }
            jgen.writeEndArray();

            jgen.writeArrayFieldStart(ORDER_BY_CLAUSE);
            for (OrderByField orderByField : constraints.getOrderByClause()) {
                orderByFieldSerializer.serialize(orderByField, jgen, provider);
            }
            jgen.writeEndArray();

            jgen.writeNumberField(LIMIT_FIELD, constraints.getLimit());
        }
    }

    public static final class Deserializer extends BaseDeserializer<Constraints> implements VersionedSerDe.Deserializer<Constraints>
    {
        private final ValueSetSerDe.Deserializer valueSetDeserializer;
        private final VersionedSerDe.Deserializer<FederationExpression> federationExpressionDeserializer;
        private final VersionedSerDe.Deserializer<OrderByField> orderByFieldDeserializer;

        public Deserializer(ValueSetSerDe.Deserializer valueSetDeserializer,
                            VersionedSerDe.Deserializer<FederationExpression> federationExpressionDeserializer,
                            VersionedSerDe.Deserializer<OrderByField> orderByFieldDeserializer)
        {
            super(Constraints.class);
            this.valueSetDeserializer = requireNonNull(valueSetDeserializer, "valueSetSerDe is null");
            this.federationExpressionDeserializer = requireNonNull(federationExpressionDeserializer, "federationExpressionSerDe is null");
            this.orderByFieldDeserializer = requireNonNull(orderByFieldDeserializer, "orderByFieldSerDe is null");
        }

        @Override
        public Constraints doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, SUMMARY_FIELD);
            validateObjectStart(jparser.nextToken());
            ImmutableMap.Builder<String, ValueSet> summaryMap = ImmutableMap.builder();
            while (jparser.nextToken() != JsonToken.END_OBJECT) {
                String column = jparser.getCurrentName();
                summaryMap.put(column, valueSetDeserializer.deserialize(jparser, ctxt));
            }

            assertFieldName(jparser, EXPRESSION_FIELD);
            validateArrayStart(jparser);
            ImmutableList.Builder<FederationExpression> federationExpression = ImmutableList.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                validateObjectStart(jparser.getCurrentToken());
                federationExpression.add(federationExpressionDeserializer.doDeserialize(jparser, ctxt));
                validateObjectEnd(jparser);
            }

            assertFieldName(jparser, ORDER_BY_CLAUSE);
            validateArrayStart(jparser);
            ImmutableList.Builder<OrderByField> orderByClauseBuilder = ImmutableList.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                validateObjectStart(jparser.getCurrentToken());
                orderByClauseBuilder.add(orderByFieldDeserializer.doDeserialize(jparser, ctxt));
                validateObjectEnd(jparser);
            }
            

            long limit = getNextLongField(jparser, LIMIT_FIELD);

            return new Constraints(summaryMap.build(), federationExpression.build(), orderByClauseBuilder.build(), limit);
        }
    }
}
