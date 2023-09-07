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

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ArrowTypeSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class FunctionCallExpressionSerDeV4
{
    private static final String FUNCTION_NAME_FIELD = "functionName";
    private static final String TYPE_FIELD = "type";
    private static final String ARGUMENTS_FIELD = "arguments";

    private FunctionCallExpressionSerDeV4() {}

    public static final class Serializer extends TypedSerializer<FederationExpression> implements VersionedSerDe.Serializer<FederationExpression>
    {
        private final VersionedSerDe.Serializer<FunctionName> functionNameSerializer;
        private final ArrowTypeSerDe.Serializer arrowTypeSerializer;

        private VersionedSerDe.Serializer<FederationExpression> federationExpressionSerializer;

        public Serializer(VersionedSerDe.Serializer<FunctionName> functionNameSerializer,
                          ArrowTypeSerDe.Serializer arrowTypeSerializer)
        {
            super(FederationExpression.class, FunctionCallExpression.class);
            this.functionNameSerializer = requireNonNull(functionNameSerializer, "functionNameSerializer is null");
            this.arrowTypeSerializer = requireNonNull(arrowTypeSerializer, "arrowTypeSerializer is null");
        }

        public void setFederationExpressionSerializer(VersionedSerDe.Serializer<FederationExpression> federationExpressionSerializer)
        {
            this.federationExpressionSerializer = federationExpressionSerializer;
        }

        @Override
        protected void doTypedSerialize(FederationExpression federationExpression, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            FunctionCallExpression functionCallExpression = (FunctionCallExpression) federationExpression;

            jgen.writeFieldName(TYPE_FIELD);
            arrowTypeSerializer.serialize(functionCallExpression.getType(), jgen, provider);

            jgen.writeFieldName(FUNCTION_NAME_FIELD);
            functionNameSerializer.serialize(functionCallExpression.getFunctionName(), jgen, provider);

            jgen.writeArrayFieldStart(ARGUMENTS_FIELD);
            for (FederationExpression expression : functionCallExpression.getArguments()) {
                federationExpressionSerializer.serialize(expression, jgen, provider);
            }
            jgen.writeEndArray();
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationExpression> implements VersionedSerDe.Deserializer<FederationExpression>
    {
        private final VersionedSerDe.Deserializer<FunctionName> functionNameDeserializer;
        private final ArrowTypeSerDe.Deserializer arrowTypeDeserializer;

        private VersionedSerDe.Deserializer<FederationExpression> federationExpressionDeserializer;

        public Deserializer(VersionedSerDe.Deserializer<FunctionName> functionNameDeserializer,
                            ArrowTypeSerDe.Deserializer arrowTypeDeserializer)
        {
            super(FederationExpression.class, FunctionCallExpression.class);
            this.functionNameDeserializer = requireNonNull(functionNameDeserializer, "functionNameSerializer is null");
            this.arrowTypeDeserializer = requireNonNull(arrowTypeDeserializer, "arrowTypeSerializer is null");
        }

        public void setFederationExpressionSerializer(VersionedSerDe.Deserializer<FederationExpression> federationExpressionDeserializer)
        {
            this.federationExpressionDeserializer = federationExpressionDeserializer;
        }

        @Override
        protected FunctionCallExpression doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, TYPE_FIELD);
            ArrowType type = arrowTypeDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, FUNCTION_NAME_FIELD);
            FunctionName functionName = functionNameDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, ARGUMENTS_FIELD);
            validateArrayStart(jparser);
            ImmutableList.Builder<FederationExpression> federationExpressionList = ImmutableList.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                validateObjectStart(jparser.getCurrentToken());
                federationExpressionList.add(federationExpressionDeserializer.doDeserialize(jparser, ctxt));
                validateObjectEnd(jparser);
            }

            return new FunctionCallExpression(type, functionName, federationExpressionList.build());
        }
    }
}
