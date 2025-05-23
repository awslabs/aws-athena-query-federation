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
import com.amazonaws.athena.connector.lambda.serde.DelegatingDeserializer;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.google.common.collect.ImmutableSet;

public class FederationExpressionSerDeV4
{
    private FederationExpressionSerDeV4() {}

    public static final class Serializer extends DelegatingSerializer<FederationExpression> implements VersionedSerDe.Serializer<FederationExpression>
    {
        public Serializer(
                ConstantExpressionSerDeV4.Serializer constantExpressionSerializer,
                FunctionCallExpressionSerDeV4.Serializer functionCallExpressionSerializer,
                VariableExpressionSerDeV4.Serializer variableExpressionSerializer)
        {
            super(FederationExpression.class, ImmutableSet.of(
                    constantExpressionSerializer,
                    functionCallExpressionSerializer,
                    variableExpressionSerializer));
        }
    }

    public static final class Deserializer extends DelegatingDeserializer<FederationExpression> implements VersionedSerDe.Deserializer<FederationExpression>
    {
        public Deserializer(
                ConstantExpressionSerDeV4.Deserializer constantExpressionDeserializer,
                FunctionCallExpressionSerDeV4.Deserializer functionCallExpressionDeserializer,
                VariableExpressionSerDeV4.Deserializer variableExpressionDeserializer)
        {
            super(FederationExpression.class, ImmutableSet.of(
                    constantExpressionDeserializer,
                    functionCallExpressionDeserializer,
                    variableExpressionDeserializer));
        }
    }
}
