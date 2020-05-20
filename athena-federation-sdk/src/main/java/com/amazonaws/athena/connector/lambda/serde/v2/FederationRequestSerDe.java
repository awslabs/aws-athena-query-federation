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

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.DelegatingDeserializer;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerializer;
import com.amazonaws.athena.connector.lambda.serde.PingRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.google.common.collect.ImmutableSet;

final class FederationRequestSerDe
{
    private FederationRequestSerDe(){}

    static final class Serializer extends DelegatingSerializer<FederationRequest>
    {
        Serializer(
                PingRequestSerDe.Serializer pingSerializer,
                ListSchemasRequestSerDe.Serializer listSchemasSerializer,
                ListTablesRequestSerDe.Serializer listTablesSerializer,
                GetTableRequestSerDe.Serializer getTableSerializer,
                GetTableLayoutRequestSerDe.Serializer getTableLayoutSerializer,
                GetSplitsRequestSerDe.Serializer getSplitsSerializer,
                ReadRecordsRequestSerDe.Serializer readRecordsSerializer,
                UserDefinedFunctionRequestSerDe.Serializer userDefinedFunctionSerializer)
        {
            super(FederationRequest.class, ImmutableSet.<TypedSerializer<FederationRequest>>builder()
                    .add(pingSerializer)
                    .add(listSchemasSerializer)
                    .add(listTablesSerializer)
                    .add(getTableSerializer)
                    .add(getTableLayoutSerializer)
                    .add(getSplitsSerializer)
                    .add(readRecordsSerializer)
                    .add(userDefinedFunctionSerializer)
                    .build());
        }
    }

    static final class Deserializer extends DelegatingDeserializer<FederationRequest>
    {
        Deserializer(
                PingRequestSerDe.Deserializer pingDeserializer,
                ListSchemasRequestSerDe.Deserializer listSchemasDeserializer,
                ListTablesRequestSerDe.Deserializer listTablesDeserializer,
                GetTableRequestSerDe.Deserializer getTableDeserializer,
                GetTableLayoutRequestSerDe.Deserializer getTableLayoutDeserializer,
                GetSplitsRequestSerDe.Deserializer getSplitsDeserializer,
                ReadRecordsRequestSerDe.Deserializer readRecordsDeserializer,
                UserDefinedFunctionRequestSerDe.Deserializer userDefinedFunctionDeserializer)
        {
            super(FederationRequest.class, ImmutableSet.<TypedDeserializer<FederationRequest>>builder()
                    .add(pingDeserializer)
                    .add(listSchemasDeserializer)
                    .add(listTablesDeserializer)
                    .add(getTableDeserializer)
                    .add(getTableLayoutDeserializer)
                    .add(getSplitsDeserializer)
                    .add(readRecordsDeserializer)
                    .add(userDefinedFunctionDeserializer)
                    .build());
        }
    }
}
