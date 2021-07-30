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

import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.DelegatingDeserializer;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerializer;
import com.amazonaws.athena.connector.lambda.serde.PingResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.google.common.collect.ImmutableSet;

public final class FederationResponseSerDe
{
    private FederationResponseSerDe(){}

    public static final class Serializer extends DelegatingSerializer<FederationResponse>
    {
        public Serializer(
                PingResponseSerDe.Serializer pingSerializer,
                ListSchemasResponseSerDe.Serializer listSchemasSerializer,
                ListTablesResponseSerDe.Serializer listTablesSerializer,
                GetTableResponseSerDe.Serializer getTableSerializer,
                GetTableLayoutResponseSerDe.Serializer getTableLayoutSerializer,
                GetSplitsResponseSerDe.Serializer getSplitsSerializer,
                ReadRecordsResponseSerDe.Serializer readRecordsSerializer,
                RemoteReadRecordsResponseSerDe.Serializer remoteReadRecordsSerializer,
                UserDefinedFunctionResponseSerDe.Serializer userDefinedFunctionSerializer)
        {
            super(FederationResponse.class, ImmutableSet.<TypedSerializer<FederationResponse>>builder()
                    .add(pingSerializer)
                    .add(listSchemasSerializer)
                    .add(listTablesSerializer)
                    .add(getTableSerializer)
                    .add(getTableLayoutSerializer)
                    .add(getSplitsSerializer)
                    .add(readRecordsSerializer)
                    .add(remoteReadRecordsSerializer)
                    .add(userDefinedFunctionSerializer)
                    .build());
        }
    }

    public static final class Deserializer extends DelegatingDeserializer<FederationResponse>
    {
        public Deserializer(
                PingResponseSerDe.Deserializer pingDeserializer,
                ListSchemasResponseSerDe.Deserializer listSchemasDeserializer,
                ListTablesResponseSerDe.Deserializer listTablesDeserializer,
                GetTableResponseSerDe.Deserializer getTableDeserializer,
                GetTableLayoutResponseSerDe.Deserializer getTableLayoutDeserializer,
                GetSplitsResponseSerDe.Deserializer getSplitsDeserializer,
                ReadRecordsResponseSerDe.Deserializer readRecordsDeserializer,
                RemoteReadRecordsResponseSerDe.Deserializer remoteReadRecordsDeserializer,
                UserDefinedFunctionResponseSerDe.Deserializer userDefinedFunctionDeserializer)
        {
            super(FederationResponse.class, ImmutableSet.<TypedDeserializer<FederationResponse>>builder()
                    .add(pingDeserializer)
                    .add(listSchemasDeserializer)
                    .add(listTablesDeserializer)
                    .add(getTableDeserializer)
                    .add(getTableLayoutDeserializer)
                    .add(getSplitsDeserializer)
                    .add(readRecordsDeserializer)
                    .add(remoteReadRecordsDeserializer)
                    .add(userDefinedFunctionDeserializer)
                    .build());
        }
    }
}
