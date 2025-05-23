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

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.DelegatingDeserializer;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerializer;
import com.amazonaws.athena.connector.lambda.serde.PingRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetSplitsRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetTableLayoutRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetTableRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ListSchemasRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ListTablesRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ReadRecordsRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.UserDefinedFunctionRequestSerDe;
import com.google.common.collect.ImmutableSet;

public class FederationRequestSerDeV4
{
    private FederationRequestSerDeV4() {}

    public static final class Serializer extends DelegatingSerializer<FederationRequest> implements VersionedSerDe.Serializer<FederationRequest>
    {
        public Serializer(
                PingRequestSerDe.Serializer pingSerializer,
                ListSchemasRequestSerDe.Serializer listSchemasSerializer,
                ListTablesRequestSerDe.Serializer listTablesSerializer,
                GetTableRequestSerDe.Serializer getTableSerializer,
                GetTableLayoutRequestSerDe.Serializer getTableLayoutSerializer,
                GetSplitsRequestSerDe.Serializer getSplitsSerializer,
                ReadRecordsRequestSerDe.Serializer readRecordsSerializer,
                UserDefinedFunctionRequestSerDe.Serializer userDefinedFunctionSerializer,
                GetDataSourceCapabilitiesRequestSerDeV4.Serializer getDataSourceCapabilitiesSerializer)
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
                    .add(getDataSourceCapabilitiesSerializer)
                    .build());
        }
    }

    public static final class Deserializer extends DelegatingDeserializer<FederationRequest> implements VersionedSerDe.Deserializer<FederationRequest>
    {
        public Deserializer(
                PingRequestSerDe.Deserializer pingDeserializer,
                ListSchemasRequestSerDe.Deserializer listSchemasDeserializer,
                ListTablesRequestSerDe.Deserializer listTablesDeserializer,
                GetTableRequestSerDe.Deserializer getTableDeserializer,
                GetTableLayoutRequestSerDe.Deserializer getTableLayoutDeserializer,
                GetSplitsRequestSerDe.Deserializer getSplitsDeserializer,
                ReadRecordsRequestSerDe.Deserializer readRecordsDeserializer,
                UserDefinedFunctionRequestSerDe.Deserializer userDefinedFunctionDeserializer,
                GetDataSourceCapabilitiesRequestSerDeV4.Deserializer getDataSourceCapabilitiesDeserializer)
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
                    .add(getDataSourceCapabilitiesDeserializer)
                    .build());
        }
    }
}
