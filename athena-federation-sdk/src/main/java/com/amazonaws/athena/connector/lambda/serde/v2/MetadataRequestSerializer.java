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

import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public abstract class MetadataRequestSerializer
        extends TypedSerializer<FederationRequest>
{
    static final String IDENTITY_FIELD = "identity";
    static final String QUERY_ID_FIELD = "queryId";
    static final String CATALOG_NAME_FIELD = "catalogName";

    private final FederatedIdentitySerDe.Serializer identitySerializer;

    protected MetadataRequestSerializer(Class<? extends FederationRequest> subType, FederatedIdentitySerDe.Serializer identitySerializer)
    {
        super(FederationRequest.class, subType);
        this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
    }

    @Override
    protected void doTypedSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
            throws IOException
    {
        MetadataRequest metadataRequest = (MetadataRequest) federationRequest;

        jgen.writeFieldName(IDENTITY_FIELD);
        identitySerializer.serialize(metadataRequest.getIdentity(), jgen, provider);

        jgen.writeStringField(QUERY_ID_FIELD, metadataRequest.getQueryId());
        jgen.writeStringField(CATALOG_NAME_FIELD, metadataRequest.getCatalogName());

        doRequestSerialize(metadataRequest, jgen, provider);
    }

    protected abstract void doRequestSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
            throws IOException;
}
