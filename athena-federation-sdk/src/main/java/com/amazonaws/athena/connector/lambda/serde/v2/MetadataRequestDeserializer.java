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
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;

import java.io.IOException;

import static com.amazonaws.athena.connector.lambda.serde.v2.MetadataRequestSerializer.CATALOG_NAME_FIELD;
import static com.amazonaws.athena.connector.lambda.serde.v2.MetadataRequestSerializer.IDENTITY_FIELD;
import static com.amazonaws.athena.connector.lambda.serde.v2.MetadataRequestSerializer.QUERY_ID_FIELD;
import static java.util.Objects.requireNonNull;

public abstract class MetadataRequestDeserializer
        extends TypedDeserializer<FederationRequest>
{
    private final FederatedIdentitySerDe.Deserializer identityDeserializer;

    protected MetadataRequestDeserializer(Class<? extends FederationRequest> subType, FederatedIdentitySerDe.Deserializer identityDeserializer)
    {
        super(FederationRequest.class, subType);
        this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
    }

    protected FederationRequest doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
            throws IOException
    {
        assertFieldName(jparser, IDENTITY_FIELD);
        FederatedIdentity identity = identityDeserializer.deserialize(jparser, ctxt);

        String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        return doRequestDeserialize(jparser, ctxt, identity, queryId, catalogName);
    }

    protected abstract FederationRequest doRequestDeserialize(JsonParser jparser, DeserializationContext ctxt, FederatedIdentity identity, String queryId, String catalogName)
            throws IOException;
}
