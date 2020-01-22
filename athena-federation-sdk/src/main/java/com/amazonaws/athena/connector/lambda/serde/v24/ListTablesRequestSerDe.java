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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ListTablesRequestSerDe
        extends TypedSerDe<FederationRequest>
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SCHEMA_NAME_FIELD = "schemaName";

    private final FederatedIdentitySerDe federatedIdentitySerDe;

    public ListTablesRequestSerDe(FederatedIdentitySerDe federatedIdentitySerDe)
    {
        this.federatedIdentitySerDe = requireNonNull(federatedIdentitySerDe, "federatedIdentitySerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationRequest federationRequest)
            throws IOException
    {
        ListTablesRequest listTablesRequest = (ListTablesRequest) federationRequest;

        jgen.writeFieldName(IDENTITY_FIELD);
        federatedIdentitySerDe.serialize(jgen, listTablesRequest.getIdentity());

        jgen.writeStringField(QUERY_ID_FIELD, listTablesRequest.getQueryId());
        jgen.writeStringField(CATALOG_NAME_FIELD, listTablesRequest.getCatalogName());
        jgen.writeStringField(SCHEMA_NAME_FIELD, listTablesRequest.getSchemaName());
    }

    @Override
    public ListTablesRequest doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, IDENTITY_FIELD);
        FederatedIdentity federatedIdentity = federatedIdentitySerDe.deserialize(jparser);

        String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);
        String schemaName = getNextStringField(jparser, SCHEMA_NAME_FIELD);

        return new ListTablesRequest(federatedIdentity, queryId, catalogName, schemaName);
    }
}
