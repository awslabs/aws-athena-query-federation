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

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

final class FederatedIdentitySerDe
{
    private static final String ID_FIELD = "id";
    private static final String PRINCIPLE_FIELD = "principal";
    private static final String ACCOUNT_FIELD = "account";

    private FederatedIdentitySerDe(){}

    static final class Serializer extends BaseSerializer<FederatedIdentity>
    {
        Serializer()
        {
            super(FederatedIdentity.class);
        }

        @Override
        protected void doSerialize(FederatedIdentity federatedIdentity, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeStringField(ID_FIELD, federatedIdentity.getId());
            jgen.writeStringField(PRINCIPLE_FIELD, federatedIdentity.getPrincipal());
            jgen.writeStringField(ACCOUNT_FIELD, federatedIdentity.getAccount());
        }
    }

    static final class Deserializer extends BaseDeserializer<FederatedIdentity>
    {
        Deserializer()
        {
            super(FederatedIdentity.class);
        }

        @Override
        protected FederatedIdentity doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String id = getNextStringField(jparser, ID_FIELD);
            String principal = getNextStringField(jparser, PRINCIPLE_FIELD);
            String account = getNextStringField(jparser, ACCOUNT_FIELD);
            return new FederatedIdentity(id, principal, account);
        }
    }
}
