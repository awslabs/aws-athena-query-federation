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
package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Since this SerDe is used in {@link PingRequestSerDe} it needs to be forwards compatible.
 */
public final class FederatedIdentitySerDe
{
    private static final String ID_FIELD = "id";
    private static final String COMPATIBILITY_ID = "UNKNOWN";
    private static final String PRINCIPAL_FIELD = "principal";
    private static final String COMPATIBILITY_PRINCIPAL = "UNKNOWN";
    private static final String ACCOUNT_FIELD = "account";
    private static final String ARN_FIELD = "arn";
    private static final String TAGS_FIELD = "tags";
    private static final String GROUPS_FIELD = "groups";
    private static final String CONFIG_OPTIONS_FIELD = "configOptions";
    // new fields should only be appended to the end for forwards compatibility

    private FederatedIdentitySerDe() {}

    public static final class Serializer extends BaseSerializer<FederatedIdentity>
    {
        public Serializer()
        {
            super(FederatedIdentity.class);
        }

        @Override
        public void doSerialize(FederatedIdentity federatedIdentity, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeStringField(ID_FIELD, COMPATIBILITY_ID);
            jgen.writeStringField(PRINCIPAL_FIELD, COMPATIBILITY_PRINCIPAL);
            jgen.writeStringField(ACCOUNT_FIELD, federatedIdentity.getAccount());
            jgen.writeStringField(ARN_FIELD, federatedIdentity.getArn());
            writeStringMap(jgen, TAGS_FIELD, federatedIdentity.getPrincipalTags());
            writeStringArray(jgen, GROUPS_FIELD, federatedIdentity.getIamGroups());
            writeStringMap(jgen, CONFIG_OPTIONS_FIELD, federatedIdentity.getConfigOptions());
            // new fields should only be appended to the end for backwards and forwards compatibility
        }
    }

    public static final class Deserializer extends BaseDeserializer<FederatedIdentity>
    {
        public Deserializer()
        {
            super(FederatedIdentity.class);
        }

        @Override
        public FederatedIdentity deserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            if (jparser.nextToken() != JsonToken.VALUE_NULL) {
                validateObjectStart(jparser.getCurrentToken());
                FederatedIdentity federatedIdentity = doDeserialize(jparser, ctxt);
                return federatedIdentity;
            }

            return null;
        }

        @Override
        public FederatedIdentity doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            // First, read fields that we no longer care about.
            getNextStringField(jparser, ID_FIELD);
            getNextStringField(jparser, PRINCIPAL_FIELD);

            String account = getNextStringField(jparser, ACCOUNT_FIELD);
            String arn = getNextStringField(jparser, ARN_FIELD);
            Map<String, String> principalTags = getNextStringMap(jparser, TAGS_FIELD);
            List<String> groups = getNextStringArray(jparser, GROUPS_FIELD);

            // Handle configOptions field for backwards compatibility
            Map<String, String> configOptions = new HashMap<>();
            JsonToken nextToken = jparser.nextToken();
            if (nextToken == JsonToken.FIELD_NAME && CONFIG_OPTIONS_FIELD.equals(jparser.currentName())) {
                // configOptions field exists, read it manually
                validateObjectStart(jparser);
                while (jparser.nextToken() != JsonToken.END_OBJECT) {
                    configOptions.put(jparser.currentName(), jparser.getValueAsString());
                }
                // Consume END_OBJECT for identity object
                if (jparser.nextToken() != JsonToken.END_OBJECT) {
                    throw new IllegalStateException("Expected END_OBJECT after configOptions field");
                }
            }
            else if (nextToken != JsonToken.END_OBJECT) {
                throw new IllegalStateException("Expected END_OBJECT or configOptions field, got: " + nextToken);
            }

            return new FederatedIdentity(arn, account, principalTags, groups, configOptions);
        }
    }
}
