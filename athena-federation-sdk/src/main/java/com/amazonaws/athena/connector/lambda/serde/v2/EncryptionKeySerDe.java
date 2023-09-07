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

import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public final class EncryptionKeySerDe
{
    private static final String KEY_FIELD = "key";
    private static final String NONCE_FIELD = "nonce";

    private EncryptionKeySerDe() {}

    public static final class Serializer extends BaseSerializer<EncryptionKey>
    {
        public Serializer()
        {
            super(EncryptionKey.class);
        }

        @Override
        public void doSerialize(EncryptionKey encryptionKey, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeBinaryField(KEY_FIELD, encryptionKey.getKey());
            jgen.writeBinaryField(NONCE_FIELD, encryptionKey.getNonce());
        }
    }

    public static final class Deserializer extends BaseDeserializer<EncryptionKey>
    {
        public Deserializer()
        {
            super(EncryptionKey.class);
        }

        @Override
        public EncryptionKey doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            byte[] key = getNextBinaryField(jparser, KEY_FIELD);
            byte [] nonce = getNextBinaryField(jparser, NONCE_FIELD);

            return new EncryptionKey(key, nonce);
        }
    }
}
