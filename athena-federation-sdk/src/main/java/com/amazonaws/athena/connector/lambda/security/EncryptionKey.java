package com.amazonaws.athena.connector.lambda.security;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

/**
 * Holder for an AES-GCM compatible encryption key and nonce.
 */
public class EncryptionKey
{
    private final byte[] key;
    private final byte[] nonce;

    @JsonCreator
    public EncryptionKey(@JsonProperty("key") byte[] key, @JsonProperty("nonce") byte[] nonce)
    {
        this.key = key;
        this.nonce = nonce;
    }

    @JsonProperty
    public byte[] getKey()
    {
        return key;
    }

    @JsonProperty
    public byte[] getNonce()
    {
        return nonce;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EncryptionKey that = (EncryptionKey) o;

        return Arrays.equals(this.key, that.key) &&
                Arrays.equals(this.nonce, that.nonce);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(key) + 31 + Arrays.hashCode(nonce);
    }
}
