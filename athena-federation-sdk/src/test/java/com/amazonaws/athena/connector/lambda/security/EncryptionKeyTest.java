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
package com.amazonaws.athena.connector.lambda.security;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class EncryptionKeyTest
{
    @Test
    public void deriveWithLabelKeepsKeyAndChangesNonce()
    {
        EncryptionKey original = new LocalKeyFactory().create();

        EncryptionKey derived = original.deriveWithLabel("schema-sidecar");

        assertArrayEquals(original.getKey(), derived.getKey());
        assertEquals(original.getNonce().length, derived.getNonce().length);
        assertFalse(java.util.Arrays.equals(original.getNonce(), derived.getNonce()));
        assertNotEquals(original, derived);
    }

    @Test
    public void deriveWithLabelIsDeterministicAndLabelSpecific()
    {
        EncryptionKey original = new LocalKeyFactory().create();

        assertEquals(original.deriveWithLabel("a"), original.deriveWithLabel("a"));
        assertNotEquals(original.deriveWithLabel("a"), original.deriveWithLabel("b"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void deriveWithLabelRejectsEmptyLabel()
    {
        new LocalKeyFactory().create().deriveWithLabel("");
    }

    @Test
    public void derivedKeyRoundTripsThroughAesGcm()
    {
        EncryptionKey derived = new LocalKeyFactory().create().deriveWithLabel("schema-sidecar");
        AesGcmBlockCrypto crypto = new AesGcmBlockCrypto(new com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl());
        byte[] plaintext = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        assertArrayEquals(plaintext, crypto.decrypt(derived, crypto.encrypt(derived, plaintext)));
    }
}
