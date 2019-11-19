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

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * An EncryptionKeyFactory that is backed by a local source of randomness.
 *
 * @see com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory
 */
public class LocalKeyFactory
        implements EncryptionKeyFactory
{
    public EncryptionKey create()
    {
        try {
            SecureRandom random = SecureRandom.getInstanceStrong();
            KeyGenerator keyGen = KeyGenerator.getInstance(AesGcmBlockCrypto.KEYSPEC);
            keyGen.init(AesGcmBlockCrypto.KEY_BYTES * 8, random);
            SecretKey key = keyGen.generateKey();
            final byte[] nonce = new byte[AesGcmBlockCrypto.NONCE_BYTES];
            random.nextBytes(nonce);
            return new EncryptionKey(key.getEncoded(), nonce);
        }
        catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }
}
