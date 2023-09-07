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

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An EncryptionKeyFactory that is backed by a local source of randomness.
 *
 * @see com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory
 */
public class LocalKeyFactory
        implements EncryptionKeyFactory
{
    private static ExecutorService es = Executors.newSingleThreadExecutor();

    public EncryptionKey create()
    {
        // Try to generate the key in a separate thread
        Future<EncryptionKey> future = es.submit(() -> {
            SecureRandom random = SecureRandom.getInstanceStrong();
            KeyGenerator keyGen = KeyGenerator.getInstance(AesGcmBlockCrypto.KEYSPEC);
            keyGen.init(AesGcmBlockCrypto.KEY_BYTES * 8, random);
            SecretKey key = keyGen.generateKey();
            final byte[] nonce = new byte[AesGcmBlockCrypto.NONCE_BYTES];
            random.nextBytes(nonce);
            return new EncryptionKey(key.getEncoded(), nonce);
        });

        // Now wait at most 1 second for it to finish.
        // If it doesn't finish within a second, its blocked on /dev/random, so we'll
        // cancel the future and interrupt the thread.
        try {
            return future.get(1000, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
            throw new RuntimeException("Attempt to generate key took too long. There may be an issue where your platform does not have enough entropy in /dev/random. Consider using KmsKeyFactory instead", ex);
        }
        catch (Exception ex) {
            // Rethrow unchecked because the underlying interface doesn't declare any throws
            throw new RuntimeException(ex);
        }
        finally {
            future.cancel(true);
        }
    }
}
