package com.amazonaws.athena.connector.lambda.security;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

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
