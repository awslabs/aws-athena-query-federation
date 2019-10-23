package com.amazonaws.athena.connector.lambda.security;

public interface EncryptionKeyFactory
{
    /**
     * @return A key that satisfies the specification defined in BlockCrypto
     */
    EncryptionKey create();
}
