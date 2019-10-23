package com.amazonaws.athena.connector.lambda.security;

import com.amazonaws.athena.connector.lambda.data.Block;
import org.apache.arrow.vector.types.pojo.Schema;

public interface BlockCrypto
{
    byte[] encrypt(EncryptionKey key, Block block);

    Block decrypt(EncryptionKey key, byte[] bytes, Schema schema);

    byte[] decrypt(EncryptionKey key, byte[] bytes);
}
