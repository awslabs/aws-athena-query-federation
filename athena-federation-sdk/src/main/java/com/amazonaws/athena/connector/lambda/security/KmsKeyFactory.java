package com.amazonaws.athena.connector.lambda.security;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.model.DataKeySpec;
import com.amazonaws.services.kms.model.GenerateDataKeyRequest;
import com.amazonaws.services.kms.model.GenerateDataKeyResult;
import com.amazonaws.services.kms.model.GenerateRandomRequest;
import com.amazonaws.services.kms.model.GenerateRandomResult;

public class KmsKeyFactory
        implements EncryptionKeyFactory
{
    private final AWSKMS kmsClient;
    private final String masterKeyId;

    public KmsKeyFactory(AWSKMS kmsClient, String masterKeyId)
    {
        this.kmsClient = kmsClient;
        this.masterKeyId = masterKeyId;
    }

    public EncryptionKey create()
    {
        GenerateDataKeyResult dataKeyResult =
                kmsClient.generateDataKey(
                        new GenerateDataKeyRequest()
                                .withKeyId(masterKeyId)
                                .withKeySpec(DataKeySpec.AES_128));

        GenerateRandomRequest randomRequest = new GenerateRandomRequest()
                .withNumberOfBytes(AesGcmBlockCrypto.NONCE_BYTES);
        GenerateRandomResult randomResult = kmsClient.generateRandom(randomRequest);

        return new EncryptionKey(dataKeyResult.getPlaintext().array(), randomResult.getPlaintext().array());
    }
}
