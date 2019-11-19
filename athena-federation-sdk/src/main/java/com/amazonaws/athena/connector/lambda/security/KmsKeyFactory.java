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

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.model.DataKeySpec;
import com.amazonaws.services.kms.model.GenerateDataKeyRequest;
import com.amazonaws.services.kms.model.GenerateDataKeyResult;
import com.amazonaws.services.kms.model.GenerateRandomRequest;
import com.amazonaws.services.kms.model.GenerateRandomResult;

/**
 * An EncryptionKeyFactory that is backed by AWS KMS.
 *
 * @see com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory
 */
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

    /**
     * @return A key that satisfies the specification defined in BlockCrypto
     */
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
