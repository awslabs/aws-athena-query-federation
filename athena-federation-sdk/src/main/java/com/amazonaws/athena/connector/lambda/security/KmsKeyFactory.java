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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.GenerateRandomRequest;
import software.amazon.awssdk.services.kms.model.GenerateRandomResponse;
import software.amazon.awssdk.services.kms.model.NotFoundException;

/**
 * An EncryptionKeyFactory that is backed by AWS KMS.
 *
 * @see com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory
 */
public class KmsKeyFactory
        implements EncryptionKeyFactory
{
    private final KmsClient kmsClient;
    private final String masterKeyId;

    public KmsKeyFactory(KmsClient kmsClient, String masterKeyId)
    {
        this.kmsClient = kmsClient;
        this.masterKeyId = masterKeyId;
    }

    /**
     * @return A key that satisfies the specification defined in BlockCrypto
     */
    public EncryptionKey create()
    {
        GenerateDataKeyResponse dataKeyResponse;
        try {
            dataKeyResponse = kmsClient.generateDataKey(
                            GenerateDataKeyRequest.builder()
                                    .keyId(masterKeyId)
                                    .keySpec(DataKeySpec.AES_128)
                                    .build());
        }
        catch (NotFoundException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString()).build());
        }

        GenerateRandomRequest randomRequest = GenerateRandomRequest.builder()
                .numberOfBytes(AesGcmBlockCrypto.NONCE_BYTES)
                .build();
        GenerateRandomResponse randomResponse = kmsClient.generateRandom(randomRequest);

        return new EncryptionKey(dataKeyResponse.plaintext().asByteArray(), randomResponse.plaintext().asByteArray());
    }
}
