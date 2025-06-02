/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.GenerateRandomRequest;
import software.amazon.awssdk.services.kms.model.GenerateRandomResponse;

public class KmsKeyFactoryTest {

    @Mock
    private KmsClient mockKmsClient;

    private KmsKeyFactory kmsKeyFactory;

    private static final String MASTER_KEY_ID = "test-key-id";

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        kmsKeyFactory = new KmsKeyFactory(mockKmsClient, MASTER_KEY_ID);
    }

    @Test
    public void testCreate() {
        byte[] testPlaintextKey = new byte[] { 1, 2, 3, 4, 5 };
        byte[] testNonce = new byte[] { 9, 8, 7 };

        // Mock responses
        GenerateDataKeyResponse dataKeyResponse = GenerateDataKeyResponse.builder()
                .plaintext(SdkBytes.fromByteArray(testPlaintextKey))
                .build();

        GenerateRandomResponse randomResponse = GenerateRandomResponse.builder()
                .plaintext(SdkBytes.fromByteArray(testNonce))
                .build();

        when(mockKmsClient.generateDataKey((GenerateDataKeyRequest) any())).thenReturn(dataKeyResponse);
        when(mockKmsClient.generateRandom((GenerateRandomRequest) any())).thenReturn(randomResponse);

        // Act
        EncryptionKey result = kmsKeyFactory.create();

        // Assert
        assertArrayEquals(testPlaintextKey, result.getKey());
        assertArrayEquals(testNonce, result.getNonce());

        // Verify interactions
        verify(mockKmsClient).generateDataKey((GenerateDataKeyRequest) any());
        verify(mockKmsClient).generateRandom((GenerateRandomRequest) any());
    }
}
