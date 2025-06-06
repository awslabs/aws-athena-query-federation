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

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.CryptoResult;;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.kms.KmsClient;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class KmsEncryptionProviderTest
{
    @Mock
    KmsClient kmsClient;

    @Mock
    Cache<String, CryptoMaterialsManager> cache;

    @Mock
    CryptoMaterialsManager cryptoMaterialsManager;

    @Mock
    AwsCrypto awsCrypto;

    @Mock
    CryptoResult cryptoResult;

    private KmsEncryptionProvider kmsEncryptionProvider;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        kmsEncryptionProvider = new KmsEncryptionProvider(kmsClient, cache, awsCrypto);
    }

    @Test
    public void testDecrypt() {
        assertNotNull(cache, "Mock `cache` should be initialized by Mockito");
        Mockito.when(cache.getIfPresent("test"))
                .thenReturn(cryptoMaterialsManager);
        Mockito.when(awsCrypto.decryptData(cryptoMaterialsManager, "data".getBytes(StandardCharsets.UTF_8))).thenReturn(cryptoResult);
        Mockito.when(cryptoResult.getResult()).thenReturn("test".getBytes());
        byte[] encode = Base64.getEncoder().encode("data".getBytes(StandardCharsets.UTF_8));
        String s = kmsEncryptionProvider.decryptData(
                "test",
                new String(encode));
        assertEquals("test", s);
        Mockito.verify(awsCrypto, Mockito.times(1)).decryptData(cryptoMaterialsManager, "data".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testFASDecrypt() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        FASToken fasToken = new FASToken("access", "secret", "security");
        String tokens = objectMapper.writeValueAsString(fasToken);
        String serializedTokens = Base64.getEncoder().encodeToString(tokens.getBytes(StandardCharsets.UTF_8));
        Mockito.when(cache.getIfPresent("test"))
                .thenReturn(cryptoMaterialsManager);
        Mockito.when(awsCrypto.decryptData(cryptoMaterialsManager, tokens.getBytes(StandardCharsets.UTF_8))).thenReturn(cryptoResult);
        Mockito.when(cryptoResult.getResult()).thenReturn(objectMapper.writeValueAsString(fasToken).getBytes(StandardCharsets.UTF_8));

        AwsCredentials s = kmsEncryptionProvider.getFasCredentials("test", serializedTokens);
        assertEquals("access", s.accessKeyId());
        assertEquals("secret", s.secretAccessKey());
        Mockito.verify(awsCrypto, Mockito.times(1)).decryptData(cryptoMaterialsManager,
                tokens.getBytes(StandardCharsets.UTF_8));
    }
}