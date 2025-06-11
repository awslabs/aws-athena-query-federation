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
import com.amazonaws.encryptionsdk.MasterKeyProvider;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.CryptoMaterialsCache;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.kmssdkv2.KmsMasterKey;
import com.amazonaws.encryptionsdk.kmssdkv2.KmsMasterKeyProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.kms.KmsClient;

import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class KmsEncryptionProvider
{
    private static final int CACHE_DURATION = 24;
    private static final int CACHE_SIZE = 1;
    private static final int MAX_CACHE_SIZE = 100;

    private final Cache<String, CryptoMaterialsManager> cache;
    private final AwsCrypto awsCrypto;
    private final KmsClient kmsClient;

    public KmsEncryptionProvider(KmsClient kmsClient)
    {
        this.kmsClient = kmsClient;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterWrite(CACHE_DURATION, TimeUnit.HOURS)
                .build();
        awsCrypto = AwsCrypto.builder().build();
    }

    @VisibleForTesting
    protected KmsEncryptionProvider(final KmsClient kmsClient,
                                 final Cache<String, CryptoMaterialsManager> cache,
                                 final AwsCrypto awsCrypto)
    {
        this.cache = cache;
        this.awsCrypto = awsCrypto;
        this.kmsClient = kmsClient;
    }

    public AwsCredentials getFasCredentials(final String kmsKeyId,
                                            final String fasToken)
    {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String fasString = this.decryptData(kmsKeyId, fasToken);
            FASToken credentials = objectMapper.readValue(fasString, FASToken.class);
            return AwsSessionCredentials.builder()
                    .accessKeyId(credentials.getAccessToken())
                    .secretAccessKey(credentials.getSecretToken())
                    .sessionToken(credentials.getSecurityToken())
                    .build();
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param kmsKeyId the KMS key
     * @param data     The encrypted data
     * @return the unencrypted data
     */
    public String decryptData(final String kmsKeyId,
                       final String data)
    {
        CryptoMaterialsManager cachingCmm = getCachingCmm(kmsKeyId);
        byte[] decode = Base64.getDecoder().decode(data);
        return new String(awsCrypto.decryptData(cachingCmm, decode).getResult());
    }

    private CryptoMaterialsManager getCachingCmm(String kmsKeyId)
    {
        CryptoMaterialsManager cachingCmm = cache.getIfPresent(kmsKeyId);
        if (cachingCmm == null) {
            CryptoMaterialsCache cache = new LocalCryptoMaterialsCache(CACHE_SIZE);
            MasterKeyProvider<KmsMasterKey> keyProvider = KmsMasterKeyProvider.builder()
                    .customRegionalClientSupplier((region) -> kmsClient)
                    .buildStrict(kmsKeyId);

            cachingCmm = CachingCryptoMaterialsManager.newBuilder()
                    .withMasterKeyProvider(keyProvider)
                    .withCache(cache)
                    .withMaxAge(CACHE_DURATION, TimeUnit.HOURS)
                    .build();
            this.cache.put(kmsKeyId, cachingCmm);
        }
        return cachingCmm;
    }
}
