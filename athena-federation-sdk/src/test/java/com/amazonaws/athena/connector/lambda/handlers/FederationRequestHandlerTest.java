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
package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.KmsEncryptionProvider;
import org.junit.Before;
import org.mockito.Mock;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.FAS_TOKEN;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SERVICE_KMS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FederationRequestHandlerTest {

    @Mock
    private KmsEncryptionProvider kmsEncryptionProvider;

    @Mock
    private AwsCredentials awsCredentials;

    @Mock
    private CachableSecretsManager secretsManager;

    @Mock
    private FederationRequest federationRequest;

    @Mock
    private FederatedIdentity federatedIdentity;

    @Mock
    private S3Client defaultS3Client;

    @Mock
    private AthenaClient defaultAthenaClient;

    private FederationRequestHandler handler;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Map<String, String> configMap = new HashMap<>();
        handler = new TestMetadataHandler("TestType", configMap, secretsManager, kmsEncryptionProvider);
    }

    @Test
    public void testGetSessionCredentials() {
        String kmsKeyId = "test-key-id";
        String tokenString = "test-token";

        when(kmsEncryptionProvider.getFasCredentials(kmsKeyId, tokenString))
                .thenReturn(awsCredentials);

        AwsCredentials result = handler.getSessionCredentials(kmsKeyId, tokenString, kmsEncryptionProvider);

        assertEquals(awsCredentials, result);
        verify(kmsEncryptionProvider).getFasCredentials(kmsKeyId, tokenString);
    }

    @Test
    public void testGetRequestOverrideConfig_WithValidConfig() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(FAS_TOKEN, "test-token");
        configOptions.put(SERVICE_KMS, "test-kms-key");

        when(kmsEncryptionProvider.getFasCredentials(anyString(), anyString()))
                .thenReturn(awsCredentials);

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(
                configOptions, kmsEncryptionProvider);

        assertNotNull(result);
        assertTrue(result.credentialsProvider().isPresent());
    }

    @Test
    public void testGetRequestOverrideConfig_WithEmptyConfig() {
        Map<String, String> configOptions = new HashMap<>();

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(
                configOptions, kmsEncryptionProvider);

        assertNull(result);
    }

    @Test
    public void testGetRequestOverrideConfig_WithNullConfig() {
        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(
                null, kmsEncryptionProvider);

        assertNull(result);
    }

    @Test
    public void testGetRequestOverrideConfig_WithMissingFasToken() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(SERVICE_KMS, "test-kms-key");

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(
                configOptions, kmsEncryptionProvider);

        assertNull(result);
        verify(kmsEncryptionProvider, never()).getFasCredentials(anyString(), anyString());
    }

    @Test
    public void testGetRequestOverrideConfig_WithMissingServiceKms() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(FAS_TOKEN, "test-token");

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(
                configOptions, kmsEncryptionProvider);

        assertNull(result);
        verify(kmsEncryptionProvider, never()).getFasCredentials(anyString(), anyString());
    }

    @Test
    public void testGetRequestOverrideConfig_MapOverload() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(FAS_TOKEN, "test-token");
        configOptions.put(SERVICE_KMS, "test-kms-key");

        when(kmsEncryptionProvider.getFasCredentials(anyString(), anyString()))
                .thenReturn(awsCredentials);

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(configOptions);

        assertNotNull(result);
        assertTrue(result.credentialsProvider().isPresent());
    }

    @Test
    public void testGetRequestOverrideConfig_FromFederationRequest_WithFederatedIdentity() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(FAS_TOKEN, "test-token");
        configOptions.put(SERVICE_KMS, "test-kms-key");

        when(federationRequest.getIdentity()).thenReturn(federatedIdentity);
        when(federatedIdentity.getConfigOptions()).thenReturn(configOptions);
        when(kmsEncryptionProvider.getFasCredentials(anyString(), anyString()))
                .thenReturn(awsCredentials);

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(federationRequest);

        assertNotNull(result);
        assertTrue(result.credentialsProvider().isPresent());
    }

    @Test
    public void testGetRequestOverrideConfig_FromFederationRequest_WithNullIdentity() {
        when(federationRequest.getIdentity()).thenReturn(null);

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(federationRequest);

        assertNull(result);
    }

    @Test
    public void testGetRequestOverrideConfig_FromFederationRequest_WithNullConfigOptions() {
        when(federationRequest.getIdentity()).thenReturn(federatedIdentity);
        when(federatedIdentity.getConfigOptions()).thenReturn(null);

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(federationRequest);

        assertNull(result);
    }

    @Test
    public void testGetRequestOverrideConfig_FromFederationRequest_WithoutFasToken() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(SERVICE_KMS, "test-kms-key");

        when(federationRequest.getIdentity()).thenReturn(federatedIdentity);
        when(federatedIdentity.getConfigOptions()).thenReturn(configOptions);

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(federationRequest);

        assertNull(result);
    }

    @Test
    public void testGetRequestOverrideConfig_FromFederationRequest_NonFederated() {
        Map<String, String> configOptions = new HashMap<>();

        when(federationRequest.getIdentity()).thenReturn(federatedIdentity);
        when(federatedIdentity.getConfigOptions()).thenReturn(configOptions);

        AwsRequestOverrideConfiguration result = handler.getRequestOverrideConfig(federationRequest);

        assertNull(result);
    }

    @Test
    public void testGetS3Client_WithOverrideConfig() {
        AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .credentialsProvider(credentialsProvider)
                .build();

        S3Client result = handler.getS3Client(overrideConfig, defaultS3Client);

        assertNotNull(result);
    }

    @Test
    public void testGetS3Client_WithNullOverrideConfig() {
        S3Client result = handler.getS3Client(null, defaultS3Client);

        assertEquals(defaultS3Client, result);
    }

    @Test
    public void testGetS3Client_WithOverrideConfigButNoCredentials() {
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .build();

        S3Client result = handler.getS3Client(overrideConfig, defaultS3Client);

        assertEquals(defaultS3Client, result);
    }

    @Test
    public void testGetAthenaClient_WithOverrideConfig() {
        AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .credentialsProvider(credentialsProvider)
                .build();

        AthenaClient result = handler.getAthenaClient(overrideConfig, defaultAthenaClient);

        assertNotNull(result);
    }

    @Test
    public void testGetAthenaClient_WithNullOverrideConfig() {
        AthenaClient result = handler.getAthenaClient(null, defaultAthenaClient);

        assertEquals(defaultAthenaClient, result);
    }

    @Test
    public void testGetAthenaClient_WithOverrideConfigButNoCredentials() {
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .build();

        AthenaClient result = handler.getAthenaClient(overrideConfig, defaultAthenaClient);

        assertEquals(defaultAthenaClient, result);
    }

    @Test
    public void testIsRequestFederated_WithFederatedRequest() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(FAS_TOKEN, "test-token");

        when(federationRequest.getIdentity()).thenReturn(federatedIdentity);
        when(federatedIdentity.getConfigOptions()).thenReturn(configOptions);

        boolean result = handler.isRequestFederated(federationRequest);

        assertTrue(result);
    }

    @Test
    public void testIsRequestFederated_WithNonFederatedRequest() {
        Map<String, String> configOptions = new HashMap<>();

        when(federationRequest.getIdentity()).thenReturn(federatedIdentity);
        when(federatedIdentity.getConfigOptions()).thenReturn(configOptions);

        boolean result = handler.isRequestFederated(federationRequest);

        assertFalse(result);
    }

    @Test
    public void testIsRequestFederated_WithNullIdentity() {
        when(federationRequest.getIdentity()).thenReturn(null);

        boolean result = handler.isRequestFederated(federationRequest);

        assertFalse(result);
    }

    @Test
    public void testIsRequestFederated_WithNullConfigOptions() {
        when(federationRequest.getIdentity()).thenReturn(federatedIdentity);
        when(federatedIdentity.getConfigOptions()).thenReturn(null);

        boolean result = handler.isRequestFederated(federationRequest);

        assertFalse(result);
    }

    @Test
    public void testResolveSecrets() {
        String rawString = "MyString${WithSecret}";
        String expectedResult = "MyStringResolvedSecret";

        when(secretsManager.resolveSecrets(rawString)).thenReturn(expectedResult);

        String result = handler.resolveSecrets(rawString);

        assertEquals(expectedResult, result);
        verify(secretsManager).resolveSecrets(rawString);
    }

    @Test
    public void testResolveWithDefaultCredentials() {
        String rawString = "MyString${Secret}";
        String expectedResult = "username:password";

        when(secretsManager.resolveWithDefaultCredentials(rawString)).thenReturn(expectedResult);

        String result = handler.resolveWithDefaultCredentials(rawString);

        assertEquals(expectedResult, result);
        verify(secretsManager).resolveWithDefaultCredentials(rawString);
    }

    @Test
    public void testGetSecret() {
        String secretName = "test-secret";
        String expectedSecret = "secret-value";

        when(secretsManager.getSecret(secretName)).thenReturn(expectedSecret);

        String result = handler.getSecret(secretName);

        assertEquals(expectedSecret, result);
        verify(secretsManager).getSecret(secretName);
    }

    @Test
    public void testGetSecret_WithRequestOverrideConfiguration() {
        String secretName = "test-secret";
        String expectedSecret = "secret-value";
        AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .credentialsProvider(credentialsProvider)
                .build();

        when(secretsManager.getSecret(secretName, overrideConfig)).thenReturn(expectedSecret);

        String result = handler.getSecret(secretName, overrideConfig);

        assertEquals(expectedSecret, result);
        verify(secretsManager).getSecret(secretName, overrideConfig);
    }

    @Test
    public void testGetCredentialProvider_WithValidSecretName() {
        String secretName = "test-db-secret";
        String secretValue = "{\"username\":\"testuser\",\"password\":\"testpass\"}";
        AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .credentialsProvider(credentialsProvider)
                .build();

        TestMetadataHandler handlerWithSecret = new TestMetadataHandler(
                "TestType", new HashMap<>(), secretsManager, kmsEncryptionProvider, secretName);

        when(secretsManager.getSecret(secretName, overrideConfig)).thenReturn(secretValue);

        CredentialsProvider result = handlerWithSecret.getCredentialProvider(overrideConfig);

        assertNotNull(result);
        verify(secretsManager).getSecret(secretName, overrideConfig);
    }

    @Test
    public void testGetCredentialProvider_WithBlankSecretName() {
        TestMetadataHandler handlerWithoutSecret = new TestMetadataHandler(
                "TestType", new HashMap<>(), secretsManager, kmsEncryptionProvider, "");

        CredentialsProvider result = handlerWithoutSecret.getCredentialProvider(null);

        assertNull(result);
        verify(secretsManager, never()).getSecret(anyString(), any());
    }

    @Test
    public void testGetCredentialProvider_WithNullSecretName() {
        CredentialsProvider result = handler.getCredentialProvider(null);

        assertNull(result);
        verify(secretsManager, never()).getSecret(anyString(), any());
    }

    @Test
    public void testCreateCredentialsProvider() {
        String secretName = "test-secret";
        String secretValue = "{\"username\":\"testuser\",\"password\":\"testpass\"}";
        AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .credentialsProvider(credentialsProvider)
                .build();

        when(secretsManager.getSecret(secretName, overrideConfig)).thenReturn(secretValue);

        CredentialsProvider result = handler.createCredentialsProvider(secretName, overrideConfig);

        assertNotNull(result);
        verify(secretsManager).getSecret(secretName, overrideConfig);
    }

    @Test
    public void testGetDatabaseConnectionSecret_DefaultBehavior() {
        String result = handler.getDatabaseConnectionSecret();

        assertNull(result);
    }

    public static class TestMetadataHandler extends MetadataHandler {

        private final CachableSecretsManager secretsManager;
        private final KmsEncryptionProvider kmsEncryptionProvider;
        private final String databaseConnectionSecret;

        public TestMetadataHandler(String sourceType, Map<String, String> configOptions,
                                   CachableSecretsManager secretsManager,
                                   KmsEncryptionProvider kmsEncryptionProvider) {
            this(sourceType, configOptions, secretsManager, kmsEncryptionProvider, null);
        }

        public TestMetadataHandler(String sourceType, Map<String, String> configOptions,
                                   CachableSecretsManager secretsManager,
                                   KmsEncryptionProvider kmsEncryptionProvider,
                                   String databaseConnectionSecret) {
            super(sourceType, configOptions);
            this.secretsManager = secretsManager;
            this.kmsEncryptionProvider = kmsEncryptionProvider;
            this.databaseConnectionSecret = databaseConnectionSecret;
        }

        @Override
        public CachableSecretsManager getCachableSecretsManager() {
            return secretsManager;
        }

        @Override
        public KmsEncryptionProvider getKmsEncryptionProvider() {
            return kmsEncryptionProvider;
        }

        @Override
        public String getDatabaseConnectionSecret() {
            return databaseConnectionSecret;
        }

        @Override
        public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request,
                                  QueryStatusChecker queryStatusChecker) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws Exception {
            throw new UnsupportedOperationException();
        }
    }
}
