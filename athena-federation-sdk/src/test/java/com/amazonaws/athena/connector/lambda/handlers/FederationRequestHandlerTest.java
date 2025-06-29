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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FederationRequestHandlerTest {

    @Mock
    private KmsEncryptionProvider kmsEncryptionProvider;

    @Mock
    private AwsCredentials awsCredentials;

    private FederationRequestHandler handler;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Map<String, String> configMap = new HashMap<>();
        handler = new TestMetadataHandler("TestType", configMap);
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
    public void testGetS3Client_WithOverrideConfig() {
        AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .credentialsProvider(credentialsProvider)
                .build();

        S3Client result = handler.getS3Client(overrideConfig, S3Client.create());

        assertNotNull(result);
    }

    @Test
    public void testGetAthenaClient_WithOverrideConfig() {
        AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
                .credentialsProvider(credentialsProvider)
                .build();

        AthenaClient result = handler.getAthenaClient(overrideConfig, AthenaClient.create());

        assertNotNull(result);
    }

    public static class TestMetadataHandler extends MetadataHandler {

        public TestMetadataHandler(String sourceType, Map<String, String> configOptions) {
            super(sourceType, configOptions);
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
