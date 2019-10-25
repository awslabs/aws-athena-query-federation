package com.amazonaws.athena.connector.lambda.handlers;

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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.KmsKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperFactory;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.handlers.FederationCapabilities.CAPABILITIES;

public abstract class MetadataHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataHandler.class);
    private static final String DISABLE_ENCRYPTION = "true";
    private static final String DEFAULT_SPILL_PREFIX = "athena-federation-spill";
    protected static final String SPILL_BUCKET_ENV = "spill_bucket";
    protected static final String SPILL_PREFIX_ENV = "spill_prefix";
    protected static final String KMS_KEY_ID_ENV = "kms_key_id";
    protected static final String DISABLE_SPILL_ENCRYPTION = "disable_spill_encryption";

    private final CachableSecretsManager secretsManager;
    private final EncryptionKeyFactory encryptionKeyFactory;
    private final String spillBucket;
    private final String spillPrefix;
    private final String sourceType;

    /**
     * @param sourceType Used to aid in logging diagnostic info when raising a support case.
     */
    public MetadataHandler(String sourceType)
    {
        this.sourceType = sourceType;
        this.spillBucket = System.getenv(SPILL_BUCKET_ENV);
        this.spillPrefix = System.getenv(SPILL_PREFIX_ENV) == null ?
                DEFAULT_SPILL_PREFIX : System.getenv(SPILL_PREFIX_ENV);
        if (System.getenv(DISABLE_SPILL_ENCRYPTION) == null ||
                !DISABLE_ENCRYPTION.equalsIgnoreCase(System.getenv(DISABLE_SPILL_ENCRYPTION))) {
            encryptionKeyFactory = (System.getenv(KMS_KEY_ID_ENV) != null) ?
                    new KmsKeyFactory(AWSKMSClientBuilder.standard().build(), System.getenv(KMS_KEY_ID_ENV)) :
                    new LocalKeyFactory();
        }
        else {
            encryptionKeyFactory = null;
        }

        this.secretsManager = new CachableSecretsManager(AWSSecretsManagerClientBuilder.standard().build());
    }

    /**
     * @param sourceType Used to aid in logging diagnostic info when raising a support case.
     */
    public MetadataHandler(EncryptionKeyFactory encryptionKeyFactory,
            AWSSecretsManager secretsManager,
            String sourceType,
            String spillBucket,
            String spillPrefix)
    {
        this.encryptionKeyFactory = encryptionKeyFactory;
        this.secretsManager = new CachableSecretsManager(secretsManager);
        this.sourceType = sourceType;
        this.spillBucket = spillBucket;
        this.spillPrefix = spillPrefix;
    }

    /**
     * Resolves any secrets found in the supplied string, for example: MyString${WithSecret} would have ${WithSecret}
     * by the corresponding value of the secret in AWS Secrets Manager with that name. If no such secret is found
     * the function throws.
     *
     * @param rawString The string in which you'd like to replace SecretsManager placeholders.
     * (e.g. ThisIsA${Secret}Here - The ${Secret} would be replaced with the contents of an SecretsManager
     * secret called Secret. If no such secret is found, the function throws. If no ${} are found in
     * the input string, nothing is replaced and the original string is returned.
     */
    protected String resolveSecrets(String rawString)
    {
        return secretsManager.resolveSecrets(rawString);
    }

    protected String getSecret(String secretName)
    {
        return secretsManager.getSecret(secretName);
    }

    protected EncryptionKey makeEncryptionKey()
    {
        return (encryptionKeyFactory != null) ? encryptionKeyFactory.create() : null;
    }

    protected SpillLocation makeSpillLocation(MetadataRequest request)
    {
        return S3SpillLocation.newBuilder()
                .withBucket(spillBucket)
                .withPrefix(spillPrefix)
                .withQueryId(request.getQueryId())
                .withSplitId(UUID.randomUUID().toString())
                .build();
    }

    final public void handleRequest(InputStream inputStream, OutputStream outputStream, final Context context)
            throws IOException
    {
        try (BlockAllocator allocator = new BlockAllocatorImpl()) {
            ObjectMapper objectMapper = ObjectMapperFactory.create(allocator);
            try (FederationRequest rawReq = objectMapper.readValue(inputStream, FederationRequest.class)) {
                if (rawReq instanceof PingRequest) {
                    try (PingResponse response = doPing((PingRequest) rawReq)) {
                        assertNotNull(response);
                        objectMapper.writeValue(outputStream, response);
                    }
                    return;
                }

                if (!(rawReq instanceof MetadataRequest)) {
                    throw new RuntimeException("Expected a MetadataRequest but found " + rawReq.getClass());
                }

                MetadataRequest req = (MetadataRequest) rawReq;
                MetadataRequestType type = req.getRequestType();
                switch (type) {
                    case LIST_SCHEMAS:
                        try (ListSchemasResponse response = doListSchemaNames(allocator, (ListSchemasRequest) req)) {
                            assertNotNull(response);
                            objectMapper.writeValue(outputStream, response);
                        }
                        return;
                    case LIST_TABLES:
                        try (ListTablesResponse response = doListTables(allocator, (ListTablesRequest) req)) {
                            assertNotNull(response);
                            objectMapper.writeValue(outputStream, response);
                        }
                        return;
                    case GET_TABLE:
                        try (GetTableResponse response = doGetTable(allocator, (GetTableRequest) req)) {
                            assertNotNull(response);
                            objectMapper.writeValue(outputStream, response);
                        }
                        return;
                    case GET_TABLE_LAYOUT:
                        try (GetTableLayoutResponse response = doGetTableLayout(allocator, (GetTableLayoutRequest) req)) {
                            assertNotNull(response);
                            objectMapper.writeValue(outputStream, response);
                        }
                        return;
                    case GET_SPLITS:
                        try (GetSplitsResponse response = doGetSplits(allocator, (GetSplitsRequest) req)) {
                            assertNotNull(response);
                            objectMapper.writeValue(outputStream, response);
                        }
                        return;
                    default:
                        throw new IllegalArgumentException("Unknown request type " + type);
                }
            }
            catch (Exception ex) {
                logger.warn("handleRequest: Completed with an exception.", ex);
                throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
            }
        }
    }

    protected abstract ListSchemasResponse doListSchemaNames(final BlockAllocator allocator, final ListSchemasRequest request)
            throws Exception;

    protected abstract ListTablesResponse doListTables(final BlockAllocator allocator, final ListTablesRequest request)
            throws Exception;

    protected abstract GetTableResponse doGetTable(final BlockAllocator allocator, final GetTableRequest request)
            throws Exception;

    protected abstract GetTableLayoutResponse doGetTableLayout(final BlockAllocator allocator, final GetTableLayoutRequest request)
            throws Exception;

    protected abstract GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
            throws Exception;

    private final PingResponse doPing(PingRequest request)
    {
        PingResponse response = new PingResponse(request.getCatalogName(), request.getQueryId(), sourceType, CAPABILITIES);
        try {
            onPing(request);
        }
        catch (Exception ex) {
            logger.warn("doPing: encountered an exception while delegating onPing.", ex);
        }
        return response;
    }

    protected void onPing(PingRequest request)
    {
        //NoOp
    }

    private void assertNotNull(FederationResponse response)
    {
        if (response == null) {
            throw new RuntimeException("Response was null");
        }
    }
}
