/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.gcs.storage.StorageMetadata;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.STORAGE_SPLIT_JSON;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;
import static org.testng.AssertJUnit.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({GcsTestUtils.class, GcsUtil.class, GoogleCredentials.class, AWSSecretsManagerClientBuilder.class})
public class GcsRecordHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRecordHandlerTest.class);

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    @Mock
    GoogleCredentials credentials;

    @Mock
    StorageMetadata storageMetadata;

    private AmazonS3 amazonS3;

    private S3BlockSpiller spillWriter;

    private final List<ByteHolderTest> mockS3Storage = new ArrayList<>();

    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private final EncryptionKey encryptionKey = keyFactory.create();
    private final String queryId = UUID.randomUUID().toString();
    private final S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(queryId)
            .withIsDirectory(true)
            .build();
    private FederatedIdentity federatedIdentity;
    GcsRecordHandler gcsRecordHandler;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws IOException
    {
        System.setProperty("aws.region", "us-east-1");
        LOGGER.info("Starting init.");
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        BlockAllocator allocator = new BlockAllocatorImpl();
        amazonS3 = mock(AmazonS3.class);
        mockS3Client();
        //Create Spill config
        //This will be enough for a single block
        //This will force the writer to spill.
        //Async Writing.
        SpillConfig spillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(encryptionKey)
                //This will be enough for a single block
                .withMaxBlockBytes(100000)
                //This will force the writer to spill.
                .withMaxInlineBlockBytes(100)
                //Async Writing.
                .withNumSpillThreads(0)
                .withRequestId(UUID.randomUUID().toString())
                .withSpillLocation(s3SpillLocation)
                .build();
        PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(List.of("v1")).withSecretString("{\"athena_gcs_keys\": \"test\"}");
        when(secretsManager.getSecretValue(any())).thenReturn(getSecretValueResult);
        PowerMockito.mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        suppress(constructor(StorageMetadata.class, String.class, Map.class));
        //PowerMockito.mockStatic(StorageDatasourceFactory.class);
        //PowerMockito.when(StorageDatasourceFactory.createDatasource(anyString(), any())).thenReturn(storageMetadata);
        Schema schemaForRead = new Schema(GcsTestUtils.getTestSchemaFieldsArrow());
        spillWriter = new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead, ConstraintEvaluator.emptyEvaluator());

        // Mocking GcsUtil
        PowerMockito.mockStatic(GcsUtil.class);
        PowerMockito.when(GcsUtil.getGcsCredentialJsonString(anyString(), anyString())).thenReturn("mockJson");
        final File parquetFile = new File(GcsRecordHandlerTest.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        PowerMockito.when(GcsUtil.createUri(anyString())).thenReturn( "file:" + parquetFile.getPath() + "/" + "person-data.parquet");

        // The class we want to test.
        gcsRecordHandler = new GcsRecordHandler(amazonS3, secretsManager, athena);

        LOGGER.info("Completed init.");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReadWithConstraint()
            throws Exception
    {
        // Mocking split
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn("[{\"fileName\": \"data.parquet\"}]");
        when(split.getProperty(CLASSIFICATION_GLUE_TABLE_PARAM)).thenReturn("parquet");

        // Test readWithConstraint
        try (ReadRecordsRequest request = new ReadRecordsRequest(
                federatedIdentity,
                GcsTestUtils.PROJECT_1_NAME,
                "queryId",
                new TableName("dataset1", "table1"), // dummy table
                GcsTestUtils.getDatatypeTestSchema(),
                split,
                new Constraints(Collections.EMPTY_MAP),
                0, //This is ignored when directly calling readWithConstraints.
                0);
             ConstraintEvaluator evaluator = mock(ConstraintEvaluator.class)) {  //This is ignored when directly calling readWithConstraints.
            //Always return true for the evaluator to keep all rows.

            when(evaluator.apply(any(String.class), any(Object.class))).thenAnswer((InvocationOnMock invocationOnMock) -> true);
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            //Execute the test
            gcsRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
            assertEquals("Total records should be 2", 2, spillWriter.getBlock().getRowCount());
        }
    }

    // Mocking Amazon S3 for spilling records
    private void mockS3Client()
    {
        when(amazonS3.putObject(any()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                    ByteHolderTest byteHolderTest = new ByteHolderTest();
                    byteHolderTest.setBytes(ByteStreams.toByteArray(inputStream));
                    mockS3Storage.add(byteHolderTest);
                    return mock(PutObjectResult.class);
                });

        when(amazonS3.getObject(anyString(), anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolderTest byteHolderTest = mockS3Storage.get(0);
                    mockS3Storage.remove(0);
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolderTest.getBytes()), null));
                    return mockObject;
                });
    }

    private static class ByteHolderTest
    {
        private byte[] bytes;
        void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }
        byte[] getBytes()
        {
            return bytes;
        }
    }
}
