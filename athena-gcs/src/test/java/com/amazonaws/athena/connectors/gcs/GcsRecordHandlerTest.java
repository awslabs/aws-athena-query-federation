/*-
 * #%L
 * athena-google-bigquery
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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.datasource.CsvDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
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
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.io.ByteStreams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
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
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({GcsTestUtils.class, GcsSchemaUtils.class, StorageDatasourceFactory.class, GoogleCredentials.class, GcsSchemaUtils.class, AWSSecretsManagerClientBuilder.class, ServiceAccountCredentials.class})
public class GcsRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(GcsRecordHandlerTest.class);

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    @Mock
    GoogleCredentials credentials;

    @Mock
    CsvDatasource csvDatasource;

    private final List<ByteHolderTest> mockS3Storage = new ArrayList<>();
    private AmazonS3 amazonS3;
    private BlockSpiller spillWriter;
    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private FederatedIdentity federatedIdentity;
    GcsRecordHandler gcsRecordHandler;

    private final List<String> spilledRecords = new ArrayList<>();

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        PowerMockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        environmentVariables.set("gcs_credential_key", "gcs_credential_keys");
        System.setProperty("aws.region", "us-east-1");
        logger.info("Starting init.");
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        amazonS3 = mock(AmazonS3.class);
        mockS3Client();
        //Create Spill config
        //This will be enough for a single block
        //This will force the writer to spill.
        //Async Writing.
        PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(List.of("v1")).withSecretString("{\"gcs_credential_keys\": \"test\"}");
        Mockito.when(secretsManager.getSecretValue(Mockito.any())).thenReturn(getSecretValueResult);
        PowerMockito.mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        suppress(constructor(AbstractStorageDatasource.class, StorageDatasourceConfig.class));
        PowerMockito.mockStatic(StorageDatasourceFactory.class);
        PowerMockito.when(StorageDatasourceFactory.createDatasource(anyString(), Mockito.any())).thenReturn(csvDatasource);
        gcsRecordHandler = new GcsRecordHandler(amazonS3, secretsManager, athena);
    }

    @Test
    public void testReadWithConstraint()
            throws Exception
    {
        String bucket = "bucket";
        String prefix = "prefix";
        try (ReadRecordsRequest request = new ReadRecordsRequest(
                federatedIdentity,
                GcsTestUtils.PROJECT_1_NAME,
                "queryId",
                new TableName("dataset1", "table1"),
                GcsTestUtils.getBlockTestSchema(),
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket(bucket)
                                .withPrefix(prefix)
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).build(),
                new Constraints(Map.of()),
                0,          //This is ignored when directly calling readWithConstraints.
                0)) {   //This is ignored when directly calling readWithConstraints.
            //Always return try for the evaluator to keep all rows.
            ConstraintEvaluator evaluator = mock(ConstraintEvaluator.class);
            when(evaluator.apply(any(String.class), any(Object.class))).thenAnswer(
                    (InvocationOnMock invocationOnMock) -> true
            );

            //Mock out the Google BigQuery Job.
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            //Execute the test
            spillWriter = mock(BlockSpiller.class);
            doAnswer(l -> {
                spillWriter.writeRows(mock(BlockWriter.RowWriter.class));
                return l;
            }).when(csvDatasource).readRecords(any(), any(), any(),
                    any(), any(), any());
            doAnswer(l -> spilledRecords.add("record")).when(spillWriter).writeRows(any());
            gcsRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
            //Ensure that there was a spill so that we can read the spilled block.
            assertFalse(spilledRecords.isEmpty());
        }
    }

    //Mocks the S3 client by storing any putObjects() and returning the object when getObject() is called.
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
