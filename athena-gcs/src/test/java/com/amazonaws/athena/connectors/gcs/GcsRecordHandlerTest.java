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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.UUID;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.FILE_FORMAT;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.STORAGE_SPLIT_JSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(PER_CLASS)
public class GcsRecordHandlerTest extends GenericGcsTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRecordHandlerTest.class);

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    @Mock
    GoogleCredentials credentials;

    private S3BlockSpiller spillWriter;


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

    private static final BufferAllocator bufferAllocator = new RootAllocator();


    @BeforeAll
    public void initCommonMockedStatic()
    {
        super.initCommonMockedStatic();
        System.setProperty("aws.region", "us-east-1");
        LOGGER.info("Starting init.");
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        BlockAllocator allocator = new BlockAllocatorImpl();
        AmazonS3 amazonS3 = mock(AmazonS3.class);

        // Create Spill config
        // This will be enough for a single block
        // This will force the writer to spill.
        // Async Writing.
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
        // To mock AmazonS3 via AmazonS3ClientBuilder
        mockedS3Builder.when(AmazonS3ClientBuilder::defaultClient).thenReturn(amazonS3);
        // To mock AWSSecretsManager via AWSSecretsManagerClientBuilder
        mockedSecretManagerBuilder.when(AWSSecretsManagerClientBuilder::defaultClient).thenReturn(secretsManager);
        // To mock AmazonAthena via AmazonAthenaClientBuilder
        mockedAthenaClientBuilder.when(AmazonAthenaClientBuilder::defaultClient).thenReturn(athena);
        mockedGoogleCredentials.when(() -> GoogleCredentials.fromStream(any())).thenReturn(credentials);
        Schema schemaForRead = new Schema(GcsTestUtils.getTestSchemaFieldsArrow());
        spillWriter = new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());

        // Mocking GcsUtil
        final File parquetFile = new File(GcsRecordHandlerTest.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        mockedGcsUtil.when(() -> GcsUtil.createUri(anyString())).thenReturn("file:" + parquetFile.getPath() + "/" + "person-data.parquet");

        // The class we want to test.
        gcsRecordHandler = new GcsRecordHandler(bufferAllocator, com.google.common.collect.ImmutableMap.of());
        LOGGER.info("Completed init.");
    }

    @AfterAll
    public void closeMockedObjects() {
        super.closeMockedObjects();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReadWithConstraint()
            throws Exception
    {
        // Mocking split
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn("[\"data.parquet\"]");
        when(split.getProperty(FILE_FORMAT)).thenReturn("parquet");

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
                0)) {  //This is ignored when directly calling readWithConstraints.

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            // Execute the test
            gcsRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
            assertEquals(2, spillWriter.getBlock().getRowCount(), "Total records should be 2");
        }
    }

}
