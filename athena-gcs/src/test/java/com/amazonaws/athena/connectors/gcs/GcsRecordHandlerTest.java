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
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.File;
import java.util.Collections;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.FILE_FORMAT;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.STORAGE_SPLIT_JSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(PER_CLASS)
public class GcsRecordHandlerTest extends GenericGcsTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRecordHandlerTest.class);

    private static final String QUERY_ID = "queryId";
    private static final String PARQUET = "parquet";
    private static final String DATASET_NAME = "dataset1";
    private static final String TABLE_NAME = "table1";
    private static final String DATA_PARQUET = "[\"data.parquet\"]";

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient athena;

    @Mock
    GoogleCredentials credentials;

    private S3BlockSpiller spillWriter;
    private BlockAllocator allocator;
    private SpillConfig spillConfig;
    private Schema schemaForRead;
    private S3Client amazonS3;

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
        allocator = new BlockAllocatorImpl();
        amazonS3 = mock(S3Client.class);

        // Create Spill config
        // This will be enough for a single block
        // This will force the writer to spill.
        // Async Writing.
        spillConfig = SpillConfig.newBuilder()
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
        mockedS3Builder.when(S3Client::create).thenReturn(amazonS3);
        // To mock SecretsManagerClient via SecretsManagerClient
        mockedSecretManagerBuilder.when(SecretsManagerClient::create).thenReturn(secretsManager);
        // To mock AmazonAthena via AmazonAthenaClientBuilder
        mockedAthenaClientBuilder.when(AthenaClient::create).thenReturn(athena);
        mockedGoogleCredentials.when(() -> GoogleCredentials.fromStream(any())).thenReturn(credentials);
        schemaForRead = new Schema(GcsTestUtils.getTestSchemaFieldsArrow());

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
        allocator.close();
        bufferAllocator.close();
    }

    @BeforeEach
    public void resetSpillWriter() {
        // Reset the spillWriter before each test to ensure isolation
        spillWriter = new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead,
                ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void readWithConstraint_withParquetSplit_returnsTwoRows()
            throws Exception
    {
        // Mocking split
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn(DATA_PARQUET);
        when(split.getProperty(FILE_FORMAT)).thenReturn(PARQUET);

        // Test readWithConstraint
        try (ReadRecordsRequest request = readRecordRequest(split)) {

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            // Execute the test
            gcsRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
            assertEquals(2, spillWriter.getBlock().getRowCount(), "Total records should be 2");
        }
    }

    @Test
    public void readWithConstraint_withPartitionColumns_returnsTwoRows()
    {
        try {
            final String id = "id";
            final String name = "name";
            final String idValue = "12345";
            final String nameValue = "test_partition";

            // Mocking split with partition column properties
            Split split = mock(Split.class);
            when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn(DATA_PARQUET);
            when(split.getProperty(FILE_FORMAT)).thenReturn(PARQUET);

            // Add partition column properties - these should match field names in the schema
            when(split.getProperty(id)).thenReturn(idValue);
            when(split.getProperty(name)).thenReturn(nameValue);

            // Mock getProperties to return a map containing the partition properties
            java.util.Map<String, String> splitProperties = new java.util.HashMap<>();
            splitProperties.put(STORAGE_SPLIT_JSON, DATA_PARQUET);
            splitProperties.put(FILE_FORMAT, PARQUET);
            splitProperties.put(id, idValue);
            splitProperties.put(name, nameValue);
            when(split.getProperties()).thenReturn(splitProperties);

            // Test readWithConstraint with partition columns
            try (ReadRecordsRequest request = readRecordRequest(split)) {
                QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

                gcsRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
                assertEquals(2, spillWriter.getBlock().getRowCount(), "Total records should be 2");
            }
        } catch (Exception e) {
            Assertions.fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void readWithConstraint_withEmptyFileList_writesNoRows() throws Exception
    {
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn("[]");
        when(split.getProperty(FILE_FORMAT)).thenReturn(PARQUET);
        when(split.getProperties()).thenReturn(Collections.emptyMap());

        try (ReadRecordsRequest request = readRecordRequest(split)) {
            gcsRecordHandler.readWithConstraint(spillWriter, request, mock(QueryStatusChecker.class));
            assertEquals(0, spillWriter.getBlock().getRowCount(), "No files means no rows");
        }
    }

    @Test
    public void readWithConstraint_withUnsupportedFormat_throwsIllegalArgumentException() throws Exception
    {
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn(DATA_PARQUET);
        when(split.getProperty(FILE_FORMAT)).thenReturn("orc");
        when(split.getProperties()).thenReturn(Collections.emptyMap());

        try (ReadRecordsRequest request = readRecordRequest(split)) {
            assertThrows(IllegalArgumentException.class,
                    () -> gcsRecordHandler.readWithConstraint(spillWriter, request, mock(QueryStatusChecker.class)));
        }
    }

    @Test
    public void readWithConstraint_withInvalidSplitJson_throwsException() throws Exception
    {
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn("not-valid-json");
        when(split.getProperty(FILE_FORMAT)).thenReturn(PARQUET);

        try (ReadRecordsRequest request = readRecordRequest(split)) {
            assertThrows(Exception.class,
                    () -> gcsRecordHandler.readWithConstraint(spillWriter, request, mock(QueryStatusChecker.class)));
        }
    }

    private ReadRecordsRequest readRecordRequest(Split split) {
        return new ReadRecordsRequest(
                federatedIdentity,
                GcsTestUtils.PROJECT_1_NAME,
                QUERY_ID,
                new TableName(DATASET_NAME, TABLE_NAME), // dummy table
                GcsTestUtils.getDatatypeTestSchema(),
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                0, //This is ignored when directly calling readWithConstraints.
                0);//This is ignored when directly calling readWithConstraints.
    }
}
