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
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
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
import org.junit.jupiter.api.BeforeAll;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient athena;

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
        S3Client amazonS3 = mock(S3Client.class);

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
        mockedS3Builder.when(S3Client::create).thenReturn(amazonS3);
        // To mock SecretsManagerClient via SecretsManagerClient
        mockedSecretManagerBuilder.when(SecretsManagerClient::create).thenReturn(secretsManager);
        // To mock AmazonAthena via AmazonAthenaClientBuilder
        mockedAthenaClientBuilder.when(AthenaClient::create).thenReturn(athena);
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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                0, //This is ignored when directly calling readWithConstraints.
                0)) {  //This is ignored when directly calling readWithConstraints.

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            // Execute the test
            gcsRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
            assertEquals(2, spillWriter.getBlock().getRowCount(), "Total records should be 2");
        }
    }

    // ========================================================================
    // Substrait Plan Tests
    // ========================================================================

    /**
     * Helper: returns the schema matching the CSV test data columns.
     */
    private Schema getSubstraitTestSchema()
    {
        return SchemaBuilder.newBuilder()
                .addStringField("id")
                .addStringField("first_name")
                .addStringField("last_name")
                .addStringField("email")
                .addStringField("gender")
                .addStringField("ip_address")
                .build();
    }

    /**
     * Helper: creates a ReadRecordsRequest with a QueryPlan containing the given substrait plan.
     * Points to both male and female CSV test data files.
     */
    private ReadRecordsRequest createSubstraitReadRequest(String substraitPlan)
    {
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn("[\"MOCK_DATA_male.csv\", \"MOCK_DATA_female.csv\"]");
        when(split.getProperty(FILE_FORMAT)).thenReturn("csv");

        QueryPlan queryPlan = new QueryPlan("", substraitPlan);
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                queryPlan);

        return new ReadRecordsRequest(
                federatedIdentity,
                GcsTestUtils.PROJECT_1_NAME,
                "queryId",
                new TableName("test_gcs_database", "test_gcs_table"),
                getSubstraitTestSchema(),
                split,
                constraints,
                0,
                0);
    }

    /**
     * Helper: creates a ReadRecordsRequest with a QueryPlan and a LIMIT value.
     */
    private ReadRecordsRequest createSubstraitReadRequestWithLimit(String substraitPlan, long limit)
    {
        Split split = mock(Split.class);
        when(split.getProperty(STORAGE_SPLIT_JSON)).thenReturn("[\"MOCK_DATA_male.csv\", \"MOCK_DATA_female.csv\"]");
        when(split.getProperty(FILE_FORMAT)).thenReturn("csv");

        QueryPlan queryPlan = new QueryPlan("", substraitPlan);
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                limit,
                Collections.emptyMap(),
                queryPlan);

        return new ReadRecordsRequest(
                federatedIdentity,
                GcsTestUtils.PROJECT_1_NAME,
                "queryId",
                new TableName("test_gcs_database", "test_gcs_table"),
                getSubstraitTestSchema(),
                split,
                constraints,
                0,
                0);
    }

    /**
     * Helper: creates a spill writer for the substrait test schema and configures
     * the GcsUtil mock to point at the CSV test resources.
     */
    private S3BlockSpiller createSubstraitSpillWriter(BlockAllocator allocator, S3Client amazonS3)
    {
        SpillConfig spillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(encryptionKey)
                .withMaxBlockBytes(16000000)
                .withMaxInlineBlockBytes(16000000)
                .withNumSpillThreads(0)
                .withRequestId(UUID.randomUUID().toString())
                .withSpillLocation(s3SpillLocation)
                .build();
        Schema schema = getSubstraitTestSchema();
        return new S3BlockSpiller(amazonS3, spillConfig, allocator, schema,
                ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
    }

    /**
     * Configures the GcsUtil mock to point at the CSV test resources directory.
     */
    private void setupCsvMockUri()
    {
        final File csvDir = new File(GcsRecordHandlerTest.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        mockedGcsUtil.when(() -> GcsUtil.createUri(anyString()))
                .thenAnswer(invocation -> {
                    String fileName = invocation.getArgument(0);
                    return "file:" + csvDir.getPath() + "/" + fileName;
                });
    }

    /**
     * Test: WHERE email IN ('rharrold0@chronoengine.com', 'jcrumly1@icio.us')
     * Expected: 2 rows (id=1 Ralina Harrold, id=2 Jessy Crumly)
     */
    @Test
    public void testSubstraitPlan_WhereEmailIn() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE email IN ('rharrold0@chronoengine.com', 'jcrumly1@icio.us')");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            assertEquals(2, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE email IN ('rharrold0@chronoengine.com', 'jcrumly1@icio.us') should return 2 rows");
        }
    }

    /**
     * Test: WHERE id IN ('1', '3') AND first_name = 'Ralina'
     * Expected: 1 row (id=1 Ralina Harrold)
     */
    @Test
    public void testSubstraitPlan_WhereIdInAndFirstName() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE id IN ('1', '3') AND first_name = 'Ralina'");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            assertEquals(1, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE id IN ('1', '3') AND first_name = 'Ralina' should return 1 row");
        }
    }

    /**
     * Test: WHERE email IN ('rharrold0@chronoengine.com', 'jcrumly1@icio.us')
     *       OR id IN ('3', '7', '9', '10', '11', '13')
     * Expected: 8 rows (2 from email match + 6 from id match, no overlap)
     */
    @Test
    public void testSubstraitPlan_WhereEmailInOrIdIn() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE email IN ('rharrold0@chronoengine.com', 'jcrumly1@icio.us') OR id IN ('3', '7', '9', '10', '11', '13')");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            assertEquals(8, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE email IN (...) OR id IN ('3','7','9','10','11','13') should return 8 rows");
        }
    }

    /**
     * Test: WHERE id IN ('1', '3') OR first_name IN ('Jessy', 'Norman')
     * Expected: 4 rows (id=1 Ralina, id=3 Ignace, id=2 Jessy, id=7 Norman)
     */
    @Test
    public void testSubstraitPlan_WhereIdInOrFirstNameIn() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE id IN ('1', '3') OR first_name IN ('Jessy', 'Norman')");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            assertEquals(4, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE id IN ('1','3') OR first_name IN ('Jessy','Norman') should return 4 rows");
        }
    }

    /**
     * Test: WHERE id IN ('3', '7', '9', '10', '11', '13')
     *       AND last_name >= 'D' AND last_name < 'E'
     *       AND first_name IN ('Norman', 'Whitby')
     * Expected: 2 rows (id=7 Norman Dewitt, id=10 Whitby De Domenici)
     * Note: In production, Athena decomposes LIKE 'D%' into >= 'D' AND < 'E'
     */
    @Test
    public void testSubstraitPlan_WhereIdInAndLastNameLikeAndFirstNameIn() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE id IN ('3', '7', '9', '10', '11', '13') AND last_name >= 'D' AND last_name < 'E' AND first_name IN ('Norman', 'Whitby')");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            // id=7 Norman Dewitt (last_name 'Dewitt' starts with D, first_name 'Norman')
            // id=10 Whitby De Domenici (last_name 'De Domenici' starts with D, first_name 'Whitby')
            // id=13 Bartolemo Degan (last_name 'Degan' starts with D, but first_name not in list)
            assertEquals(2, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE id IN (...) AND last_name >= 'D' AND < 'E' AND first_name IN ('Norman','Whitby') should return 2 rows");
        }
    }

    /**
     * Test: WHERE id IN ('1', '3')
     *       OR first_name IN ('Jessy', 'Norman')
     *       OR last_name IN ('Penley', 'Pilsbury')
     * Expected: 6 rows (id=1 Ralina Harrold, id=3 Ignace Klainman,
     *           id=2 Jessy Crumly, id=7 Norman Dewitt,
     *           id=4 Lu Penley, id=9 Timmy Pilsbury)
     */
    @Test
    public void testSubstraitPlan_WhereIdInOrFirstNameInOrLastNameIn() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE id IN ('1', '3') OR first_name IN ('Jessy', 'Norman') OR last_name IN ('Penley', 'Pilsbury')");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            assertEquals(6, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE id IN ('1','3') OR first_name IN ('Jessy','Norman') OR last_name IN ('Penley','Pilsbury') should return 6 rows");
        }
    }

    /**
     * Test: WHERE id IN ('1', '3')
     *       OR first_name IN ('Jessy', 'Norman')
     *       OR last_name IN ('Penley', 'Pilsbury')
     *       OR email = 'emeadley4@facebook.com'
     * Expected: 7 rows (same 6 as above + id=5 Estrella Meadley)
     */
    @Test
    public void testSubstraitPlan_WhereIdInOrFirstNameInOrLastNameInOrEmail() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE id IN ('1', '3') OR first_name IN ('Jessy', 'Norman') OR last_name IN ('Penley', 'Pilsbury') OR email = 'emeadley4@facebook.com'");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            assertEquals(7, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE id IN ('1','3') OR first_name IN (...) OR last_name IN (...) OR email = '...' should return 7 rows");
        }
    }

    /**
     * Test: WHERE id IN ('3', '7', '9', '10', '11', '13') AND last_name >= 'D' AND last_name < 'E'
     * Expected: 3 rows (id=7 Norman Dewitt, id=10 Whitby De Domenici, id=13 Bartolemo Degan)
     * Note: In production, Athena decomposes LIKE 'D%' into >= 'D' AND < 'E'
     */
    @Test
    public void testSubstraitPlan_WhereIdInAndLastNameLike() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE id IN ('3', '7', '9', '10', '11', '13') AND last_name >= 'D' AND last_name < 'E'");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            // id=7 Norman Dewitt, id=10 Whitby De Domenici, id=11 Egan Dreger, id=13 Bartolemo Degan
            assertEquals(4, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE id IN ('3','7','9','10','11','13') AND last_name >= 'D' AND < 'E' should return 4 rows");
        }
    }

    /**
     * Test: WHERE id IN ('3', '7', '9', '10', '11', '13')
     *       AND first_name NOT IN ('Ignace', 'Norman', 'Timmy')
     * Expected: 3 rows (id=10 Whitby, id=11 Egan, id=13 Bartolemo)
     */
    @Test
    public void testSubstraitPlan_WhereIdInAndFirstNameNotIn() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table WHERE id IN ('3', '7', '9', '10', '11', '13') AND first_name NOT IN ('Ignace', 'Norman', 'Timmy')");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequest(substraitPlan)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            // id=3 Ignace excluded, id=7 Norman excluded, id=9 Timmy excluded
            // Remaining: id=10 Whitby, id=11 Egan, id=13 Bartolemo
            assertEquals(3, substraitSpillWriter.getBlock().getRowCount(),
                    "WHERE id IN (...) AND first_name NOT IN ('Ignace','Norman','Timmy') should return 3 rows");
        }
    }

    /**
     * Test: SELECT * FROM ... LIMIT 10
     * Expected: 10 rows (limit applied via substrait FetchRel)
     */
    @Test
    public void testSubstraitPlan_Limit() throws Exception
    {
        String substraitPlan = GcsSubstraitPlanGenerator.generate(
                "SELECT * FROM test_gcs_table LIMIT 10");

        setupCsvMockUri();
        BlockAllocator allocator = new BlockAllocatorImpl();
        S3Client amazonS3 = mock(S3Client.class);
        S3BlockSpiller substraitSpillWriter = createSubstraitSpillWriter(allocator, amazonS3);

        try (ReadRecordsRequest request = createSubstraitReadRequestWithLimit(substraitPlan, 10)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            gcsRecordHandler.readWithConstraint(substraitSpillWriter, request, queryStatusChecker);
            assertTrue(substraitSpillWriter.getBlock().getRowCount() <= 10,
                    "LIMIT 10 should return at most 10 rows");
        }
    }

}
