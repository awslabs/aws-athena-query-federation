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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.*;
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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
public class BigQueryRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandlerTest.class);

    private String bucket = "bucket";
    private String prefix = "prefix";

    @Mock
    BigQuery bigQuery;

    @Mock
    AWSSecretsManager awsSecretsManager;

    @Mock
    private AmazonAthena athena;

    private BigQueryRecordHandler bigQueryRecordHandler;

    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private AmazonS3 amazonS3;
    private S3BlockSpiller spillWriter;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private EncryptionKey encryptionKey = keyFactory.create();
    private SpillConfig spillConfig;
    private String queryId = UUID.randomUUID().toString();
    private S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(queryId)
            .withIsDirectory(true)
            .build();
    private FederatedIdentity federatedIdentity;

    @Before
    public void init()
    {
        System.setProperty("aws.region", "us-east-1");
        logger.info("Starting init.");
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        //MockitoAnnotations.initMocks(this);

        allocator = new BlockAllocatorImpl();
        amazonS3 = mock(AmazonS3.class);

        mockS3Client();

        //Create Spill config
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

        schemaForRead = new Schema(BigQueryTestUtils.getTestSchemaFieldsArrow());
        spillWriter = new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead, ConstraintEvaluator.emptyEvaluator());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        //Mock the BigQuery Client to return Datasets, and Table Schema information.
        BigQueryPage<Dataset> datasets = new BigQueryPage<Dataset>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, 2));
        when(bigQuery.listDatasets(any(String.class))).thenReturn(datasets);
        BigQueryPage<Table> tables = new BigQueryPage<Table>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME, "dataset1", 2));
        when(bigQuery.listTables(any(DatasetId.class))).thenReturn(tables);

        //The class we want to test.
        bigQueryRecordHandler = new BigQueryRecordHandler(amazonS3, awsSecretsManager, athena, bigQuery);

        logger.info("Completed init.");
    }

    @Test
    public void testReadWithConstraint()
            throws Exception
    {
        try (ReadRecordsRequest request = new ReadRecordsRequest(
                federatedIdentity,
                BigQueryTestUtils.PROJECT_1_NAME,
                "queryId",
                new TableName("dataset1", "table1"),
                BigQueryTestUtils.getBlockTestSchema(),
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket(bucket)
                                .withPrefix(prefix)
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).build(),
                new Constraints(Collections.EMPTY_MAP),
                0,          //This is ignored when directly calling readWithConstraints.
                0)) {   //This is ignored when directly calling readWithConstraints.
            //Always return try for the evaluator to keep all rows.
            ConstraintEvaluator evaluator = mock(ConstraintEvaluator.class);
            when(evaluator.apply(any(String.class), any(Object.class))).thenAnswer(
                    (InvocationOnMock invocationOnMock) -> {
                        return true;
                    }
            );

            //Populate the schema and data that the mocked Google BigQuery client will return.
            com.google.cloud.bigquery.Schema tableSchema = BigQueryTestUtils.getTestSchema();
            List<FieldValueList> tableRows = Arrays.asList(
                    BigQueryTestUtils.getBigQueryFieldValueList(false, 1000, "test1", 123123.12312),
                    BigQueryTestUtils.getBigQueryFieldValueList(true, 500, "test2", 5345234.22111),
                    BigQueryTestUtils.getBigQueryFieldValueList(false, 700, "test3", 324324.23423),
                    BigQueryTestUtils.getBigQueryFieldValueList(true, 900, null, null),
                    BigQueryTestUtils.getBigQueryFieldValueList(null, null, "test5", 2342.234234),
                    BigQueryTestUtils.getBigQueryFieldValueList(true, 1200, "test6", 1123.12312),
                    BigQueryTestUtils.getBigQueryFieldValueList(false, 100, "test7", 1313.12312),
                    BigQueryTestUtils.getBigQueryFieldValueList(true, 120, "test8", 12313.1312),
                    BigQueryTestUtils.getBigQueryFieldValueList(false, 300, "test9", 12323.1312)
            );
            Page<FieldValueList> fieldValueList = new BigQueryPage<>(tableRows);
            TableResult result = new TableResult(tableSchema, tableRows.size(), fieldValueList);

            //Mock out the Google BigQuery Job.
            Job mockBigQueryJob = mock(Job.class);
            when(mockBigQueryJob.isDone()).thenReturn(false).thenReturn(true);
            when(mockBigQueryJob.getQueryResults()).thenReturn(result);
            when(bigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            //Execute the test
            bigQueryRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
            PowerMockito.mockStatic(System.class);
            PowerMockito.when(System.getenv(anyString())).thenReturn("test");
            logger.info("Project Name: "+BigQueryUtils.getProjectName(request.getCatalogName()));

            //Ensure that there was a spill so that we can read the spilled block.
            assertTrue(spillWriter.spilled());
        }
    }

    @Test
    public void getObjectFromFieldValue()
            throws Exception
    {
        org.apache.arrow.vector.types.pojo.Schema testSchema = SchemaBuilder.newBuilder()
                .addDateDayField("datecol")
                .addDateMilliField("datetimecol")
                .addStringField("timestampcol")
                .build();

        try (ReadRecordsRequest request = new ReadRecordsRequest(
                federatedIdentity,
                BigQueryTestUtils.PROJECT_1_NAME,
                "queryId",
                new TableName("dataset1", "table1"),
                testSchema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket(bucket)
                                .withPrefix(prefix)
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).build(),
                new Constraints(Collections.EMPTY_MAP),
                0,          //This is ignored when directly calling readWithConstraints.
                0)) {   //This is ignored when directly calling readWithConstraints.
            //Always return try for the evaluator to keep all rows.
            ConstraintEvaluator evaluator = mock(ConstraintEvaluator.class);
            when(evaluator.apply(any(String.class), any(Object.class))).thenAnswer(
                    (InvocationOnMock invocationOnMock) -> {
                        return true;
                    }
            );

            // added schema with columns datecol, datetimecol, timestampcol
            List<com.google.cloud.bigquery.Field> testSchemaFields = Arrays.asList(com.google.cloud.bigquery.Field.of("datecol", LegacySQLTypeName.DATE),
                    com.google.cloud.bigquery.Field.of("datetimecol", LegacySQLTypeName.DATETIME),
                    com.google.cloud.bigquery.Field.of("timestampcol", LegacySQLTypeName.TIMESTAMP));
            com.google.cloud.bigquery.Schema tableSchema = com.google.cloud.bigquery.Schema.of(testSchemaFields);

            // mocked table rows
            List<FieldValue> firstRowValues = Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2016-02-05"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2021-10-30T10:10:10"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2014-12-03T12:30:00.450Z"));
            FieldValueList firstRow = FieldValueList.of(firstRowValues,FieldList.of(testSchemaFields));
            List<FieldValueList> tableRows = Arrays.asList(firstRow);

            Page<FieldValueList> fieldValueList = new BigQueryPage<>(tableRows);
            TableResult result = new TableResult(tableSchema, tableRows.size(), fieldValueList);

            //Mock out the Google BigQuery Job.
            Job mockBigQueryJob = mock(Job.class);
            when(mockBigQueryJob.isDone()).thenReturn(false).thenReturn(true);
            when(mockBigQueryJob.getQueryResults()).thenReturn(result);
            when(bigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            //Execute the test
            bigQueryRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
            PowerMockito.mockStatic(System.class);
            PowerMockito.when(System.getenv(anyString())).thenReturn("test");
            logger.info("Project Name: "+BigQueryUtils.getProjectName(request.getCatalogName()));

        }
    }
    //Mocks the S3 client by storing any putObjects() and returning the object when getObject() is called.
    private void mockS3Client()
    {
        when(amazonS3.putObject(anyObject()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    mockS3Storage.add(byteHolder);
                    return mock(PutObjectResult.class);
                });

        when(amazonS3.getObject(anyString(), anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder = mockS3Storage.get(0);
                    mockS3Storage.remove(0);
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolder.getBytes()), null));
                    return mockObject;
                });
    }

    private class ByteHolder
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
