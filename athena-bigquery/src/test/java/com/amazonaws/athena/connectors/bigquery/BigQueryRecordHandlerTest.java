/*-
 * #%L
 * athena-bigquery
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

package com.amazonaws.athena.connectors.bigquery;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
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
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandlerTest.class);

    private String bucket = "bucket";
    private String prefix = "prefix";

    @Mock
    BigQuery bigQuery;

    @Mock
    AWSSecretsManager awsSecretsManager;

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
    private S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder()
        .withBucket(UUID.randomUUID().toString())
        .withSplitId(UUID.randomUUID().toString())
        .withQueryId(UUID.randomUUID().toString())
        .withIsDirectory(true)
        .build();

    @Before
    public void init()
    {
        logger.info("Starting init.");
        MockitoAnnotations.initMocks(this);

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
        bigQueryRecordHandler = new BigQueryRecordHandler(amazonS3, awsSecretsManager, bigQuery);

        logger.info("Completed init.");
    }

    @Test
    public void testReadWithConstraint()
        throws Exception
    {
        try (ReadRecordsRequest request = new ReadRecordsRequest(
            BigQueryTestUtils.FEDERATED_IDENTITY,
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
            when(mockBigQueryJob.waitFor()).thenReturn(mockBigQueryJob);
            when(mockBigQueryJob.getQueryResults()).thenReturn(result);
            when(bigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);

            //Execute the test
            bigQueryRecordHandler.readWithConstraint(spillWriter, request);

            //Ensure that there was a spill so that we can read the spilled block.
            assertTrue(spillWriter.spilled());
            //Calling getSpillLocations() forces a flush.
            assertEquals(1, spillWriter.getSpillLocations().size());

            //Read the spilled block
            Block block = spillReader.read(s3SpillLocation, encryptionKey, schemaForRead);

            assertEquals("The number of rows expected do not match!", tableRows.size(), block.getRowCount());
            validateBlock(block, tableRows);
        }
    }

    private void validateBlock(Block block, List<FieldValueList> tableRows)
    {
        //Iterator through the fields
        for (Field field : block.getFields()) {
            FieldReader fieldReader = block.getFieldReader(field.getName());
            int currentCount = 0;
            //Iterator through the rows and match up with the block
            for (FieldValueList tableRow : tableRows) {
                FieldValue orgValue = tableRow.get(field.getName());
                fieldReader.setPosition(currentCount);
                currentCount++;

                logger.debug("comparing: {} with {}", orgValue.getValue(), fieldReader.readObject());

                //Check for null values.
                if ((orgValue.getValue() == null || fieldReader.readObject() == null)) {
                    assertTrue(orgValue.isNull());
                    assertFalse(fieldReader.isSet());
                    continue;
                }

                //Check regular values.
                Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
                switch (type) {
                    case INT:
                        assertEquals(orgValue.getLongValue(), (long) fieldReader.readInteger());
                        break;
                    case BIT:
                        assertEquals(orgValue.getBooleanValue(), fieldReader.readBoolean());
                        break;
                    case FLOAT4:
                        assertEquals(orgValue.getDoubleValue(), fieldReader.readFloat(), 0.001);
                        break;
                    case FLOAT8:
                        assertEquals(orgValue.getDoubleValue(), fieldReader.readDouble(), 0.001);
                        break;
                    case VARCHAR:
                        assertEquals(orgValue.getStringValue(), fieldReader.readText().toString());
                        break;
                    default:
                        throw new RuntimeException("No validation configured for field " + field.getName() + ":" + type + " " + field.getChildren());
                }
            }
        }
    }

    //Mocks the S3 client by storing any putObjects() and returning the object when getObject() is called.
    private void mockS3Client()
    {
        when(amazonS3.putObject(anyObject(), anyObject(), anyObject(), anyObject()))
            .thenAnswer((InvocationOnMock invocationOnMock) -> {
                InputStream inputStream = (InputStream) invocationOnMock.getArguments()[2];
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
