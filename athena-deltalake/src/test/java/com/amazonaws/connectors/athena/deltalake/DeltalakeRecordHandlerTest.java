/*-
 * #%L
 * athena-deltalake
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_FILE_PROPERTY;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_PARTITION_VALUES_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DeltalakeRecordHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeRecordHandlerTest.class);

    private DeltalakeRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;
    private S3BlockSpillReader spillReader;

    @Rule
    public TestName testName = new TestName();

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        String isValidCol = "is_valid";
        String regionCol = "region";
        String eventDateCol = "event_date";
        String dataBucket = "test-bucket-1";

        schemaForRead = SchemaBuilder.newBuilder()
            .addBitField(isValidCol)
            .addStringField(regionCol)
            .addDateDayField(eventDateCol)
            .addIntField("amount")
            .addDecimalField("amount_decimal", 10, 2)
            .build();

        allocator = new BlockAllocatorImpl();

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", S3_ENDPOINT);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.access.key", "NO_NEED");
        conf.set("fs.s3a.secret.key", "NO_NEED");

        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, S3_REGION);
        amazonS3 = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        this.handler = new DeltalakeRecordHandler(
            amazonS3,
            mock(AWSSecretsManager.class),
            mock(AmazonAthena.class),
            conf,
            dataBucket);

        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        String catalogName = "catalog";
        for (int i = 0; i < 2; i++) {
            Map<String, ValueSet> constraintsMap = new HashMap<>();
            String queryId = "queryId-" + System.currentTimeMillis();

            ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                catalogName,
                queryId,
                new TableName("test-database-2", "partitioned-table"),
                schemaForRead,
                Split.newBuilder(makeSpillLocation(queryId, "1234"), null)
                    .add(SPLIT_PARTITION_VALUES_PROPERTY, "{\"is_valid\":\"true\",\"region\":\"asia\",\"event_date\":\"2020-12-21\"}")
                    .add(SPLIT_FILE_PROPERTY, "is_valid=true/region=asia/event_date=2020-12-21/part-00000-58828e3c-041e-47b4-80dd-196ae1b1d1a6-c000.snappy.parquet")
                    .build(),
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
            );

            RecordResponse rawResponse = handler.doReadRecords(allocator, request);
            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

            Block expectedBlock = allocator.createBlock(schemaForRead);
            BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 0,  true);
            BlockUtils.setValue(expectedBlock.getFieldVector("region"), 0,  "asia");
            BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 0,  LocalDate.of(2020, 12, 21));
            BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 0, 100);
            BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 0, null);

            BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 0,  true);
            BlockUtils.setValue(expectedBlock.getFieldVector("region"), 0,  "asia");
            BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 0,  LocalDate.of(2020, 12, 21));
            BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 0, 100);
            BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 0, null);

            BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 1,  true);
            BlockUtils.setValue(expectedBlock.getFieldVector("region"), 1,  "asia");
            BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 1,  LocalDate.of(2020, 12, 21));
            BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 1, 200);
            BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 1, null);

            BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 2,  true);
            BlockUtils.setValue(expectedBlock.getFieldVector("region"), 2,  "asia");
            BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 2,  LocalDate.of(2020, 12, 21));
            BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 2, 350);
            BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 2, null);

            expectedBlock.setRowCount(3);

            ReadRecordsResponse expectedResponse = new ReadRecordsResponse(catalogName, expectedBlock);

            assertEquals(3, response.getRecords().getRowCount());
            assertEquals(expectedResponse, response);
        }
    }
}
