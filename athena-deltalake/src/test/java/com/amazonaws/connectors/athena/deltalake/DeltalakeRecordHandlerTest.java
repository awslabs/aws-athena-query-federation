/*-
 * #%L
 * athena-example
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.Int96Value;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.junit.*;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.*;

import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_FILE_PROPERTY;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_PARTITION_VALUES_PROPERTY;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.endsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeltalakeRecordHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeRecordHandlerTest.class);

    private DeltalakeRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;
    private AWSSecretsManager awsSecretsManager;
    private AmazonAthena athena;
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

        schemaForRead = SchemaBuilder.newBuilder()
                .addBitField(isValidCol)
                .addStringField(regionCol)
                .addDateDayField(eventDateCol)
                .addIntField("amount")
                .build();

        allocator = new BlockAllocatorImpl();

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", S3_ENDPOINT);
        conf.set("fs.s3a.path.style.access", "true");

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
                conf);

        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        for (int i = 0; i < 2; i++) {
            Map<String, ValueSet> constraintsMap = new HashMap<>();

            ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                    "catalog",
                    "queryId-" + System.currentTimeMillis(),
                    new TableName("test-database-2", "partitioned-table"),
                    schemaForRead,
                    Split.newBuilder(makeSpillLocation(), null)
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

            assertEquals(3, response.getRecords().getRowCount());
            assertEquals(
                    "[is_valid : true], [region : asia], [event_date : 18617], [amount : 100]",
                    BlockUtils.rowToString(response.getRecords(), 0)
            );
            assertEquals(
                    "[is_valid : true], [region : asia], [event_date : 18617], [amount : 200]",
                    BlockUtils.rowToString(response.getRecords(), 1)
            );
            assertEquals(
                    "[is_valid : true], [region : asia], [event_date : 18617], [amount : 350]",
                    BlockUtils.rowToString(response.getRecords(), 2)
            );
        }
    }

    private static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    }

    private SpillLocation makeSpillLocation()
    {
        return S3SpillLocation.newBuilder()
                .withBucket("athena-virtuoso-test")
                .withPrefix("lambda-spill")
                .withQueryId(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
    }
}
