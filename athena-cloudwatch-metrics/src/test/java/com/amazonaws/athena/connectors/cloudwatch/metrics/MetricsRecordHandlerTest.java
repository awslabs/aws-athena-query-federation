/*-
 * #%L
 * athena-cloudwatch-metrics
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
package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricSamplesTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricsTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.athena.connectors.cloudwatch.metrics.TestUtils.makeStringEquals;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.DIMENSION_VALUE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.PERIOD_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetricsRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsRecordHandlerTest.class);
    //Schema for the metrics table.
    private static final Table METRIC_TABLE = new MetricsTable();
    //Schema for the metric_samples table.
    private static final Table METRIC_DATA_TABLE = new MetricSamplesTable();
    private static final TableName METRICS_TABLE_NAME = new TableName("default", METRIC_TABLE.getName());
    private static final TableName METRIC_SAMPLES_TABLE_NAME = new TableName("default", METRIC_DATA_TABLE.getName());
    private static final String TEST_NAMESPACE = "namespace";
    private static final String TEST_METRIC_NAME = "metricName";
    private static final String TEST_STATISTIC = "p90";
    private static final String TEST_PERIOD = "60";
    private static final String TEST_DIMENSION_NAME = "dimName";
    private static final String TEST_DIMENSION_VALUE = "dimValue";

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private List<ByteHolder> mockS3Storage;
    private MetricsRecordHandler handler;
    private S3BlockSpillReader spillReader;
    private BlockAllocator allocator;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Mock
    private CloudWatchClient mockMetrics;

    @Mock
    private S3Client mockS3;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
    {
        mockS3Storage = new ArrayList<>();
        allocator = new BlockAllocatorImpl();
        handler = new MetricsRecordHandler(mockS3, mockSecretsManager, mockAthena, mockMetrics, ImmutableMap.of());
        spillReader = new S3BlockSpillReader(mockS3, allocator);

        Mockito.lenient().when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        Mockito.lenient().when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void readMetricsWithConstraint()
            throws Exception
    {
        logger.info("readMetricsWithConstraint: enter");

        int numMetrics = 100;
        AtomicLong numCalls = new AtomicLong(0);
        when(mockMetrics.listMetrics(nullable(ListMetricsRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListMetricsRequest request = invocation.getArgument(0, ListMetricsRequest.class);
            numCalls.incrementAndGet();
            //assert that the namespace filter was indeed pushed down
            assertEquals(TEST_NAMESPACE, request.namespace());
            String nextToken = (request.nextToken() == null) ? "valid" : null;
            List<Metric> metrics = new ArrayList<>();

            for (int i = 0; i < numMetrics; i++) {
                metrics.add(Metric.builder()
                        .namespace(TEST_NAMESPACE)
                        .metricName("metric-" + i)
                        .dimensions(Dimension.builder()
                                .name(TEST_DIMENSION_NAME)
                                .value(TEST_DIMENSION_VALUE)
                                .build())
                        .build());
                metrics.add(Metric.builder().namespace(TEST_NAMESPACE + i).metricName("metric-" + i).build());
            }

            return ListMetricsResponse.builder().nextToken(nextToken).metrics(metrics).build();
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(NAMESPACE_FIELD, makeStringEquals(allocator, TEST_NAMESPACE));
        constraintsMap.put(DIMENSION_NAME_FIELD, makeStringEquals(allocator, TEST_DIMENSION_NAME));
        constraintsMap.put(DIMENSION_VALUE_FIELD, makeStringEquals(allocator, TEST_DIMENSION_VALUE));

        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(spillLocation, keyFactory.create()).build();

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                "catalog",
                "queryId-" + System.currentTimeMillis(),
                METRICS_TABLE_NAME,
                METRIC_TABLE.getSchema(),
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L//100GB don't expect this to spill
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readMetricsWithConstraint: rows[{}]", response.getRecordCount());

        assertEquals(numCalls.get() * numMetrics, response.getRecords().getRowCount());
        logger.info("readMetricsWithConstraint: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("readMetricsWithConstraint: exit");
    }

    @Test
    public void readMetricSamplesWithConstraint()
            throws Exception
    {
        logger.info("readMetricSamplesWithConstraint: enter");

        int numMetrics = 10;
        int numSamples = 10;
        AtomicLong numCalls = new AtomicLong(0);
        when(mockMetrics.getMetricData(nullable(GetMetricDataRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            numCalls.incrementAndGet();
            return mockMetricData(invocation, numMetrics, numSamples);
        });

        // 100GB max block size, so no spill is expected
        ReadRecordsRequest request = makeMetricSamplesRequest(100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readMetricSamplesWithConstraint: rows[{}]", response.getRecordCount());

        assertEquals(numCalls.get() * numMetrics * numSamples, response.getRecords().getRowCount());
        logger.info("readMetricSamplesWithConstraint: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("readMetricSamplesWithConstraint: exit");
    }

    @Test
    public void readMetricSamplesRetriesWhenCloudWatchThrottles()
            throws Exception
    {
        logger.info("readMetricSamplesRetriesWhenCloudWatchThrottles: enter");

        int numMetrics = 10;
        int numSamples = 10;
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicLong successfulCalls = new AtomicLong(0);
        when(mockMetrics.getMetricData(nullable(GetMetricDataRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            if (attempts.getAndIncrement() == 0) {
                throw CloudWatchException.builder()
                        .message("Request has been throttled. (Service: CloudWatch, Status Code: 400)")
                        .statusCode(400)
                        .build();
            }
            successfulCalls.incrementAndGet();
            return mockMetricData(invocation, numMetrics, numSamples);
        });

        ReadRecordsRequest request = makeMetricSamplesRequest(100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertEquals(successfulCalls.get() * numMetrics * numSamples, response.getRecords().getRowCount());
        assertTrue(attempts.get() > successfulCalls.get());

        logger.info("readMetricSamplesRetriesWhenCloudWatchThrottles: exit");
    }

    @Test
    public void readMetricSamplesRetriesThrottleAfterRowsSpilled()
            throws Exception
    {
        logger.info("readMetricSamplesRetriesThrottleAfterRowsSpilled: enter");

        int numMetrics = 10;
        int numSamples = 10;
        // Call 0: first page succeeds. Call 1: second page is throttled. Call 2: retry succeeds.
        AtomicInteger attempts = new AtomicInteger(0);
        when(mockMetrics.getMetricData(nullable(GetMetricDataRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            if (attempts.getAndIncrement() == 1) {
                throw CloudWatchException.builder()
                        .message("Request has been throttled. (Service: CloudWatch, Status Code: 400)")
                        .statusCode(400)
                        .build();
            }
            return mockMetricData(invocation, numMetrics, numSamples);
        });

        // A max block size of 1 byte makes every written row spill, so the first page spills before the second page is requested.
        ReadRecordsRequest request = makeMetricSamplesRequest(1L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        // Rows spilled, so the response must be remote. Page 2 was throttled once and then retried successfully.
        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);
        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            assertTrue(response.getNumberBlocks() > 0);
        }
        assertEquals(3, attempts.get());

        logger.info("readMetricSamplesRetriesThrottleAfterRowsSpilled: exit");
    }

    /**
     * Builds a metric_samples read request for the shared test metric. The inline block size is 100GB, so only
     * the max block size decides whether rows spill.
     */
    private ReadRecordsRequest makeMetricSamplesRequest(long maxBlockSize)
    {
        return new ReadRecordsRequest(identity,
                "catalog",
                "queryId-" + System.currentTimeMillis(),
                METRIC_SAMPLES_TABLE_NAME,
                METRIC_DATA_TABLE.getSchema(),
                makeMetricSamplesSplit(),
                new Constraints(makeMetricSamplesConstraints(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                maxBlockSize,
                100_000_000_000L);
    }

    private Map<String, ValueSet> makeMetricSamplesConstraints()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(NAMESPACE_FIELD, makeStringEquals(allocator, TEST_NAMESPACE));
        constraintsMap.put(STATISTIC_FIELD, makeStringEquals(allocator, TEST_STATISTIC));
        constraintsMap.put(DIMENSION_NAME_FIELD, makeStringEquals(allocator, TEST_DIMENSION_NAME));
        constraintsMap.put(DIMENSION_VALUE_FIELD, makeStringEquals(allocator, TEST_DIMENSION_VALUE));
        return constraintsMap;
    }

    private Split makeMetricSamplesSplit()
    {
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        List<Dimension> dimensions = Collections.singletonList(Dimension.builder().name(TEST_DIMENSION_NAME).value(TEST_DIMENSION_VALUE).build());
        List<MetricDataQuery> metricDataQueries = new ArrayList<>();
        metricDataQueries.add(MetricDataQuery.builder()
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(TEST_NAMESPACE)
                                .metricName(TEST_METRIC_NAME)
                                .dimensions(dimensions)
                                .build())
                        .period(60)
                        .stat(TEST_STATISTIC)
                        .build())
                .id("m1")
                .build());

        return Split.newBuilder(spillLocation, keyFactory.create())
                .add(MetricDataQuerySerDe.SERIALIZED_METRIC_DATA_QUERIES_FIELD_NAME, MetricDataQuerySerDe.serialize(metricDataQueries))
                .add(METRIC_NAME_FIELD, TEST_METRIC_NAME)
                .add(NAMESPACE_FIELD, TEST_NAMESPACE)
                .add(STATISTIC_FIELD, TEST_STATISTIC)
                .add(PERIOD_FIELD, TEST_PERIOD)
                .build();
    }

    private GetMetricDataResponse mockMetricData(InvocationOnMock invocation, int numMetrics, int numSamples)
    {
        GetMetricDataRequest request = invocation.getArgument(0, GetMetricDataRequest.class);

        /**
         * Confirm that all available criteria were pushed down into Cloudwatch Metrics
         */
        List<MetricDataQuery> queries = request.metricDataQueries();
        assertEquals(1, queries.size());
        MetricDataQuery query = queries.get(0);
        MetricStat stat = query.metricStat();
        assertEquals("m1", query.id());
        assertNotNull(stat.period());
        assertNotNull(stat.metric());
        assertNotNull(stat.stat());
        assertNotNull(stat.metric().metricName());
        assertNotNull(stat.metric().namespace());
        assertNotNull(stat.metric().dimensions());
        assertEquals(1, stat.metric().dimensions().size());

        String nextToken = (request.nextToken() == null) ? "valid" : null;
        List<MetricDataResult> samples = new ArrayList<>();

        for (int i = 0; i < numMetrics; i++) {
            List<Double> values = new ArrayList<>();
            List<Instant> timestamps = new ArrayList<>();
            for (double j = 0; j < numSamples; j++) {
                values.add(j);
                timestamps.add(new Date(System.currentTimeMillis() + (int) j).toInstant());
            }
            samples.add(MetricDataResult.builder().values(values).timestamps(timestamps).id("m1").build());
        }

        return GetMetricDataResponse.builder().nextToken(nextToken).metricDataResults(samples).build();
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
