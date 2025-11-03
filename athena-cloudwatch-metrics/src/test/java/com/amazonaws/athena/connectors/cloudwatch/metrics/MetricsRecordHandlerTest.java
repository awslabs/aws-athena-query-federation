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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.cloudwatch.metrics.MetricDataQuerySerDe;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricSamplesTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricsTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table;
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
import static org.mockito.Mockito.mock;
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
            throws Exception
    {
        mockS3Storage = new ArrayList<>();
        allocator = new BlockAllocatorImpl();
        handler = new MetricsRecordHandler(mockS3, mockSecretsManager, mockAthena, mockMetrics, com.google.common.collect.ImmutableMap.of());
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
            throws Exception
    {
        allocator.close();
    }

    @Test
    public void readMetricsWithConstraint()
            throws Exception
    {
        logger.info("readMetricsWithConstraint: enter");

        String namespace = "namespace";
        String dimName = "dimName";
        String dimValue = "dimValye";

        int numMetrics = 100;
        AtomicLong numCalls = new AtomicLong(0);
        when(mockMetrics.listMetrics(nullable(ListMetricsRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListMetricsRequest request = invocation.getArgument(0, ListMetricsRequest.class);
            numCalls.incrementAndGet();
            //assert that the namespace filter was indeed pushed down
            assertEquals(namespace, request.namespace());
            String nextToken = (request.nextToken() == null) ? "valid" : null;
            List<Metric> metrics = new ArrayList<>();

            for (int i = 0; i < numMetrics; i++) {
                metrics.add(Metric.builder()
                        .namespace(namespace)
                        .metricName("metric-" + i)
                        .dimensions(Dimension.builder()
                                .name(dimName)
                                .value(dimValue)
                                .build())
                        .build());
                metrics.add(Metric.builder().namespace(namespace + i).metricName("metric-" + i).build());
            }

            return ListMetricsResponse.builder().nextToken(nextToken).metrics(metrics).build();
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(NAMESPACE_FIELD, makeStringEquals(allocator, namespace));
        constraintsMap.put(DIMENSION_NAME_FIELD, makeStringEquals(allocator, dimName));
        constraintsMap.put(DIMENSION_VALUE_FIELD, makeStringEquals(allocator, dimValue));

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

        String namespace = "namespace";
        String metricName = "metricName";
        String statistic = "p90";
        String period = "60";
        String dimName = "dimName";
        String dimValue = "dimValue";
        List<Dimension> dimensions = Collections.singletonList(Dimension.builder().name(dimName).value(dimValue).build());

        int numMetrics = 10;
        int numSamples = 10;
        AtomicLong numCalls = new AtomicLong(0);
        when(mockMetrics.getMetricData(nullable(GetMetricDataRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            numCalls.incrementAndGet();
            return mockMetricData(invocation, numMetrics, numSamples);
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(NAMESPACE_FIELD, makeStringEquals(allocator, namespace));
        constraintsMap.put(STATISTIC_FIELD, makeStringEquals(allocator, statistic));
        constraintsMap.put(DIMENSION_NAME_FIELD, makeStringEquals(allocator, dimName));
        constraintsMap.put(DIMENSION_VALUE_FIELD, makeStringEquals(allocator, dimValue));

        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        List<MetricDataQuery> metricDataQueries = new ArrayList<>();
        metricDataQueries.add(MetricDataQuery.builder()
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(namespace)
                                .metricName(metricName)
                                .dimensions(dimensions)
                                .build())
                        .period(60)
                        .stat(statistic)
                        .build())
                .id("m1")
                .build());

        Split split = Split.newBuilder(spillLocation, keyFactory.create())
                .add(MetricDataQuerySerDe.SERIALIZED_METRIC_DATA_QUERIES_FIELD_NAME, MetricDataQuerySerDe.serialize(metricDataQueries))
                .add(METRIC_NAME_FIELD, metricName)
                .add(NAMESPACE_FIELD, namespace)
                .add(STATISTIC_FIELD, statistic)
                .add(PERIOD_FIELD, period)
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                "catalog",
                "queryId-" + System.currentTimeMillis(),
                METRIC_SAMPLES_TABLE_NAME,
                METRIC_DATA_TABLE.getSchema(),
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L//100GB don't expect this to spill
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readMetricSamplesWithConstraint: rows[{}]", response.getRecordCount());

        assertEquals(numCalls.get() * numMetrics * numSamples, response.getRecords().getRowCount());
        logger.info("readMetricSamplesWithConstraint: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("readMetricSamplesWithConstraint: exit");
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
