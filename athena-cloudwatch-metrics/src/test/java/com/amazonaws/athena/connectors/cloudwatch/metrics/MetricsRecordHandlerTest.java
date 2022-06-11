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
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricSamplesTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.MetricsTable;
import com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricDataResult;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.cloudwatch.model.MetricDataQuery;
import com.amazonaws.services.cloudwatch.model.MetricDataResult;
import com.amazonaws.services.cloudwatch.model.MetricStat;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
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

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private List<ByteHolder> mockS3Storage;
    private MetricsRecordHandler handler;
    private S3BlockSpillReader spillReader;
    private BlockAllocator allocator;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Mock
    private AmazonCloudWatch mockMetrics;

    @Mock
    private AmazonS3 mockS3;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        mockS3Storage = new ArrayList<>();
        allocator = new BlockAllocatorImpl();
        handler = new MetricsRecordHandler(mockS3, mockSecretsManager, mockAthena, mockMetrics);
        spillReader = new S3BlockSpillReader(mockS3, allocator);

        when(mockS3.putObject(anyObject()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return mock(PutObjectResult.class);
                });

        when(mockS3.getObject(anyString(), anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolder.getBytes()), null));
                    return mockObject;
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
        when(mockMetrics.listMetrics(any(ListMetricsRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListMetricsRequest request = invocation.getArgumentAt(0, ListMetricsRequest.class);
            numCalls.incrementAndGet();
            //assert that the namespace filter was indeed pushed down
            assertEquals(namespace, request.getNamespace());
            String nextToken = (request.getNextToken() == null) ? "valid" : null;
            List<Metric> metrics = new ArrayList<>();

            for (int i = 0; i < numMetrics; i++) {
                metrics.add(new Metric().withNamespace(namespace).withMetricName("metric-" + i)
                        .withDimensions(new Dimension().withName(dimName).withValue(dimValue)));
                metrics.add(new Metric().withNamespace(namespace + i).withMetricName("metric-" + i));
            }

            return new ListMetricsResult().withNextToken(nextToken).withMetrics(metrics);
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
                new Constraints(constraintsMap),
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
        List<Dimension> dimensions = Collections.singletonList(new Dimension().withName(dimName).withValue(dimValue));

        int numMetrics = 10;
        int numSamples = 10;
        AtomicLong numCalls = new AtomicLong(0);
        when(mockMetrics.getMetricData(any(GetMetricDataRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
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

        List<MetricStat> metricStats = new ArrayList<>();
        metricStats.add(new MetricStat()
                .withMetric(new Metric()
                        .withNamespace(namespace)
                        .withMetricName(metricName)
                        .withDimensions(dimensions))
                .withPeriod(60)
                .withStat(statistic));

        Split split = Split.newBuilder(spillLocation, keyFactory.create())
                .add(MetricStatSerDe.SERIALIZED_METRIC_STATS_FIELD_NAME, MetricStatSerDe.serialize(metricStats))
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
                new Constraints(constraintsMap),
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

    private GetMetricDataResult mockMetricData(InvocationOnMock invocation, int numMetrics, int numSamples)
    {
        GetMetricDataRequest request = invocation.getArgumentAt(0, GetMetricDataRequest.class);

        /**
         * Confirm that all available criteria were pushed down into Cloudwatch Metrics
         */
        List<MetricDataQuery> queries = request.getMetricDataQueries();
        assertEquals(1, queries.size());
        MetricDataQuery query = queries.get(0);
        MetricStat stat = query.getMetricStat();
        assertEquals("m1", query.getId());
        assertNotNull(stat.getPeriod());
        assertNotNull(stat.getMetric());
        assertNotNull(stat.getStat());
        assertNotNull(stat.getMetric().getMetricName());
        assertNotNull(stat.getMetric().getNamespace());
        assertNotNull(stat.getMetric().getDimensions());
        assertEquals(1, stat.getMetric().getDimensions().size());

        String nextToken = (request.getNextToken() == null) ? "valid" : null;
        List<MetricDataResult> samples = new ArrayList<>();

        for (int i = 0; i < numMetrics; i++) {
            List<Double> values = new ArrayList<>();
            List<Date> timestamps = new ArrayList<>();
            for (double j = 0; j < numSamples; j++) {
                values.add(j);
                timestamps.add(new Date(System.currentTimeMillis() + (int) j));
            }
            samples.add(new MetricDataResult().withValues(values).withTimestamps(timestamps).withId("m1"));
        }

        return new GetMetricDataResult().withNextToken(nextToken).withMetricDataResults(samples);
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
