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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricStatSerDe.SERIALIZED_METRIC_STATS_FIELD_NAME;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetricsMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsMetadataHandlerTest.class);

    private final String defaultSchema = "default";
    private final FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());

    private MetricsMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private AmazonCloudWatch mockMetrics;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        handler = new MetricsMetadataHandler(mockMetrics, new LocalKeyFactory(), mockSecretsManager, mockAthena, "spillBucket", "spillPrefix", com.google.common.collect.ImmutableMap.of());
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames()
    {
        logger.info("doListSchemas - enter");

        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());

        assertTrue(res.getSchemas().size() == 1);
        assertEquals(defaultSchema, res.getSchemas().iterator().next());

        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");

        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default", defaultSchema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertEquals(2, res.getTables().size());
        assertTrue(res.getTables().contains(new TableName(defaultSchema, "metrics")));
        assertTrue(res.getTables().contains(new TableName(defaultSchema, "metric_samples")));

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetMetricsTable()
    {
        logger.info("doGetMetricsTable - enter");

        GetTableRequest metricsTableReq = new GetTableRequest(identity, "queryId", "default", new TableName(defaultSchema, "metrics"), Collections.emptyMap());
        GetTableResponse metricsTableRes = handler.doGetTable(allocator, metricsTableReq);
        logger.info("doGetMetricsTable - {} {}", metricsTableRes.getTableName(), metricsTableRes.getSchema());

        assertEquals(new TableName(defaultSchema, "metrics"), metricsTableRes.getTableName());
        assertNotNull(metricsTableRes.getSchema());
        assertEquals(6, metricsTableRes.getSchema().getFields().size());

        logger.info("doGetMetricsTable - exit");
    }

    @Test
    public void doGetMetricSamplesTable()
    {
        logger.info("doGetMetricSamplesTable - enter");

        GetTableRequest metricsTableReq = new GetTableRequest(identity,
                "queryId",
                "default",
                new TableName(defaultSchema, "metric_samples"), Collections.emptyMap());

        GetTableResponse metricsTableRes = handler.doGetTable(allocator, metricsTableReq);
        logger.info("doGetMetricSamplesTable - {} {}", metricsTableRes.getTableName(), metricsTableRes.getSchema());

        assertEquals(new TableName(defaultSchema, "metric_samples"), metricsTableRes.getTableName());
        assertNotNull(metricsTableRes.getSchema());
        assertEquals(9, metricsTableRes.getSchema().getFields().size());

        logger.info("doGetMetricSamplesTable - exit");
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(METRIC_NAME_FIELD,
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add("MyMetric").build());

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "queryId",
                "default",
                new TableName(defaultSchema, "metrics"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertTrue(res.getPartitions().getRowCount() == 1);

        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetMetricsSplits()
            throws Exception
    {
        logger.info("doGetMetricsSplits: enter");

        Schema schema = SchemaBuilder.newBuilder().addIntField("partitionId").build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("partitionId"), 1, 1);
        partitions.setRowCount(1);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName(defaultSchema, "metrics"),
                partitions,
                Collections.singletonList("partitionId"),
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                continuationToken);
        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);
            logger.info("doGetMetricsSplits: req[{}]", req);

            MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            logger.info("doGetMetricsSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());
            assertEquals(1, response.getSplits().size());

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertEquals(0, numContinuations);

        logger.info("doGetMetricsSplits: exit");
    }

    @Test
    public void doGetMetricSamplesSplits()
            throws Exception
    {
        logger.info("doGetMetricSamplesSplits: enter");

        String namespaceFilter = "MyNameSpace";
        String statistic = "p90";
        int numMetrics = 10;

        when(mockMetrics.listMetrics(nullable(ListMetricsRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListMetricsRequest request = invocation.getArgument(0, ListMetricsRequest.class);

            //assert that the namespace filter was indeed pushed down
            assertEquals(namespaceFilter, request.getNamespace());
            String nextToken = (request.getNextToken() == null) ? "valid" : null;
            List<Metric> metrics = new ArrayList<>();

            for (int i = 0; i < numMetrics; i++) {
                metrics.add(new Metric().withNamespace(namespaceFilter).withMetricName("metric-" + i));
            }

            return new ListMetricsResult().withNextToken(nextToken).withMetrics(metrics);
        });

        Schema schema = SchemaBuilder.newBuilder().addIntField("partitionId").build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("partitionId"), 1, 1);
        partitions.setRowCount(1);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(NAMESPACE_FIELD,
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add(namespaceFilter).build());
        constraintsMap.put(STATISTIC_FIELD,
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add(statistic).build());

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName(defaultSchema, "metric_samples"),
                partitions,
                Collections.singletonList("partitionId"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                continuationToken);

        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);
            logger.info("doGetMetricSamplesSplits: req[{}]", req);

            MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            logger.info("doGetMetricSamplesSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());
            assertEquals(3, response.getSplits().size());
            for (Split nextSplit : response.getSplits()) {
                assertNotNull(nextSplit.getProperty(SERIALIZED_METRIC_STATS_FIELD_NAME));
            }

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertEquals(1, numContinuations);

        logger.info("doGetMetricSamplesSplits: exit");
    }

    @Test
    public void doGetMetricSamplesSplitsEmptyMetrics()
            throws Exception
    {
        logger.info("doGetMetricSamplesSplitsEmptyMetrics: enter");

        String namespace = "NameSpace";
        String invalidNamespaceFilter = "InvalidNameSpace";
        int numMetrics = 10;

        when(mockMetrics.listMetrics(nullable(ListMetricsRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            List<Metric> metrics = new ArrayList<>();
            for (int i = 0; i < numMetrics; i++) {
                metrics.add(new Metric().withNamespace(namespace).withMetricName("metric-" + i));
            }
            return new ListMetricsResult().withNextToken(null).withMetrics(metrics);
        });

        Schema schema = SchemaBuilder.newBuilder().addIntField("partitionId").build();

        Block partitions = allocator.createBlock(schema);
        BlockUtils.setValue(partitions.getFieldVector("partitionId"), 1, 1);
        partitions.setRowCount(1);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(NAMESPACE_FIELD,
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add(invalidNamespaceFilter).build());

        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName(defaultSchema, "metric_samples"),
                partitions,
                Collections.singletonList("partitionId"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);
        logger.info("doGetMetricSamplesSplitsEmptyMetrics: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;

        assertEquals(0, response.getSplits().size());
        assertEquals(null, response.getContinuationToken());
    }
}
