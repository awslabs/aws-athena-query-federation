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
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.base.Strings;

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
import static com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.MetricStatSerDe.SERIALIZED_METRIC_STATS_FIELD_NAME;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.METRIC_NAME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.NAMESPACE_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.metrics.tables.Table.STATISTIC_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetricsMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetricsMetadataHandlerTest.class);

    private final String defaultSchema = "default";
    private final FederatedIdentity identity = FederatedIdentity.newBuilder().setArn("arn").setAccount("account").build();

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

        ListSchemasRequest req = ListSchemasRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default").build();
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemasList());

        assertTrue(res.getSchemasList().size() == 1);
        assertEquals(defaultSchema, res.getSchemasList().iterator().next());

        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");

        ListTablesRequest req = ListTablesRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default").setSchemaName(defaultSchema).setPageSize(UNLIMITED_PAGE_SIZE_VALUE).build();
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTablesList());

        assertEquals(2, res.getTablesList().size());
        assertTrue(res.getTablesList().contains(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metrics").build()));
        assertTrue(res.getTablesList().contains(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metric_samples").build()));

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetMetricsTable()
    {
        logger.info("doGetMetricsTable - enter");

        GetTableRequest metricsTableReq = GetTableRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default").setTableName(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metrics")).build();
        GetTableResponse metricsTableRes = handler.doGetTable(allocator, metricsTableReq);
        logger.info("doGetMetricsTable - {} {}", metricsTableRes.getTableName(), metricsTableRes.getSchema());

        assertEquals(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metrics").build(), metricsTableRes.getTableName());
        assertNotNull(metricsTableRes.getSchema());
        assertEquals(6, ProtobufMessageConverter.fromProtoSchema(allocator, metricsTableRes.getSchema()).getFields().size());

        logger.info("doGetMetricsTable - exit");
    }

    @Test
    public void doGetMetricSamplesTable()
    {
        logger.info("doGetMetricSamplesTable - enter");

        GetTableRequest metricsTableReq = GetTableRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default").setTableName(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metric_samples")).build();
        GetTableResponse metricsTableRes = handler.doGetTable(allocator, metricsTableReq);
        logger.info("doGetMetricSamplesTable - {} {}", metricsTableRes.getTableName(), metricsTableRes.getSchema());

        assertEquals(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metric_samples").build(), metricsTableRes.getTableName());
        assertNotNull(metricsTableRes.getSchema());
        assertEquals(9, ProtobufMessageConverter.fromProtoSchema(allocator, metricsTableRes.getSchema()).getFields().size());

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

        GetTableLayoutRequest req = GetTableLayoutRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default")
            .setTableName(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metrics").build()).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
            .build();
        
        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertTrue(ProtobufMessageConverter.fromProtoBlock(allocator, res.getPartitions()).getRowCount() == 1);

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
        GetSplitsRequest originalReq = GetSplitsRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("catalog_name")
            .setTableName(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metrics").build())
            .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
            .addPartitionCols("partitionId")
            .build();
        int numContinuations = 0;
        do {
            GetSplitsRequest.Builder reqBuilder = originalReq.toBuilder();
            if (!Strings.isNullOrEmpty(continuationToken))
            {
                reqBuilder.setContinuationToken(continuationToken);
            }
            GetSplitsRequest req = reqBuilder.build();
            logger.info("doGetMetricsSplits: req[{}]", req);

            GetSplitsResponse response = handler.doGetSplits(allocator, req);
            continuationToken = response.getContinuationToken();

            logger.info("doGetMetricsSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplitsList().size());
            assertEquals(1, response.getSplitsList().size());

            if (!Strings.isNullOrEmpty(continuationToken)) {
                numContinuations++;
            }
        }
        while (!Strings.isNullOrEmpty(continuationToken));

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

        GetSplitsRequest originalReq = GetSplitsRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("catalog_name")
            .setTableName(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metric_samples").build())
            .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
            .addPartitionCols("partitionId")
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
            .build();

        int numContinuations = 0;
        do {
            GetSplitsRequest.Builder reqBuilder = originalReq.toBuilder();
            if (!Strings.isNullOrEmpty(continuationToken))
            {
                reqBuilder.setContinuationToken(continuationToken);
            }
            GetSplitsRequest req = reqBuilder.build();
            logger.info("doGetMetricSamplesSplits: req[{}]", req);

            GetSplitsResponse response = handler.doGetSplits(allocator, req);
            
            continuationToken = response.getContinuationToken();

            logger.info("doGetMetricSamplesSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplitsList().size());
            assertEquals(3, response.getSplitsList().size());
            for (Split nextSplit : response.getSplitsList()) {
                assertNotNull(nextSplit.getPropertiesMap().get(SERIALIZED_METRIC_STATS_FIELD_NAME));
            }

            if (!Strings.isNullOrEmpty(continuationToken)) {
                numContinuations++;
            }
        }
        while (!Strings.isNullOrEmpty(continuationToken));

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


        GetSplitsRequest req = GetSplitsRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("catalog_name")
            .setTableName(TableName.newBuilder().setSchemaName(defaultSchema).setTableName("metric_samples").build())
            .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
            .addPartitionCols("partitionId")
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
            .build();
        logger.info("doGetMetricSamplesSplitsEmptyMetrics: req[{}]", req);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);
        
        assertEquals(0, response.getSplitsList().size());
        assertFalse(response.hasContinuationToken());
    }
}
