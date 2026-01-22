/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

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
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResultField;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connectors.cloudwatch.qpt.CloudwatchQueryPassthrough;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.argThat;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchMetadataHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private static final String CATALOG_NAME = "default";
    private static final String QUERY_ID = "queryId";
    private CloudwatchMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private CloudWatchLogsClient mockAwsLogs;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
    {
        Mockito.lenient().when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            return DescribeLogStreamsResponse.builder()
                    .logStreams(
                            LogStream.builder().logStreamName("table-9").build(),
                            LogStream.builder().logStreamName("table-10").build())
                    .build();
        });

        when(mockAwsLogs.describeLogGroups(nullable(DescribeLogGroupsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            return DescribeLogGroupsResponse.builder()
                    .logGroups(
                            LogGroup.builder().logGroupName("schema-1").build(),
                            LogGroup.builder().logGroupName("schema-20").build())
                    .build();
        });
        handler = new CloudwatchMetadataHandler(mockAwsLogs, new LocalKeyFactory(), mockSecretsManager, mockAthena, "spillBucket", "spillPrefix", com.google.common.collect.ImmutableMap.of());
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames_withDefaultCatalog_returnsAllSchemasFromPaginatedResults()
            throws TimeoutException
    {
        logger.info("doListSchemas - enter");

        when(mockAwsLogs.describeLogGroups(nullable(DescribeLogGroupsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogGroupsRequest request = (DescribeLogGroupsRequest) invocationOnMock.getArguments()[0];

            DescribeLogGroupsResponse.Builder responseBuilder = DescribeLogGroupsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogGroup> logGroups = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogGroup nextLogGroup = LogGroup.builder().logGroupName("schema-" + i).build();
                    logGroups.add(nextLogGroup);
                }
            }

            responseBuilder.logGroups(logGroups);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        ListSchemasRequest req = new ListSchemasRequest(identity, QUERY_ID, CATALOG_NAME);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());

        assertEquals(30, res.getSchemas().size());
        verify(mockAwsLogs, times(4)).describeLogGroups(nullable(DescribeLogGroupsRequest.class));
        verifyNoMoreInteractions(mockAwsLogs);

        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables_withValidSchema_returnsAllLogStreamsTablePlusPaginatedResults()
            throws TimeoutException
    {
        logger.info("doListTables - enter");

        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) invocationOnMock.getArguments()[0];

            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogStream nextLogStream = LogStream.builder().logStreamName("table-" + i).build();
                    logStreams.add(nextLogStream);
                }
            }

            responseBuilder.logStreams(logStreams);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        ListTablesRequest req = new ListTablesRequest(identity, QUERY_ID, CATALOG_NAME,
                "schema-1", null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertTrue(res.getTables().contains(new TableName("schema-1", "all_log_streams")));

        assertEquals(31, res.getTables().size());

        verify(mockAwsLogs, times(4)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));
        verify(mockAwsLogs, times(1)).describeLogGroups(nullable(DescribeLogGroupsRequest.class));
        verifyNoMoreInteractions(mockAwsLogs);

        logger.info("doListTables - exit");
    }

    @Test
    public void doListTables_withPagination_returnsPaginatedTables() throws TimeoutException {

        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) invocationOnMock.getArguments()[0];

            assertEquals("Expected nextToken to be set", "test-token", request.nextToken());
            assertEquals("Expected limit to be set", Integer.valueOf(50), request.limit());

            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();

            List<LogStream> logStreams = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                LogStream nextLogStream = LogStream.builder().logStreamName("paginated-table-" + i).build();
                logStreams.add(nextLogStream);
            }

            responseBuilder.logStreams(logStreams);
            responseBuilder.nextToken("next-test-token");

            return responseBuilder.build();
        });

        // Test with pagination (pageSize != UNLIMITED_PAGE_SIZE_VALUE)
        ListTablesRequest req = new ListTablesRequest(identity, QUERY_ID, CATALOG_NAME,
                "schema-1", "test-token", 50);
        ListTablesResponse res = handler.doListTables(allocator, req);
        assertEquals("Should have 5 tables", 5, res.getTables().size());
        assertEquals("Should return next token", "next-test-token", res.getNextToken());
        assertTrue("Should contain paginated table", res.getTables().contains(new TableName("schema-1", "paginated-table-0")));

        verify(mockAwsLogs, times(1)).describeLogStreams(argThat((DescribeLogStreamsRequest request) ->
            request.nextToken() != null && request.nextToken().equals("test-token") &&
            request.limit() != null && request.limit().equals(50)
        ));
    }

    @Test
    public void doGetTable_withValidTableName_returnsTableSchemaWithColumns()
    {
        logger.info("doGetTable - enter");
        String expectedSchema = "schema-20";

        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) invocationOnMock.getArguments()[0];

            assertEquals(expectedSchema, request.logGroupName());
            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogStream nextLogStream = LogStream.builder().logStreamName("table-" + i).build();
                    logStreams.add(nextLogStream);
                }
            }

            responseBuilder.logStreams(logStreams);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        GetTableRequest req = new GetTableRequest(identity, QUERY_ID, CATALOG_NAME, new TableName(expectedSchema, "table-9"), Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {} {}", res.getTableName(), res.getSchema());

        assertEquals(new TableName(expectedSchema, "table-9"), res.getTableName());
        assertNotNull(res.getSchema());

        verify(mockAwsLogs, times(1)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableLayout_withValidRequest_returnsOnePartitionMatchingConstraint()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) invocationOnMock.getArguments()[0];

            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                int continuation = request.nextToken() == null ? 0 : Integer.valueOf(request.nextToken());
                for (int i = continuation * 100; i < 300; i++) {
                    LogStream nextLogStream = LogStream.builder()
                            .logStreamName("table-" + i)
                            .build();
                    logStreams.add(nextLogStream);
                }
            }

            responseBuilder.logStreams(logStreams);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("log_stream",
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add("table-10").build());

        Schema schema = SchemaBuilder.newBuilder().addStringField("log_stream").build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                QUERY_ID,
                CATALOG_NAME,
                new TableName("schema-1", "all_log_streams"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                schema,
                Collections.singleton("log_stream"));

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertNotNull(res.getPartitions().getSchema().findField("log_stream"));
        assertEquals(1, res.getPartitions().getRowCount());

        verify(mockAwsLogs, times(4)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));

        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetSplits_withValidRequest_paginatesThroughAllPartitions()
    {
        logger.info("doGetSplits: enter");

        Schema schema = SchemaBuilder.newBuilder()
                .addField(CloudwatchMetadataHandler.LOG_STREAM_FIELD, new ArrowType.Utf8())
                .addField(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD, new ArrowType.Int(64, true))
                .addField(CloudwatchMetadataHandler.LOG_GROUP_FIELD, new ArrowType.Utf8())
                .build();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 2_000;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD), i, 2016L + i);
            BlockUtils.setValue(partitions.getFieldVector(CloudwatchMetadataHandler.LOG_STREAM_FIELD), i, "log_stream_" + i);
            BlockUtils.setValue(partitions.getFieldVector(CloudwatchMetadataHandler.LOG_GROUP_FIELD), i, "log_group_" + i);
        }
        partitions.setRowCount(num_partitions);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                QUERY_ID,
                "catalog_name",
                new TableName("schema", "all_log_streams"),
                partitions,
                Collections.singletonList(CloudwatchMetadataHandler.LOG_STREAM_FIELD),
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                continuationToken);
        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);
            logger.info("doGetSplits: req[{}]", req);

            MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

            for (Split nextSplit : response.getSplits()) {
                assertNotNull(nextSplit.getProperty(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD));
                assertNotNull(nextSplit.getProperty(CloudwatchMetadataHandler.LOG_STREAM_FIELD));
                assertNotNull(nextSplit.getProperty(CloudwatchMetadataHandler.LOG_GROUP_FIELD));
            }

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertTrue(numContinuations > 0);

        logger.info("doGetSplits: exit");
    }

    @Test
    public void doGetSplits_withQueryPassthrough_returnsSplitWithPassthroughArgs() {
        
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(CloudwatchQueryPassthrough.STARTTIME, "1000");
        qptArguments.put(CloudwatchQueryPassthrough.ENDTIME, "5000");
        qptArguments.put(CloudwatchQueryPassthrough.QUERYSTRING, "fields @timestamp, @message");
        qptArguments.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, "/aws/lambda/test");
        qptArguments.put(CloudwatchQueryPassthrough.LIMIT, "100");
        qptArguments.put("schemaFunctionName", "SYSTEM.QUERY");

        Constraints qptConstraints = new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null);

        Schema schema = SchemaBuilder.newBuilder()
                .addField(CloudwatchMetadataHandler.LOG_STREAM_FIELD, new ArrowType.Utf8())
                .build();
        Block partitions = allocator.createBlock(schema);
        partitions.setRowCount(0);

        GetSplitsRequest request = new GetSplitsRequest(identity,
                QUERY_ID,
                "catalog_name",
                new TableName("system", "query"),
                partitions,
                Collections.emptyList(),
                qptConstraints,
                null);

        GetSplitsResponse response = handler.doGetSplits(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have exactly one split for QPT", 1, response.getSplits().size());
        assertNull("Should not have continuation token for QPT", response.getContinuationToken());

        Split qptSplit = response.getSplits().iterator().next();
        assertEquals("Should have STARTTIME property", "1000", qptSplit.getProperty(CloudwatchQueryPassthrough.STARTTIME));
        assertEquals("Should have ENDTIME property", "5000", qptSplit.getProperty(CloudwatchQueryPassthrough.ENDTIME));
        assertEquals("Should have QUERYSTRING property", "fields @timestamp, @message", qptSplit.getProperty(CloudwatchQueryPassthrough.QUERYSTRING));
        assertEquals("Should have LOGGROUPNAMES property", "/aws/lambda/test", qptSplit.getProperty(CloudwatchQueryPassthrough.LOGGROUPNAMES));
        assertEquals("Should have LIMIT property", "100", qptSplit.getProperty(CloudwatchQueryPassthrough.LIMIT));
    }

    @Test
    public void doGetDataSourceCapabilities_withValidRequest_returnsQueryPassthroughCapability() {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(identity, QUERY_ID, CATALOG_NAME);
        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);
        assertNotNull(response);
        assertNotNull(response.getCapabilities());
        assertFalse(response.getCapabilities().isEmpty());

        assertEquals(CATALOG_NAME, response.getCatalogName());
        
        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        assertTrue("Should contain SYSTEM.QUERY capability", capabilities.containsKey("SYSTEM.QUERY"));
        
        List<OptimizationSubType> queryCapabilities = capabilities.get("SYSTEM.QUERY");
        assertEquals("Should have 3 optimization subtypes", 3, queryCapabilities.size());
    }

    @Test
    public void doGetQueryPassthroughSchema_withValidArguments_returnsSchemaWithFieldsFromQueryResults() throws Exception {
            TableName tableName = new TableName("schema-1", "qpt_table");
            Map<String, String> qptArguments = new HashMap<>();
            qptArguments.put(CloudwatchQueryPassthrough.STARTTIME, "0");
            qptArguments.put(CloudwatchQueryPassthrough.ENDTIME, "1000");
            qptArguments.put(CloudwatchQueryPassthrough.QUERYSTRING, "SELECT field1, field2 FROM logs");
            qptArguments.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, "group1");
            qptArguments.put(CloudwatchQueryPassthrough.LIMIT, "1");

            List<ResultField> resultFields = new ArrayList<>();
            resultFields.add(ResultField.builder().field("field1").value("value1").build());
            resultFields.add(ResultField.builder().field("field2").value("value2").build());

            GetQueryResultsResponse queryResultsResponse = GetQueryResultsResponse.builder()
                    .results(Collections.singletonList(resultFields))
                    .status("Complete")
                    .build();

            when(mockAwsLogs.startQuery(nullable(StartQueryRequest.class)))
                    .thenReturn(StartQueryResponse.builder().queryId("query123").build());
            when(mockAwsLogs.getQueryResults(nullable(software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest.class)))
                    .thenReturn(queryResultsResponse);

            GetTableRequest req = new GetTableRequest(identity, QUERY_ID, CATALOG_NAME, tableName, qptArguments);
            GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);

            assertEquals(CATALOG_NAME, res.getCatalogName());
            assertEquals(tableName, res.getTableName());
            Schema schema = res.getSchema();
            assertNotNull(schema);
            assertEquals(2, schema.getFields().size());
            assertEquals("field1", schema.getFields().get(0).getName());
            assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(0).getType());
            assertEquals("field2", schema.getFields().get(1).getName());
            assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(1).getType());

            verify(mockAwsLogs, times(1)).startQuery(nullable(StartQueryRequest.class));
            verify(mockAwsLogs, times(1)).getQueryResults(nullable(software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest.class));

            when(mockAwsLogs.getQueryResults(nullable(software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest.class)))
                    .thenReturn(GetQueryResultsResponse.builder().results(Collections.emptyList()).status("Complete").build());

            res = handler.doGetQueryPassthroughSchema(allocator, req);

            assertEquals(CATALOG_NAME, res.getCatalogName());
            assertEquals(tableName, res.getTableName());
            assertNotNull(res.getSchema());
            assertEquals(0, res.getSchema().getFields().size());

            verify(mockAwsLogs, times(2)).getQueryResults(nullable(software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest.class));
    }

    @Test
    public void doListTables_withEmptyLogStreams_returnsOnlyAllLogStreamsTable()
            throws TimeoutException
    {
        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();
            // Return empty log streams list
            responseBuilder.logStreams(Collections.emptyList());
            return responseBuilder.build();
        });

        ListTablesRequest req = new ListTablesRequest(identity, QUERY_ID, CATALOG_NAME,
                "schema-1", null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);

        // Should only contain the special all_log_streams table
        assertEquals(1, res.getTables().size());
        assertTrue(res.getTables().contains(new TableName("schema-1", "all_log_streams")));

        verify(mockAwsLogs, times(1)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));
        verify(mockAwsLogs, times(1)).describeLogGroups(nullable(DescribeLogGroupsRequest.class));
        verifyNoMoreInteractions(mockAwsLogs);
    }

    @Test
    public void doGetTableLayout_withEmptyPartitions_returnsEmptyPartitions()
            throws Exception
    {
        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();
            // Return empty log streams list
            responseBuilder.logStreams(Collections.emptyList());
            return responseBuilder.build();
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("log_stream",
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add("non-existent-stream").build());

        Schema schema = SchemaBuilder.newBuilder().addStringField("log_stream").build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                QUERY_ID,
                CATALOG_NAME,
                new TableName("schema-1", "all_log_streams"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                schema,
                Collections.singleton("log_stream"));

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);
        assertNotNull(res.getPartitions().getSchema().findField("log_stream"));
        assertEquals(0, res.getPartitions().getRowCount());

        verify(mockAwsLogs, times(1)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));
    }


    @Test(expected = RuntimeException.class)
    public void doListSchemaNames_withMaxResultsExceeded_throwsException()
            throws TimeoutException
    {
        when(mockAwsLogs.describeLogGroups(nullable(DescribeLogGroupsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogGroupsResponse.Builder responseBuilder = DescribeLogGroupsResponse.builder();
            List<LogGroup> logGroups = new ArrayList<>();
            // Return enough log groups to exceed MAX_RESULTS (100,000)
            for (int i = 0; i < 50_000; i++) {
                logGroups.add(LogGroup.builder().logGroupName("schema-" + i).build());
            }
            responseBuilder.logGroups(logGroups);
            // Return a token to continue pagination
            responseBuilder.nextToken("continue-token");
            return responseBuilder.build();
        });

        ListSchemasRequest req = new ListSchemasRequest(identity, QUERY_ID, CATALOG_NAME);
        handler.doListSchemaNames(allocator, req);
    }
}
