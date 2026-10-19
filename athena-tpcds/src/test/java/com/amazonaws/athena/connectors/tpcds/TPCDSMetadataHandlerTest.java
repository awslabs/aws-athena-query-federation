/*-
 * #%L
 * athena-tpcds
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
package com.amazonaws.athena.connectors.tpcds;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.tpcds.qpt.TPCDSQueryPassthrough;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_NUMBER_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_SCALE_FACTOR_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_TOTAL_NUMBER_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class TPCDSMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSMetadataHandlerTest.class);
    private static final String TEST_QUERY_ID = "queryId";
    private static final String TEST_CATALOG_NAME = "default";
    private static final String TEST_CATALOG_NAME_ALT = "catalog_name";
    private static final String TEST_SCHEMA_TPCDS1 = "tpcds1";
    private static final String TEST_SCHEMA_TPCDS10 = "tpcds10";
    private static final String TEST_SCHEMA_TPCDS100 = "tpcds100";
    private static final String TEST_SCHEMA_TPCDS250 = "tpcds250";
    private static final String TEST_SCHEMA_TPCDS1000 = "tpcds1000";
    private static final String TEST_TABLE_CUSTOMER = "customer";
    private static final String TEST_TABLE_STORE = "store";
    private static final String TEST_TABLE_ITEM = "item";
    private static final String TEST_TABLE_STORE_SALES = "store_sales";
    private static final String TEST_TABLE_CATALOG_SALES = "catalog_sales";
    private static final String TEST_PARTITION_ID_FIELD = "partitionId";
    private static final String TEST_SPILL_BUCKET = "spillBucket";
    private static final String TEST_SPILL_PREFIX = "spillPrefix";
    private static final String CONFIG_ENABLE_QUERY_PASSTHROUGH = "enable_query_passthrough";
    private static final String CONFIG_VALUE_TRUE = "true";
    private static final String QPT_SIGNATURE = "SYSTEM.QUERY";
    /** Schema name yielding ceil(scale/48)=1000 splits to exercise doGetSplits batching. */
    private static final String TEST_SCHEMA_TPCDS_48000 = "tpcds48000";
    private static final String SCALE_FACTOR_1000 = "1000";
    private static final String CONTINUATION_TOKEN_5 = "5";
    private static final String TEST_IDENTITY_ARN = "arn";
    private static final String TEST_IDENTITY_ACCOUNT = "account";

    private FederatedIdentity identity = new FederatedIdentity(TEST_IDENTITY_ARN, TEST_IDENTITY_ACCOUNT, Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private TPCDSMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
    {
        handler = new TPCDSMetadataHandler(new LocalKeyFactory(), mockSecretsManager, mockAthena, TEST_SPILL_BUCKET, TEST_SPILL_PREFIX, com.google.common.collect.ImmutableMap.of());
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames_WithCatalogName_ReturnsSchemas()
    {
        logger.info("doListSchemas - enter");

        ListSchemasRequest req = new ListSchemasRequest(identity, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());

        assertTrue(res.getSchemas().size() == 5);
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables_WithCatalogAndSchema_ReturnsTables()
    {
        logger.info("doListTables - enter");

        ListTablesRequest req = new ListTablesRequest(identity, TEST_QUERY_ID, TEST_CATALOG_NAME,
                TEST_SCHEMA_TPCDS1, null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertTrue(res.getTables().contains(new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER)));

        assertTrue(res.getTables().size() == 25);

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable_WhenTableRequested_ReturnsTableSchema()
    {
        logger.info("doGetTable - enter");
        String expectedSchema = TEST_SCHEMA_TPCDS1;

        GetTableRequest req = new GetTableRequest(identity,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(expectedSchema, TEST_TABLE_CUSTOMER), Collections.emptyMap());

        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {} {}", res.getTableName(), res.getSchema());

        assertEquals(new TableName(expectedSchema, TEST_TABLE_CUSTOMER), res.getTableName());
        assertNotNull("Schema should not be null", res.getSchema());

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableLayout_WithCatalogAndTable_ReturnsSinglePartition()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Schema schema = SchemaBuilder.newBuilder().build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER),
                createConstraints(constraintsMap, Collections.emptyMap()),
                schema,
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertTrue(res.getPartitions().getRowCount() == 1);

        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetSplits_WithCatalogSchemaAndTable_ReturnsSplits()
    {
        logger.info("doGetSplits: enter");

        Schema schema = SchemaBuilder.newBuilder()
                .addIntField(TEST_PARTITION_ID_FIELD)
                .build();

        Block partitions = BlockUtils.newBlock(allocator, TEST_PARTITION_ID_FIELD, Types.MinorType.INT.getType(), 1);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME_ALT,
                new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER),
                partitions,
                Collections.EMPTY_LIST,
                createConstraints(Collections.emptyMap(), Collections.emptyMap()),
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
                assertNotNull(nextSplit.getProperty(SPLIT_NUMBER_FIELD));
                assertNotNull(nextSplit.getProperty(SPLIT_TOTAL_NUMBER_FIELD));
                assertNotNull(nextSplit.getProperty(SPLIT_SCALE_FACTOR_FIELD));
            }

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertTrue(numContinuations == 0);

        logger.info("doGetSplits: exit");
    }

    @Test
    public void doGetDataSourceCapabilities_WhenQptDisabled_ReturnsCapabilities()
    {
        GetDataSourceCapabilitiesRequest req = new GetDataSourceCapabilitiesRequest(identity, TEST_QUERY_ID, TEST_CATALOG_NAME);
        GetDataSourceCapabilitiesResponse res = handler.doGetDataSourceCapabilities(allocator, req);

        assertEquals(TEST_CATALOG_NAME, res.getCatalogName());

        Map<String, List<OptimizationSubType>> capabilities = res.getCapabilities();
        assertNotNull("Capabilities should not be null", capabilities);
    }

    @Test
    public void doGetDataSourceCapabilities_WhenQptEnabled_ReturnsCapabilities()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(CONFIG_ENABLE_QUERY_PASSTHROUGH, CONFIG_VALUE_TRUE);
        TPCDSMetadataHandler handlerWithQPT = new TPCDSMetadataHandler(
                new LocalKeyFactory(), mockSecretsManager, mockAthena, TEST_SPILL_BUCKET, TEST_SPILL_PREFIX, configOptions);

        GetDataSourceCapabilitiesRequest req = new GetDataSourceCapabilitiesRequest(identity, TEST_QUERY_ID, TEST_CATALOG_NAME);
        GetDataSourceCapabilitiesResponse res = handlerWithQPT.doGetDataSourceCapabilities(allocator, req);

        assertEquals(TEST_CATALOG_NAME, res.getCatalogName());

        Map<String, List<OptimizationSubType>> capabilities = res.getCapabilities();
        assertNotNull("Capabilities should not be null", capabilities);
        assertTrue("QPT capabilities should be present when enabled", capabilities.containsKey(QPT_SIGNATURE));
        assertEquals(3, capabilities.get(QPT_SIGNATURE).size());
    }

    @Test
    public void doGetQueryPassthroughSchema_WhenValidArguments_ReturnsSchema()
            throws Exception
    {
        Map<String, String> qptArguments = createQptArguments();

        GetTableRequest req = new GetTableRequest(identity,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER),
                qptArguments);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);

        assertEquals(new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER), res.getTableName());
        assertTrue("Schema should have fields", res.getSchema().getFields().size() > 0);
    }

    @Test
    public void doGetSplits_WhenQueryPassthrough_ReturnsSplits()
    {
        Map<String, String> qptArguments = createQptArguments();

        GetSplitsRequest req = createGetSplitsRequest(
                new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER),
                createConstraints(Collections.emptyMap(), qptArguments)
        );

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertTrue(response.getSplits().size() > 0);
        for (Split nextSplit : response.getSplits()) {
            validateSplitProperties(nextSplit);
        }
    }

    @Test
    public void doGetSplits_WhenQueryPassthroughLargeScale_ReturnsThousandSplitsAndContinuationToken()
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_CATALOG, TEST_CATALOG_NAME);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_SCHEMA, TEST_SCHEMA_TPCDS_48000);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_TABLE, TEST_TABLE_CUSTOMER);

        GetSplitsRequest req = createGetSplitsRequest(
                new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER),
                createConstraints(Collections.emptyMap(), qptArguments));

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertEquals(1000, response.getSplits().size());
        assertEquals("1000", response.getContinuationToken());
        for (Split nextSplit : response.getSplits()) {
            validateSplitProperties(nextSplit);
            assertEquals("48000", nextSplit.getProperty(SPLIT_SCALE_FACTOR_FIELD));
        }
    }

    @Test
    public void setupQueryPassthroughSplit_WhenQptRequest_ReturnsSingleSplitWithQptProperties()
    {
        Map<String, String> qptArguments = createQptArguments();

        GetSplitsRequest req = createGetSplitsRequest(
                new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER),
                createConstraints(Collections.emptyMap(), qptArguments));

        GetSplitsResponse response = handler.setupQueryPassthroughSplit(req);

        assertEquals(TEST_CATALOG_NAME_ALT, response.getCatalogName());
        assertEquals(1, response.getSplits().size());
        Split split = response.getSplits().iterator().next();
        assertEquals(TEST_CATALOG_NAME, split.getProperty(TPCDSQueryPassthrough.TPCDS_CATALOG));
        assertEquals(TEST_SCHEMA_TPCDS1, split.getProperty(TPCDSQueryPassthrough.TPCDS_SCHEMA));
        assertEquals(TEST_TABLE_CUSTOMER, split.getProperty(TPCDSQueryPassthrough.TPCDS_TABLE));
    }
    

    @Test
    public void doGetSplits_WhenContinuationTokenAndLargeScaleFactor_ReturnsSplitsFromToken()
    {
        GetSplitsRequest originalReq = createGetSplitsRequest(
                new TableName(TEST_SCHEMA_TPCDS1000, TEST_TABLE_CUSTOMER),
                createConstraints(Collections.emptyMap(), Collections.emptyMap())
        );

        String continuationToken = CONTINUATION_TOKEN_5;
        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);
        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertTrue("Should have splits when starting from continuation token", response.getSplits().size() > 0);
        for (Split nextSplit : response.getSplits()) {
            validateSplitProperties(nextSplit);
            assertEquals(SCALE_FACTOR_1000, nextSplit.getProperty(SPLIT_SCALE_FACTOR_FIELD));

            int splitNum = Integer.parseInt(nextSplit.getProperty(SPLIT_NUMBER_FIELD));
            assertTrue("Split number should be >= continuation token", splitNum >= Integer.parseInt(continuationToken));
        }
    }

    @Test
    public void doListTables_WhenDifferentSchemas_ReturnsTablesForEachSchema()
    {
        String[] schemas = {TEST_SCHEMA_TPCDS1, TEST_SCHEMA_TPCDS10, TEST_SCHEMA_TPCDS100, TEST_SCHEMA_TPCDS250, TEST_SCHEMA_TPCDS1000};

        for (String schema : schemas) {
            ListTablesRequest req = new ListTablesRequest(identity, TEST_QUERY_ID, TEST_CATALOG_NAME,
                    schema, null, UNLIMITED_PAGE_SIZE_VALUE);
            ListTablesResponse res = handler.doListTables(allocator, req);

            assertEquals(25, res.getTables().size());
            assertTrue(res.getTables().contains(new TableName(schema, TEST_TABLE_CUSTOMER)));
        }
    }

    @Test
    public void doGetTable_WhenDifferentTables_ReturnsTableSchemaForEach()
    {
        String[] tables = {TEST_TABLE_CUSTOMER, TEST_TABLE_STORE, TEST_TABLE_ITEM, TEST_TABLE_STORE_SALES, TEST_TABLE_CATALOG_SALES};

        for (String tableName : tables) {
            GetTableRequest req = new GetTableRequest(identity,
                    TEST_QUERY_ID,
                    TEST_CATALOG_NAME,
                    new TableName(TEST_SCHEMA_TPCDS1, tableName), Collections.emptyMap());

            GetTableResponse res = handler.doGetTable(allocator, req);

            assertEquals(new TableName(TEST_SCHEMA_TPCDS1, tableName), res.getTableName());
            assertTrue("Schema should have fields", res.getSchema().getFields().size() > 0);
        }
    }

    @Test
    public void doGetSplits_WhenDifferentScaleFactors_ReturnsSplitsWithCorrectScaleFactor()
    {
        String[] schemas = {TEST_SCHEMA_TPCDS1, TEST_SCHEMA_TPCDS10, TEST_SCHEMA_TPCDS100, TEST_SCHEMA_TPCDS250};
        for (String schema : schemas) {
            GetSplitsRequest req = createGetSplitsRequest(
                    new TableName(schema, TEST_TABLE_CUSTOMER),
                    createConstraints(Collections.emptyMap(), Collections.emptyMap())
            );

            GetSplitsResponse response = handler.doGetSplits(allocator, req);

            assertTrue(response.getSplits().size() > 0);
            for (Split nextSplit : response.getSplits()) {
                validateSplitProperties(nextSplit);

                int expectedScaleFactor = Integer.parseInt(schema.substring(5));
                assertEquals(String.valueOf(expectedScaleFactor), nextSplit.getProperty(SPLIT_SCALE_FACTOR_FIELD));
            }
        }
    }

    @Test
    public void doGetSplits_WhenContinuationToken_ReturnsSplits()
    {
        GetSplitsRequest originalReq = createGetSplitsRequest(
                new TableName(TEST_SCHEMA_TPCDS250, TEST_TABLE_CUSTOMER),
                createConstraints(Collections.emptyMap(), Collections.emptyMap())
        );

        String continuationToken = null;

        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);
            GetSplitsResponse response = handler.doGetSplits(allocator, req);
            continuationToken = response.getContinuationToken();

            for (Split nextSplit : response.getSplits()) {
                validateSplitProperties(nextSplit);
            }
        }
        while (continuationToken != null);
    }

    @Test(expected = RuntimeException.class)
    public void doGetTable_WhenUnknownTableRequested_ThrowsRuntimeException()
    {
        GetTableRequest req = new GetTableRequest(identity,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(TEST_SCHEMA_TPCDS1, "nonexistent_table"), Collections.emptyMap());
        handler.doGetTable(allocator, req);
    }

    @Test(expected = RuntimeException.class)
    public void doGetQueryPassthroughSchema_WhenUnknownTableInArguments_ThrowsRuntimeException()
            throws Exception
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_CATALOG, TEST_CATALOG_NAME);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_SCHEMA, TEST_SCHEMA_TPCDS1);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_TABLE, "nonexistent_table");

        GetTableRequest req = new GetTableRequest(identity,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(TEST_SCHEMA_TPCDS1, TEST_TABLE_CUSTOMER),
                qptArguments);
        handler.doGetQueryPassthroughSchema(allocator, req);
    }

    private void validateSplitProperties(Split split)
    {
        String splitNumber = split.getProperty(SPLIT_NUMBER_FIELD);
        assertNotNull("Split number should not be null", splitNumber);

        String splitTotalNumber = split.getProperty(SPLIT_TOTAL_NUMBER_FIELD);
        assertNotNull("Split total number should not be null", splitTotalNumber);

        String splitScaleFactor = split.getProperty(SPLIT_SCALE_FACTOR_FIELD);
        assertNotNull("Split scale factor should not be null", splitScaleFactor);
    }

    private Block createPartitionsBlock()
    {
        return BlockUtils.newBlock(allocator, TEST_PARTITION_ID_FIELD, Types.MinorType.INT.getType(), 1);
    }

    private Constraints createConstraints(
            Map<String, ValueSet> summaryConstraints,
            Map<String, String> queryPassthroughArguments)
    {
        return new Constraints(
                summaryConstraints,
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryPassthroughArguments,
                null);
    }

    private Map<String, String> createQptArguments()
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_CATALOG, TEST_CATALOG_NAME);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_SCHEMA, TEST_SCHEMA_TPCDS1);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_TABLE, TEST_TABLE_CUSTOMER);
        return qptArguments;
    }

    private GetSplitsRequest createGetSplitsRequest(TableName tableName, Constraints constraints)
    {
        return new GetSplitsRequest(identity,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME_ALT,
                tableName,
                createPartitionsBlock(),
                Collections.EMPTY_LIST,
                constraints,
                null);
    }
}
