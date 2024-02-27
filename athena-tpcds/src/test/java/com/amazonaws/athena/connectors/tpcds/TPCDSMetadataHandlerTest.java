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
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_NUMBER_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_SCALE_FACTOR_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_TOTAL_NUMBER_FIELD;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TPCDSMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSMetadataHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private TPCDSMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        handler = new TPCDSMetadataHandler(new LocalKeyFactory(), mockSecretsManager, mockAthena, "spillBucket", "spillPrefix", com.google.common.collect.ImmutableMap.of());
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

        assertTrue(res.getSchemas().size() == 5);
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");

        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default",
                "tpcds1", null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertTrue(res.getTables().contains(new TableName("tpcds1", "customer")));

        assertTrue(res.getTables().size() == 25);

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
    {
        logger.info("doGetTable - enter");
        String expectedSchema = "tpcds1";

        GetTableRequest req = new GetTableRequest(identity,
                "queryId",
                "default",
                new TableName(expectedSchema, "customer"), Collections.emptyMap());

        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {} {}", res.getTableName(), res.getSchema());

        assertEquals(new TableName(expectedSchema, "customer"), res.getTableName());
        assertTrue(res.getSchema() != null);

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Schema schema = SchemaBuilder.newBuilder().build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "queryId",
                "default",
                new TableName("tpcds1", "customer"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                schema,
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertTrue(res.getPartitions().getRowCount() == 1);

        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetSplits()
    {
        logger.info("doGetSplits: enter");

        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("partitionId")
                .build();

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 1);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName("tpcds1", "customer"),
                partitions,
                Collections.EMPTY_LIST,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
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
}
