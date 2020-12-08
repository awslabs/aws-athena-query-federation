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
package com.amazonaws.connectors.athena.slack;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
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
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class SlackMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(SlackMetadataHandlerTest.class);

    private SlackMetadataHandler handler = new SlackMetadataHandler(new LocalKeyFactory(),
            mock(AWSSecretsManager.class),
            mock(AmazonAthena.class),
            "spill-bucket",
            "spill-prefix");

    private boolean enableTests = System.getenv("publishing") != null &&
            System.getenv("publishing").equalsIgnoreCase("true");

    private BlockAllocatorImpl allocator;

    @Before
    public void setUp()
    {
        logger.info("setUpBefore - enter");
        allocator = new BlockAllocatorImpl();
        logger.info("setUpBefore - exit");
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames()
    {
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail. When you attempt to publis
            //using ../toos/publish.sh ...  it will set the publishing flag and force these tests. This is how we
            //avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("doListSchemaNames: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }

        logger.info("doListSchemas - enter");
        ListSchemasRequest req = new ListSchemasRequest(fakeIdentity(), "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());
        assertFalse(res.getSchemas().isEmpty());
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
    {
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail. When you attempt to publis
            //using ../toos/publish.sh ...  it will set the publishing flag and force these tests. This is how we
            //avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("doListTables: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }

        logger.info("doListTables - enter");
        ListTablesRequest req = new ListTablesRequest(fakeIdentity(), "queryId", "default", "slackapi");
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());
        assertFalse(res.getTables().isEmpty());
        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable(){
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail. When you attempt to publis
            //using ../toos/publish.sh ...  it will set the publishing flag and force these tests. This is how we
            //avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("doGetTable: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }

        logger.info("doGetTable - enter");
        GetTableRequest req = new GetTableRequest(fakeIdentity(), "queryId", "default",
                new TableName("slackanalytics", "member_analytics"));
        GetTableResponse res = handler.doGetTable(allocator, req);
        assertTrue(res.getSchema().getFields().size() > 0);
        assertTrue(res.getSchema().getCustomMetadata().size() >= 0);
        logger.info("doGetTable - {}", res);
        logger.info("doGetTable - exit");
    }

    @Test
    public void getPartitions()
            throws Exception
    {
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail. When you attempt to publis
            //using ../toos/publish.sh ...  it will set the publishing flag and force these tests. This is how we
            //avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("getPartitions: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }

        logger.info("getPartitions - enter");

        Schema tableSchema = SchemaBuilder.newBuilder()
                .addStringField("date")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("date");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("date", SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.VARCHAR.getType(), 0)), false));

        GetTableLayoutRequest req = null;
        GetTableLayoutResponse res = null;
        try {

            req = new GetTableLayoutRequest(fakeIdentity(), "queryId", "default",
                    new TableName("slackanalytics", "member_analytics"),
                    new Constraints(constraintsMap),
                    tableSchema,
                    partitionCols);

            res = handler.doGetTableLayout(allocator, req);

            logger.info("getPartitions - {}", res);
            Block partitions = res.getPartitions();
            for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
                logger.info("getPartitions:{} {}", row, BlockUtils.rowToString(partitions, row));
            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("getPartitions: partitions[{}]", partitions.getRowCount());
        }
        finally {
            try {
                req.close();
                res.close();
            }
            catch (Exception ex) {
                logger.error("getPartitions: ", ex);
            }
        }

        logger.info("getPartitions - exit");
    }

    @Test
    public void doGetSplits()
        throws Exception
    {
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail. When you attempt to publis
            //using ../toos/publish.sh ...  it will set the publishing flag and force these tests. This is how we
            //avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("doGetSplits: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }

        logger.info("doGetSplits: enter");

        String dateColVal = "date";

        //This is the schema that SampleMetadataHandler has layed out for a 'Partition' so we need to populate this
        //minimal set of info here.
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField(dateColVal)
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add(dateColVal);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(dateColVal), i, 1 + i);
        }
        partitions.setRowCount(num_partitions);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(fakeIdentity(), "queryId", "catalog_name",
                new TableName("slackanalytics", "member_analytics"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap),
                continuationToken);
        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

            logger.info("doGetSplits: req[{}]", req);
            MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            logger.info("doGetSplits: continuationToken[{}] - splits[{}]", continuationToken, response.getSplits());

            for (Split nextSplit : response.getSplits()) {
                assertNotNull(nextSplit.getProperty("date"));
            }

            assertTrue(!response.getSplits().isEmpty());

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertTrue(numContinuations == 0);

        logger.info("doGetSplits: exit");
    }

    private static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    }
}
