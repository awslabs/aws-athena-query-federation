package com.amazonaws.athena.connector.lambda.examples;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperUtil;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.lambda.invoke.LambdaFunctionException;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.examples.ExampleMetadataHandler.MAX_SPLITS_PER_REQUEST;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ExampleMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandlerTest.class);

    private BlockAllocatorImpl allocator;
    private ExampleMetadataHandler metadataHandler;

    @Before
    public void setUp()
    {
        logger.info("setUpBefore - enter");
        allocator = new BlockAllocatorImpl();
        metadataHandler = new ExampleMetadataHandler(new LocalKeyFactory(),
                mock(AWSSecretsManager.class),
                mock(AmazonAthena.class),
                "spill-bucket",
                "spill-prefix");
        logger.info("setUpBefore - exit");
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doListSchemas()
    {
        logger.info("doListSchemas - enter");
        ListSchemasRequest req = new ListSchemasRequest(IdentityUtil.fakeIdentity(), "queryId", "default");
        ObjectMapperUtil.assertSerialization(req);
        ListSchemasResponse res = metadataHandler.doListSchemaNames(allocator, req);
        ObjectMapperUtil.assertSerialization(res);
        logger.info("doListSchemas - {}", res.getSchemas());
        assertFalse(res.getSchemas().isEmpty());
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");
        ListTablesRequest req = new ListTablesRequest(IdentityUtil.fakeIdentity(), "queryId", "default", null);
        ObjectMapperUtil.assertSerialization(req);
        ListTablesResponse res = metadataHandler.doListTables(allocator, req);
        ObjectMapperUtil.assertSerialization(res);
        logger.info("doListTables - {}", res.getTables());
        assertFalse(res.getTables().isEmpty());
        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
    {
        logger.info("doGetTable - enter");
        GetTableRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), "queryId", "default",
                new TableName("custom_source", "fake_table"));
        ObjectMapperUtil.assertSerialization(req);
        GetTableResponse res = metadataHandler.doGetTable(allocator, req);
        ObjectMapperUtil.assertSerialization(res);
        assertTrue(res.getSchema().getFields().size() > 0);
        assertTrue(res.getSchema().getCustomMetadata().size() > 0);
        logger.info("doGetTable - {}", res);
        logger.info("doGetTable - exit");
    }

    @Test(expected = LambdaFunctionException.class)
    public void doGetTableFail()
    {
        try {
            logger.info("doGetTableFail - enter");
            GetTableRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), "queryId", "default",
                    new TableName("lambda", "fake"));
            metadataHandler.doGetTable(allocator, req);
        }
        catch (Exception ex) {
            logger.info("doGetTableFail: ", ex);
            throw new LambdaFunctionException(ex.getMessage(), false, "repackaged");
        }
    }

    /**
     * 200,000,000 million partitions pruned down to 38,000 and transmitted in 25 seconds
     *
     * @throws Exception
     */
    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        Schema tableSchema = SchemaBuilder.newBuilder()
                .addIntField("day")
                .addIntField("month")
                .addIntField("year")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("day");
        partitionCols.add("month");
        partitionCols.add("year");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("day", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 20)), false));

        constraintsMap.put("month", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 2)), false));

        constraintsMap.put("year", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1900)), false));

        GetTableLayoutRequest req = null;
        GetTableLayoutResponse res = null;
        try {

            req = new GetTableLayoutRequest(IdentityUtil.fakeIdentity(), "queryId", "default",
                    new TableName("schema1", "table1"),
                    new Constraints(constraintsMap),
                    tableSchema,
                    partitionCols);
            ObjectMapperUtil.assertSerialization(req);

            res = metadataHandler.doGetTableLayout(allocator, req);
            ObjectMapperUtil.assertSerialization(res);

            logger.info("doGetTableLayout - {}", res);
            Block partitions = res.getPartitions();
            for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());
        }
        finally {
            try {
                req.close();
                res.close();
            }
            catch (Exception ex) {
                logger.error("doGetTableLayout: ", ex);
            }
        }

        logger.info("doGetTableLayout - exit");
    }

    /**
     * The goal of this test is to test happy case for getting splits and also to exercise the continuation token
     * logic specifically.
     */
    @Test
    public void doGetSplits()
    {
        logger.info("doGetSplits: enter");

        String yearCol = "year";
        String monthCol = "month";
        String dayCol = "day";

        //This is the schema that ExampleMetadataHandler has layed out for a 'Partition' so we need to populate this
        //minimal set of info here.
        Schema schema = SchemaBuilder.newBuilder()
                .addField(yearCol, new ArrowType.Int(16, false))
                .addField(monthCol, new ArrowType.Int(16, false))
                .addField(dayCol, new ArrowType.Int(16, false))
                .addField(ExampleMetadataHandler.PARTITION_LOCATION, new ArrowType.Utf8())
                .addField(ExampleMetadataHandler.SERDE, new ArrowType.Utf8())
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add(yearCol);
        partitionCols.add(monthCol);
        partitionCols.add(dayCol);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(dayCol, SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 20)), false));

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 100;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(yearCol), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector(monthCol), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector(dayCol), i, (i % 28) + 1);
            BlockUtils.setValue(partitions.getFieldVector(ExampleMetadataHandler.PARTITION_LOCATION), i, String.valueOf(i));
            BlockUtils.setValue(partitions.getFieldVector(ExampleMetadataHandler.SERDE), i, "TextInputType");
        }
        partitions.setRowCount(num_partitions);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(IdentityUtil.fakeIdentity(), "queryId", "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap),
                continuationToken);
        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);
            ObjectMapperUtil.assertSerialization(req);

            logger.info("doGetSplits: req[{}]", req);
            metadataHandler.setEncryption(numContinuations % 2 == 0);
            logger.info("doGetSplits: Toggle encryption " + (numContinuations % 2 == 0));

            MetadataResponse rawResponse = metadataHandler.doGetSplits(allocator, req);
            ObjectMapperUtil.assertSerialization(rawResponse);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            logger.info("doGetSplits: continuationToken[{}] - numSplits[{}] - maxSplits[{}]",
                    new Object[] {continuationToken, response.getSplits().size(), MAX_SPLITS_PER_REQUEST});

            for (Split nextSplit : response.getSplits()) {
                if (numContinuations % 2 == 0) {
                    assertNotNull(nextSplit.getEncryptionKey());
                }
                else {
                    assertNull(nextSplit.getEncryptionKey());
                }
                assertNotNull(nextSplit.getProperty(SplitProperties.LOCATION.getId()));
                assertNotNull(nextSplit.getProperty(SplitProperties.SERDE.getId()));
                assertNotNull(nextSplit.getProperty(SplitProperties.SPLIT_PART.getId()));
            }

            assertTrue("Continuation criteria violated", (response.getSplits().size() == MAX_SPLITS_PER_REQUEST &&
                    response.getContinuationToken() != null) || response.getSplits().size() < MAX_SPLITS_PER_REQUEST);

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertTrue(numContinuations > 0);

        logger.info("doGetSplits: exit");
    }
}
