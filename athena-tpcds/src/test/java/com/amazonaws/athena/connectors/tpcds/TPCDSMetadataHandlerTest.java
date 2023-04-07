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
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
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
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.util.Strings;
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
import static com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_NUMBER_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_SCALE_FACTOR_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_TOTAL_NUMBER_FIELD;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TPCDSMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSMetadataHandlerTest.class);

    private FederatedIdentity identity = FederatedIdentity.newBuilder().setArn("arn").setAccount("account").build();
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

        ListSchemasRequest req = ListSchemasRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default").build();
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemasList());

        assertTrue(res.getSchemasList().size() == 5);
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");

        ListTablesRequest req = ListTablesRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default").setSchemaName("tpcds1").setPageSize(UNLIMITED_PAGE_SIZE_VALUE).build();
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTablesList());

        assertTrue(res.getTablesList().contains(TableName.newBuilder().setSchemaName("tpcds1").setTableName("customer").build()));

        assertTrue(res.getTablesList().size() == 25);

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
    {
        logger.info("doGetTable - enter");
        String expectedSchema = "tpcds1";

        GetTableRequest req = GetTableRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default").setTableName(TableName.newBuilder().setSchemaName(expectedSchema).setTableName("customer")).build();
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {} {}", res.getTableName(), res.getSchema());

        assertEquals(TableName.newBuilder().setSchemaName(expectedSchema).setTableName("customer").build(), res.getTableName());
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

        GetTableLayoutRequest req = GetTableLayoutRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("default")
            .setTableName(TableName.newBuilder().setSchemaName("tpcds1").setTableName("customer").build())
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schema))
            .build();
        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertTrue(ProtobufMessageConverter.fromProtoBlock(allocator, res.getPartitions()).getRowCount() == 1);

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
        GetSplitsRequest originalReq = GetSplitsRequest.newBuilder().setIdentity(identity).setQueryId("queryId").setCatalogName("catalog_name")
            .setTableName(TableName.newBuilder().setSchemaName("tpcds1").setTableName("customer").build())
            .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
            .build();
        int numContinuations = 0;
        do {
            GetSplitsRequest.Builder reqBuilder = originalReq.toBuilder();
            if (!Strings.isNullOrEmpty(continuationToken)) {
                reqBuilder.setContinuationToken(continuationToken);
            }
            GetSplitsRequest req = reqBuilder.build();
            logger.info("doGetSplits: req[{}]", req);

            GetSplitsResponse response = handler.doGetSplits(allocator, req);
            continuationToken = response.getContinuationToken();

            logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplitsList().size());

            for (Split nextSplit : response.getSplitsList()) {
                assertNotNull(nextSplit.getPropertiesMap().get(SPLIT_NUMBER_FIELD));
                assertNotNull(nextSplit.getPropertiesMap().get(SPLIT_TOTAL_NUMBER_FIELD));
                assertNotNull(nextSplit.getPropertiesMap().get(SPLIT_SCALE_FACTOR_FIELD));
            }

            if (!Strings.isNullOrEmpty(continuationToken)) {
                numContinuations++;
            }
        }
        while (!Strings.isNullOrEmpty(continuationToken));

        assertTrue(numContinuations == 0);

        logger.info("doGetSplits: exit");
    }
}
