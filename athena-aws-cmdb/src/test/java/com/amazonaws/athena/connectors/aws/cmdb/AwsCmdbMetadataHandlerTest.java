/*-
 * #%L
 * athena-aws-cmdb
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
package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.proto.data.Block;
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
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AwsCmdbMetadataHandlerTest
{
    private String catalog = "catalog";
    private String bucket = "bucket";
    private String prefix = "prefix";
    private String queryId = "queryId";
    private FederatedIdentity identity = FederatedIdentity.newBuilder().setArn("arn").setAccount("account").build();

    @Mock
    private AmazonS3 mockS3;

    @Mock
    private TableProviderFactory mockTableProviderFactory;

    @Mock
    private Constraints mockConstraints;

    @Mock
    private TableProvider mockTableProvider1;

    @Mock
    private TableProvider mockTableProvider2;

    @Mock
    private TableProvider mockTableProvider3;

    private BlockAllocator blockAllocator;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    private AwsCmdbMetadataHandler handler;

    @Before
    public void setUp()
            throws Exception
    {
        blockAllocator = new BlockAllocatorImpl();
        Map<TableName, TableProvider> tableProviderMap = new HashMap<>();
        tableProviderMap.putIfAbsent(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build(), mockTableProvider1);
        tableProviderMap.putIfAbsent(TableName.newBuilder().setSchemaName("schema1").setTableName("table2").build(), mockTableProvider1);
        tableProviderMap.putIfAbsent(TableName.newBuilder().setSchemaName("schema2").setTableName("table1").build(), mockTableProvider1);

        when(mockTableProviderFactory.getTableProviders()).thenReturn(tableProviderMap);

        Map<String, List<TableName>> schemas = new HashMap<>();
        schemas.put("schema1", new ArrayList<>());
        schemas.put("schema2", new ArrayList<>());
        schemas.get("schema1").add(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build());
        schemas.get("schema1").add(TableName.newBuilder().setSchemaName("schema1").setTableName("table2").build());
        schemas.get("schema2").add(TableName.newBuilder().setSchemaName("schema2").setTableName("table1").build());

        when(mockTableProviderFactory.getSchemas()).thenReturn(schemas);

        handler = new AwsCmdbMetadataHandler(mockTableProviderFactory, new LocalKeyFactory(), mockSecretsManager, mockAthena, bucket, prefix, com.google.common.collect.ImmutableMap.of());

        verify(mockTableProviderFactory, times(1)).getTableProviders();
        verify(mockTableProviderFactory, times(1)).getSchemas();
        verifyNoMoreInteractions(mockTableProviderFactory);
    }

    @After
    public void tearDown()
            throws Exception
    {
        blockAllocator.close();
    }

    @Test
    public void doListSchemaNames()
    {
        ListSchemasRequest request = ListSchemasRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).build();
        ListSchemasResponse response = handler.doListSchemaNames(blockAllocator, request);

        assertEquals(2, response.getSchemasList().size());
        assertTrue(response.getSchemasList().contains("schema1"));
        assertTrue(response.getSchemasList().contains("schema2"));
    }

    @Test
    public void doListTables()
    {
        ListTablesRequest request = ListTablesRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).setSchemaName("schema1").setPageSize(UNLIMITED_PAGE_SIZE_VALUE).build();
        ListTablesResponse response = handler.doListTables(blockAllocator, request);

        assertEquals(2, response.getTablesList().size());
        assertTrue(response.getTablesList().contains(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build()));
        assertTrue(response.getTablesList().contains(TableName.newBuilder().setSchemaName("schema1").setTableName("table2").build()));
    }

    @Test
    public void doGetTable()
    {
        GetTableRequest request = GetTableRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).setTableName(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build()).build();

        when(mockTableProvider1.getTable(eq(blockAllocator), eq(request))).thenReturn(
            GetTableResponse.newBuilder()
                .setCatalogName(catalog)
                .setTableName(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build())
                .build()
            );
        GetTableResponse response = handler.doGetTable(blockAllocator, request);

        assertNotNull(response);
        verify(mockTableProvider1, times(1)).getTable(eq(blockAllocator), eq(request));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        GetTableLayoutRequest request = GetTableLayoutRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog)
            .setTableName(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build()).setConstraints(ProtobufMessageConverter.toProtoConstraints(mockConstraints))
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
            .build();
        GetTableLayoutResponse response = handler.doGetTableLayout(blockAllocator, request);

        assertNotNull(response);
        assertEquals(1, ProtobufMessageConverter.fromProtoBlock(blockAllocator, response.getPartitions()).getRowCount());
    }

    @Test
    public void doGetSplits()
    {
        GetSplitsRequest request = GetSplitsRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).setTableName(TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build())
            .setPartitions(Block.newBuilder().build()).build();
        
        GetSplitsResponse response = handler.doGetSplits(blockAllocator, request);

        assertNotNull(response);
    }
}
