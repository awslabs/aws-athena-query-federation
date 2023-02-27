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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
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
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
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
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
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
    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());

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
    private Block mockBlock;

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
        tableProviderMap.putIfAbsent(new TableName("schema1", "table1"), mockTableProvider1);
        tableProviderMap.putIfAbsent(new TableName("schema1", "table2"), mockTableProvider2);
        tableProviderMap.putIfAbsent(new TableName("schema2", "table1"), mockTableProvider3);

        when(mockTableProviderFactory.getTableProviders()).thenReturn(tableProviderMap);

        Map<String, List<TableName>> schemas = new HashMap<>();
        schemas.put("schema1", new ArrayList<>());
        schemas.put("schema2", new ArrayList<>());
        schemas.get("schema1").add(new TableName("schema1", "table1"));
        schemas.get("schema1").add(new TableName("schema1", "table2"));
        schemas.get("schema2").add(new TableName("schema2", "table1"));

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
        ListSchemasRequest request = new ListSchemasRequest(identity, queryId, catalog);
        ListSchemasResponse response = handler.doListSchemaNames(blockAllocator, request);

        assertEquals(2, response.getSchemas().size());
        assertTrue(response.getSchemas().contains("schema1"));
        assertTrue(response.getSchemas().contains("schema2"));
    }

    @Test
    public void doListTables()
    {
        ListTablesRequest request = new ListTablesRequest(identity, queryId, catalog, "schema1",
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse response = handler.doListTables(blockAllocator, request);

        assertEquals(2, response.getTables().size());
        assertTrue(response.getTables().contains(new TableName("schema1", "table1")));
        assertTrue(response.getTables().contains(new TableName("schema1", "table2")));
    }

    @Test
    public void doGetTable()
    {
        GetTableRequest request = new GetTableRequest(identity, queryId, catalog, new TableName("schema1", "table1"));

        when(mockTableProvider1.getTable(eq(blockAllocator), eq(request))).thenReturn(mock(GetTableResponse.class));
        GetTableResponse response = handler.doGetTable(blockAllocator, request);

        assertNotNull(response);
        verify(mockTableProvider1, times(1)).getTable(eq(blockAllocator), eq(request));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        GetTableLayoutRequest request = new GetTableLayoutRequest(identity, queryId, catalog,
                new TableName("schema1", "table1"),
                mockConstraints,
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse response = handler.doGetTableLayout(blockAllocator, request);

        assertNotNull(response);
        assertEquals(1, response.getPartitions().getRowCount());
    }

    @Test
    public void doGetSplits()
    {
        GetSplitsRequest request = new GetSplitsRequest(identity, queryId, catalog,
                new TableName("schema1", "table1"),
                mockBlock,
                Collections.emptyList(),
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                null);

        GetSplitsResponse response = handler.doGetSplits(blockAllocator, request);

        assertNotNull(response);
    }
}
