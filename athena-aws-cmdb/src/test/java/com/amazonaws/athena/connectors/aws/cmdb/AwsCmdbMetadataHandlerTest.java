package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
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
    private FederatedIdentity identity = new FederatedIdentity("id", "principal", "account");

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

    @Mock
    private BlockAllocator mockAllocator;

    @Mock
    private Block mockBlock;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    private AwsCmdbMetadataHandler handler;

    @Before
    public void setUp()
            throws Exception
    {
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

        handler = new AwsCmdbMetadataHandler(mockTableProviderFactory, new LocalKeyFactory(), mockSecretsManager, bucket, prefix);

        verify(mockTableProviderFactory, times(1)).getTableProviders();
        verify(mockTableProviderFactory, times(1)).getSchemas();
        verifyNoMoreInteractions(mockTableProviderFactory);
    }

    @After
    public void tearDown()
            throws Exception
    {
    }

    @Test
    public void doListSchemaNames()
    {
        ListSchemasRequest request = new ListSchemasRequest(identity, queryId, catalog);
        ListSchemasResponse response = handler.doListSchemaNames(mockAllocator, request);

        assertEquals(2, response.getSchemas().size());
        assertTrue(response.getSchemas().contains("schema1"));
        assertTrue(response.getSchemas().contains("schema2"));
    }

    @Test
    public void doListTables()
    {
        ListTablesRequest request = new ListTablesRequest(identity, queryId, catalog, "schema1");
        ListTablesResponse response = handler.doListTables(mockAllocator, request);

        assertEquals(2, response.getTables().size());
        assertTrue(response.getTables().contains(new TableName("schema1", "table1")));
        assertTrue(response.getTables().contains(new TableName("schema1", "table2")));
    }

    @Test
    public void doGetTable()
    {
        GetTableRequest request = new GetTableRequest(identity, queryId, catalog, new TableName("schema1", "table1"));

        when(mockTableProvider1.getTable(eq(mockAllocator), eq(request))).thenReturn(mock(GetTableResponse.class));
        GetTableResponse response = handler.doGetTable(mockAllocator, request);

        assertNotNull(response);
        verify(mockTableProvider1, times(1)).getTable(eq(mockAllocator), eq(request));
    }

    @Test
    public void doGetTableLayout()
    {
        GetTableLayoutRequest request = new GetTableLayoutRequest(identity, queryId, catalog,
                new TableName("schema1", "table1"),
                mockConstraints,
                Collections.emptyMap());

        when(mockTableProvider1.getTableLayout(eq(mockAllocator), eq(request))).thenReturn(mock(GetTableLayoutResponse.class));
        GetTableLayoutResponse response = handler.doGetTableLayout(mockAllocator, request);

        assertNotNull(response);
        verify(mockTableProvider1, times(1)).getTableLayout(eq(mockAllocator), eq(request));
    }

    @Test
    public void doGetSplits()
    {
        GetSplitsRequest request = new GetSplitsRequest(identity, queryId, catalog,
                new TableName("schema1", "table1"),
                mockBlock,
                Collections.emptyList(),
                new Constraints(new HashMap<>()),
                null);

        GetSplitsResponse response = handler.doGetSplits(mockAllocator, request);

        assertNotNull(response);
    }
}