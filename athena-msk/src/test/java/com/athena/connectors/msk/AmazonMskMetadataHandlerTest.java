/*-
 * #%L
 * trianz-AmazonMsk-athena-sdk
 * %%
 * Copyright (C) 2019 - 2022 Trianz
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
package com.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.athena.connectors.msk.trino.QueryExecutor;
import com.athena.connectors.msk.trino.TrinoRecord;
import com.athena.connectors.msk.trino.TrinoRecordSet;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import uk.org.webcompere.systemstubs.rules.EnvironmentVariablesRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({AmazonMskUtils.class, System.class, AWSSecretsManagerClientBuilder.class, QueryExecutor.class})

public class AmazonMskMetadataHandlerTest
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final TableName TABLE_NAME = new TableName("dataset1", "table1");
    private AmazonMskMetadataHandler amazonMskMetadataHandler;

    @Mock
    QueryExecutor queryExecutor;

    @Rule
    public EnvironmentVariablesRule environmentVariables = new EnvironmentVariablesRule();

    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    private Block partitions;
    private List<String> partitionCols;
    private Constraints constraints;
    private ListTablesRequest listTablesRequest;

    @Before
    public void setUp()
    {
        System.setProperty("aws.region", "us-east-1");
        MockitoAnnotations.initMocks(this);
        amazonMskMetadataHandler = new AmazonMskMetadataHandler(queryExecutor);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        partitions = Mockito.mock(Block.class);
        partitionCols = Mockito.mock(List.class);
        constraints = Mockito.mock(Constraints.class);
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
    }

    @Test
    public void testdoListSchemaNames()
    {
        Set<String> schemaset = new HashSet<String>();
        schemaset.add("default");
        ListSchemasRequest request = mock(ListSchemasRequest.class);
        when(request.getCatalogName()).thenReturn("default");
        assertEquals(new ListSchemasResponse("schemas", schemaset).toString(), amazonMskMetadataHandler.doListSchemaNames(blockAllocator, request).toString());
    }

    @Test
    public void testdoListSchemaNamesThrowsException()
    {
        ListSchemasRequest listSchemasRequest = mock(ListSchemasRequest.class);
        when(listSchemasRequest.getCatalogName()).thenThrow(new RuntimeException("RuntimeException() "));
        ListSchemasResponse schemaNames = amazonMskMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertNull(schemaNames);
    }

    @Test
    public void testDoListTables()
    {
        listTablesRequest = new ListTablesRequest(federatedIdentity, QUERY_ID,
                "testCatalog", "default", null, 1);
        ListTablesResponse expectedResponse = new ListTablesResponse("testcatalog", new ImmutableList.Builder<TableName>().add(new TableName("default", "employee")).build(), null);
        when(queryExecutor.execute("show tables")).thenReturn(
                new TrinoRecordSet(List.of(new TrinoRecord(Arrays.asList("employee"))))
        );
        ListTablesResponse actualResponse = amazonMskMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertEquals(expectedResponse.toString(), actualResponse.toString());
    }

    @Test
    public void testdoListTablesThrowsException()
    {
        ListTablesRequest listTablesRequest = mock(ListTablesRequest.class);
        when(listTablesRequest.getCatalogName()).thenThrow(new RuntimeException("RuntimeException() "));
        ListTablesResponse tablesNames = amazonMskMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertNull(tablesNames);
    }

    @Test
    public void testDoGetTable()
    {
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, "kafka", new TableName("default", "employee"));
        TrinoRecordSet trinoRecords = new TrinoRecordSet(List.of(
                new TrinoRecord(List.of("employee", "varchar", "", ""))));
        when(queryExecutor.execute(Mockito.anyString())).thenReturn(trinoRecords);
        GetTableResponse actualResponse = amazonMskMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        assertNotNull(actualResponse);
    }

    @Test
    public void testdoGetTableThrowsException()
    {
        GetTableRequest getTableRequest = mock(GetTableRequest.class);
        when(getTableRequest.getTableName()).thenThrow(new RuntimeException("RuntimeException() "));
        GetTableResponse tablesName = amazonMskMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        assertNull(tablesName);
    }

    @Test
    public void testDoGetSplits() throws IOException,
            InterruptedException
    {
        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                "123", "catalogName", new TableName("schemaName", "tableName"),
                partitions, partitionCols, constraints
                , "token");
        when(queryExecutor.execute("select count(*) FROM default.tableName")).thenReturn(
                new TrinoRecordSet(List.of(
                        new TrinoRecord(Arrays.asList("4"))))
        );
        GetSplitsResponse getSplitsResponse = amazonMskMetadataHandler.doGetSplits(
                blockAllocator,
                request);
        assertNotNull(getSplitsResponse);
        assertEquals(1, getSplitsResponse.getSplits().size());
    }

}