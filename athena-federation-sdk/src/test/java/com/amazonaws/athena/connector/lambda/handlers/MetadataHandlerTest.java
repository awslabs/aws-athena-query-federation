/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

import static org.junit.Assert.assertEquals;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the the MetadataHandler class, specifically the new pagination logic for the LIST_TABLES request.
 */
@RunWith(MockitoJUnitRunner.class)
public class MetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataHandlerTest.class);

    private final ListTablesResponse expectedListTablesResponse = new ListTablesResponse("catalog",
            new ImmutableList.Builder<TableName>()
                    .add(new TableName("schema", "table5"))
                    .add(new TableName("schema", "table3"))
                    .add(new TableName("schema", "table1"))
                    .add(new TableName("schema", "table4"))
                    .add(new TableName("schema", "table2"))
                    .build());

    @Rule
    public TestName testName = new TestName();

    @Mock
    private MetadataHandler metadataHandler;

    @Mock
    private BlockAllocatorImpl allocator;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private OutputStream outputStream;

    @Captor
    ArgumentCaptor<ListTablesResponse> listTablesResponseCaptor;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());

        when(metadataHandler.doListTables(any(BlockAllocatorImpl.class), any(ListTablesRequest.class)))
                .thenReturn(expectedListTablesResponse);
        when(metadataHandler.doPaginatedListTables(any(BlockAllocatorImpl.class), any(ListTablesRequest.class)))
                .thenCallRealMethod();
    }

    @After
    public void cleanUp()
    {
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListTablesTest()
            throws Exception
    {
        ListTablesRequest listTablesRequest = new ListTablesRequest(IdentityUtil.fakeIdentity(),
                "query_id", "catalog", "schema");

        metadataHandler.doHandleRequest(allocator, objectMapper, listTablesRequest, outputStream);
        verify(metadataHandler, times(1)).doListTables(allocator, listTablesRequest);
        verify(metadataHandler, times(0)).doPaginatedListTables(allocator, listTablesRequest);
        verify(objectMapper, times(1))
                .writeValue(any(OutputStream.class), listTablesResponseCaptor.capture());
        ListTablesResponse actualListTablesResponse = listTablesResponseCaptor.getValue();
        assertEquals("ListTableResponse objects do not match.",
                expectedListTablesResponse, actualListTablesResponse);
    }

    @Test
    public void doPaginatedListTablesTest()
            throws Exception
    {
        ListTablesRequest listTablesRequest = new ListTablesRequest(IdentityUtil.fakeIdentity(),
                "query_id", "catalog", "schema", "table2");

        ListTablesResponse expectedListTablesResponse = new ListTablesResponse("catalog",
                new ImmutableList.Builder<TableName>()
                        .add(new TableName("schema", "table2"))
                        .add(new TableName("schema", "table3"))
                        .add(new TableName("schema", "table4"))
                        .build(), "table5");

        when(metadataHandler.getListTablesPageSize()).thenReturn(3);

        metadataHandler.doHandleRequest(allocator, objectMapper, listTablesRequest, outputStream);
        verify(metadataHandler, times(1)).doListTables(allocator, listTablesRequest);
        verify(metadataHandler, times(1)).doPaginatedListTables(allocator, listTablesRequest);
        verify(objectMapper, times(1))
                .writeValue(any(OutputStream.class), listTablesResponseCaptor.capture());
        ListTablesResponse actualListTablesResponse = listTablesResponseCaptor.getValue();
        assertEquals("ListTableResponse objects do not match.",
                expectedListTablesResponse, actualListTablesResponse);
    }
}
