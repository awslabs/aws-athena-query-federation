/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

public class PaginatedSchemaListerTest
{
    private static final String QUERY_ID = "query-id";
    private static final String CATALOG = "catalog";

    private BlockAllocator blockAllocator;
    private final FederatedIdentity identity = new FederatedIdentity(
            "arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());

    private TestPaginatedSchemaLister lister;

    @Before
    public void setUp()
    {
        blockAllocator = new BlockAllocatorImpl();
        lister = new TestPaginatedSchemaLister(Arrays.asList("charlie", "alpha", "bravo"));
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
    }

    @Test
    public void executeListSchemaNames_UnlimitedRequest_ReturnsAllSchemasWithNullNextToken() throws Exception
    {
        ListSchemasRequest request = new ListSchemasRequest(identity, QUERY_ID, CATALOG);

        ListSchemasResponse response = lister.executeListSchemaNames(blockAllocator, request);

        assertThat(response.getCatalogName()).isEqualTo(CATALOG);
        assertThat(response.getSchemas()).containsExactlyInAnyOrder("charlie", "alpha", "bravo");
        assertThat(response.getNextToken()).isNull();
        assertThat(lister.listSchemasInvocations).isEqualTo(1);
        assertThat(lister.listPaginatedSchemasInvocations).isZero();
    }

    @Test
    public void executeListSchemaNames_FirstPaginatedPage_UsesManualPagination() throws Exception
    {
        ListSchemasRequest request = new ListSchemasRequest(identity, QUERY_ID, CATALOG, null, 2);

        ListSchemasResponse response = lister.executeListSchemaNames(blockAllocator, request);

        assertThat(response.getSchemas()).containsExactly("alpha", "bravo");
        assertThat(response.getNextToken()).isEqualTo("2");
        assertThat(lister.listSchemasInvocations).isEqualTo(1);
        assertThat(lister.listPaginatedSchemasInvocations).isEqualTo(1);
    }

    @Test
    public void executeListSchemaNames_SecondPaginatedPage_UsesManualPagination() throws Exception
    {
        ListSchemasRequest request = new ListSchemasRequest(identity, QUERY_ID, CATALOG, "2", 2);

        ListSchemasResponse response = lister.executeListSchemaNames(blockAllocator, request);

        assertThat(response.getSchemas()).containsExactly("charlie");
        assertThat(response.getNextToken()).isNull();
    }

    @Test
    public void executeListSchemaNames_NativePaginationOverride_ReturnsCustomResponse() throws Exception
    {
        lister.nativePaginatedResponse = new ListSchemasResponse(
                CATALOG, List.of("native-schema"), "native-token");
        ListSchemasRequest request = new ListSchemasRequest(identity, QUERY_ID, CATALOG, null, 1);

        ListSchemasResponse response = lister.executeListSchemaNames(blockAllocator, request);

        assertThat(response.getSchemas()).containsExactly("native-schema");
        assertThat(response.getNextToken()).isEqualTo("native-token");
        assertThat(lister.listSchemasInvocations).isZero();
    }

    @Test
    public void executeListSchemaNames_UnlimitedPageSizeWithNextToken_UsesPaginatedPath() throws Exception
    {
        ListSchemasRequest request = new ListSchemasRequest(identity, QUERY_ID, CATALOG, "0", UNLIMITED_PAGE_SIZE_VALUE);

        ListSchemasResponse response = lister.executeListSchemaNames(blockAllocator, request);

        assertThat(response.getSchemas()).containsExactly("alpha", "bravo", "charlie");
        assertThat(response.getNextToken()).isNull();
        assertThat(lister.listPaginatedSchemasInvocations).isEqualTo(1);
    }

    private static final class TestPaginatedSchemaLister implements PaginatedSchemaLister
    {
        private final Collection<String> schemas;
        private ListSchemasResponse nativePaginatedResponse;
        private int listSchemasInvocations;
        private int listPaginatedSchemasInvocations;

        private TestPaginatedSchemaLister(Collection<String> schemas)
        {
            this.schemas = schemas;
        }

        @Override
        public Collection<String> listSchemas(BlockAllocator allocator, ListSchemasRequest request)
        {
            listSchemasInvocations++;
            return schemas;
        }

        @Override
        public ListSchemasResponse listPaginatedSchemas(BlockAllocator allocator, ListSchemasRequest request)
                throws Exception
        {
            listPaginatedSchemasInvocations++;
            if (nativePaginatedResponse != null) {
                return nativePaginatedResponse;
            }
            return PaginatedSchemaLister.super.listPaginatedSchemas(allocator, request);
        }
    }
}
