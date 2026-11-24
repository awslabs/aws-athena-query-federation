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
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.util.PaginationHelper;

import java.util.Collection;

import static com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest.UNLIMITED_PAGE_SIZE_VALUE;

/**
 * List-schemas pagination contract for non-JDBC {@link MetadataHandler} connectors.
 * <p>
 * Uses the same hook-based pagination approach as table listing: implement {@link #listSchemas},
 * optionally override {@link #listPaginatedSchemas} for true native pagination, and delegate
 * {@code doListSchemaNames} to {@link #executeListSchemaNames}.
 * <p>
 * JDBC connectors apply the equivalent pattern in {@code JdbcMetadataHandler} via
 * {@code listPaginatedSchemas(Connection, ListSchemasRequest)}, because schema listing requires
 * a JDBC {@link java.sql.Connection}.
 */
public interface PaginatedSchemaLister
{
    /**
     * Returns the complete list of schema names from the data source.
     */
    Collection<String> listSchemas(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception;

    /**
     * Returns a paginated list of schema names using in-memory pagination over {@link #listSchemas}.
     * Override for true native pagination from the data source.
     */
    default ListSchemasResponse listPaginatedSchemas(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception
    {
        return PaginationHelper.manualSchemasPagination(
                listSchemas(allocator, request),
                request.getNextToken(),
                request.getPageSize(),
                request.getCatalogName());
    }

    /**
     * Resolves a list-schemas request using unlimited or paginated behavior.
     */
    default ListSchemasResponse executeListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception
    {
        if (request.getPageSize() == UNLIMITED_PAGE_SIZE_VALUE && request.getNextToken() == null) {
            return new ListSchemasResponse(request.getCatalogName(), listSchemas(allocator, request), null);
        }
        return listPaginatedSchemas(allocator, request);
    }
}
