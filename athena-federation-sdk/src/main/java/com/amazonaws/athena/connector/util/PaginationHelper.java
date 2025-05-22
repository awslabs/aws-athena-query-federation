/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.util;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;

import java.util.List;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

public class PaginationHelper
{
    private PaginationHelper() {}

    /**
     * Validates that nextToken and pageSize are valid non-negative integers, with the exception of
     * pageSize allowed to be -1 signifying unlimited pages.
     * 
     * @param nextToken
     * @param pageSize
     * @return int nextToken
     */
    public static int validateAndParsePaginationArguments(String nextToken, int pageSize)
    {
        int startToken;
        try {
            startToken = nextToken == null ? 0 : Integer.parseInt(nextToken);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid next token: " + nextToken, e);
        }
        if (startToken < 0) {
            throw new IllegalArgumentException("Invalid next token format. Token must be a valid integer, received: " + startToken);
        }
        if (pageSize < UNLIMITED_PAGE_SIZE_VALUE) {
            throw new IllegalArgumentException("Page size must be either -1 for unlimited or a positive integer, received: " + pageSize);
        }
        return startToken;
    }

    /**
     * Performs a manual or "fake" pagination. Takes a list of allTables retrieved and returns a subset of tables based off
     * of startToken and pageSize.
     *
     * @param startToken the start position in the subset
     * @param pageSize the number of tables to retrieve
     * @param catalogName required to return in ListTableResponse
     * @return ListTableResponse with subset of tables.
     */
    public static ListTablesResponse manualPagination(List<TableName> allTables, int startToken, int pageSize, String catalogName)
    {
        // If startToken is at or past the end of tables list, return empty list
        if (startToken >= allTables.size()) {
            return new ListTablesResponse(catalogName, List.of(), null);
        }

        int endToken = Math.min(startToken + pageSize, allTables.size());
        if (pageSize == UNLIMITED_PAGE_SIZE_VALUE) {
            endToken = allTables.size();
        }

        String nextToken;
        if (pageSize == UNLIMITED_PAGE_SIZE_VALUE || endToken == allTables.size()) {
            nextToken = null;
        }
        else {
            // Use long to avoid potential integer overflow
            nextToken = Long.toString((long) startToken + pageSize);
        }

        return new ListTablesResponse(catalogName, allTables.subList(startToken, endToken), nextToken);
    }
}
