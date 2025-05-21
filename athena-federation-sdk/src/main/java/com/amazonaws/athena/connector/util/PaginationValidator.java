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

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

public class PaginationValidator
{
    private PaginationValidator() {}

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
}
